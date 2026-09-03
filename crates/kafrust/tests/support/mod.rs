use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub enum ScriptedResponse {
    Drop,
    Hold,
    Respond(Vec<u8>),
    RespondAndClose(Vec<u8>),
    RespondWithAddressAndClose(fn(SocketAddr) -> Vec<u8>),
    RespondAndKeepAlive(Vec<u8>),
    RespondPartial {
        body: Vec<u8>,
        frame_prefix_len: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub frame: Vec<u8>,
}

pub struct ScriptedBroker {
    address: SocketAddr,
    task: JoinHandle<io::Result<Vec<ObservedRequest>>>,
    shutdown: Option<watch::Sender<bool>>,
}

struct IncomingRequest {
    frame: Vec<u8>,
    response: oneshot::Sender<ConnectionResponse>,
}

struct ConnectionResponse {
    frame: Option<Vec<u8>>,
    close: bool,
    partial_frame_prefix_len: Option<usize>,
}

impl ScriptedBroker {
    pub async fn start(steps: Vec<ScriptedResponse>) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut observations = Vec::with_capacity(steps.len());
            for step in steps {
                let request = loop {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                return Ok(observations);
                            }
                        }
                        accepted = listener.accept() => {
                            let (stream, _) = accepted?;
                            tokio::spawn(serve_connection(
                                stream,
                                request_tx.clone(),
                                shutdown_rx.clone(),
                            ));
                        }
                        request = request_rx.recv() => {
                            let Some(request) = request else {
                                return Ok(observations);
                            };
                            break request;
                        }
                    }
                };
                let frame = request.frame;
                if frame.len() < 8 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Kafka request frame is shorter than its header",
                    ));
                }
                let observation = ObservedRequest {
                    api_key: i16::from_be_bytes([frame[0], frame[1]]),
                    api_version: i16::from_be_bytes([frame[2], frame[3]]),
                    correlation_id: i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]),
                    frame,
                };
                let correlation_id = observation.correlation_id;
                observations.push(observation);

                let (response_body, close_connection, partial_frame_prefix_len) = match step {
                    ScriptedResponse::Drop => (None, true, None),
                    ScriptedResponse::Hold => (None, false, None),
                    ScriptedResponse::Respond(body) => (Some(body), false, None),
                    ScriptedResponse::RespondAndClose(body) => (Some(body), true, None),
                    ScriptedResponse::RespondWithAddressAndClose(factory) => {
                        (Some(factory(address)), true, None)
                    }
                    ScriptedResponse::RespondAndKeepAlive(body) => (Some(body), false, None),
                    ScriptedResponse::RespondPartial {
                        body,
                        frame_prefix_len,
                    } => (Some(body), true, Some(frame_prefix_len)),
                };
                let frame = response_body.map(|body| {
                    let mut response = correlation_id.to_be_bytes().to_vec();
                    response.extend(body);
                    response
                });
                request
                    .response
                    .send(ConnectionResponse {
                        frame,
                        close: close_connection,
                        partial_frame_prefix_len,
                    })
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "scripted broker response receiver dropped",
                        )
                    })?;
            }

            let _ = shutdown_rx.changed().await;
            Ok(observations)
        });

        Ok(Self {
            address,
            task,
            shutdown: Some(shutdown_tx),
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub async fn finish(mut self) -> io::Result<Vec<ObservedRequest>> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(true);
        }
        self.task
            .await
            .map_err(|error| io::Error::other(format!("scripted broker task failed: {error}")))?
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    requests: mpsc::UnboundedSender<IncomingRequest>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let frame = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
                continue;
            }
            frame = read_frame(&mut stream) => match frame {
                Ok(frame) => frame,
                Err(_) => return,
            }
        };
        let (response_tx, response_rx) = oneshot::channel();
        if requests
            .send(IncomingRequest {
                frame,
                response: response_tx,
            })
            .is_err()
        {
            return;
        }
        let response = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
                continue;
            }
            response = response_rx => match response {
                Ok(response) => response,
                Err(_) => return,
            }
        };
        if let Some(frame) = response.frame {
            if let Some(prefix_len) = response.partial_frame_prefix_len {
                if write_partial_frame(&mut stream, &frame, prefix_len)
                    .await
                    .is_err()
                {
                    return;
                }
            } else if write_frame(&mut stream, &frame).await.is_err() {
                return;
            }
        }
        if response.close {
            return;
        }
    }
}

async fn read_frame<R>(stream: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut size = [0_u8; 4];
    stream.read_exact(&mut size).await?;
    let size = i32::from_be_bytes(size);
    if !(8..=16 * 1024 * 1024).contains(&size) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Kafka request frame length is outside the test harness limit",
        ));
    }
    let mut frame = vec![
        0_u8;
        usize::try_from(size).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Kafka request length overflow")
        })?
    ];
    stream.read_exact(&mut frame).await?;
    Ok(frame)
}

async fn write_frame<W>(stream: &mut W, frame: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let size = i32::try_from(frame.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Kafka response too large"))?;
    stream.write_all(&size.to_be_bytes()).await?;
    stream.write_all(frame).await?;
    stream.flush().await
}

async fn write_partial_frame<W>(stream: &mut W, frame: &[u8], prefix_len: usize) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let size = i32::try_from(frame.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Kafka response too large"))?;
    let prefix_len = prefix_len.min(frame.len().saturating_sub(1));
    stream.write_all(&size.to_be_bytes()).await?;
    stream.write_all(&frame[..prefix_len]).await?;
    stream.flush().await
}
