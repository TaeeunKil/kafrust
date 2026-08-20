use crate::codec::{Decoder, Encoder};
use crate::error::Result;
use crate::header::RequestHeader;

/// Kafka AddRaftVoter API key.
pub const ADD_RAFT_VOTER_API_KEY: i16 = 80;
/// Kafka RemoveRaftVoter API key.
pub const REMOVE_RAFT_VOTER_API_KEY: i16 = 81;

/// One listener endpoint supplied when adding a KRaft voter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftVoterListener {
    pub name: String,
    pub host: String,
    pub port: u16,
}

/// AddRaftVoter v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRaftVoterRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub cluster_id: Option<String>,
    pub timeout_ms: i32,
    pub voter_id: i32,
    pub voter_directory_id: [u8; 16],
    pub listeners: Vec<RaftVoterListener>,
}

impl AddRaftVoterRequestV0 {
    /// Encodes the flexible v0 request header and body.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: ADD_RAFT_VOTER_API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encode_body(
            &mut encoder,
            self.cluster_id.as_deref(),
            self.timeout_ms,
            self.voter_id,
            &self.voter_directory_id,
            &self.listeners,
        )?;
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// AddRaftVoter v1 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRaftVoterRequestV1 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub cluster_id: Option<String>,
    pub timeout_ms: i32,
    pub voter_id: i32,
    pub voter_directory_id: [u8; 16],
    pub listeners: Vec<RaftVoterListener>,
    pub ack_when_committed: bool,
}

impl AddRaftVoterRequestV1 {
    /// Encodes the flexible v1 request header and body.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: ADD_RAFT_VOTER_API_KEY,
            api_version: 1,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encode_body(
            &mut encoder,
            self.cluster_id.as_deref(),
            self.timeout_ms,
            self.voter_id,
            &self.voter_directory_id,
            &self.listeners,
        )?;
        encoder.write_bool(self.ack_when_committed);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// RemoveRaftVoter v0 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveRaftVoterRequestV0 {
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub cluster_id: Option<String>,
    pub voter_id: i32,
    pub voter_directory_id: [u8; 16],
}

impl RemoveRaftVoterRequestV0 {
    /// Encodes the flexible v0 request header and body.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        RequestHeader {
            api_key: REMOVE_RAFT_VOTER_API_KEY,
            api_version: 0,
            correlation_id: self.correlation_id,
            client_id: self.client_id.clone(),
        }
        .encode_v2(&mut encoder)?;
        encoder.write_compact_nullable_string(self.cluster_id.as_deref())?;
        encoder.write_i32(self.voter_id);
        encoder.write_uuid(&self.voter_directory_id);
        encoder.write_empty_tagged_fields();
        Ok(encoder.into_bytes())
    }
}

/// AddRaftVoter response shared by v0 and v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddRaftVoterResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
}

impl AddRaftVoterResponse {
    /// Decodes a flexible AddRaftVoter response body.
    pub fn decode_body(decoder: &mut Decoder<'_>) -> Result<Self> {
        let response = Self {
            throttle_time_ms: decoder.read_i32()?,
            error_code: decoder.read_i16()?,
            error_message: decoder.read_compact_nullable_string()?,
        };
        decoder.read_tagged_fields()?;
        Ok(response)
    }
}

/// RemoveRaftVoter response.
pub type RemoveRaftVoterResponse = AddRaftVoterResponse;

fn encode_body(
    encoder: &mut Encoder,
    cluster_id: Option<&str>,
    timeout_ms: i32,
    voter_id: i32,
    voter_directory_id: &[u8; 16],
    listeners: &[RaftVoterListener],
) -> Result<()> {
    encoder.write_compact_nullable_string(cluster_id)?;
    encoder.write_i32(timeout_ms);
    encoder.write_i32(voter_id);
    encoder.write_uuid(voter_directory_id);
    encoder.write_compact_array(Some(listeners), |encoder, listener| {
        encoder.write_compact_string(&listener.name)?;
        encoder.write_compact_string(&listener.host)?;
        encoder.write_i16(listener.port as i16);
        encoder.write_empty_tagged_fields();
        Ok(())
    })?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        AddRaftVoterRequestV0, AddRaftVoterRequestV1, AddRaftVoterResponse, RaftVoterListener,
        RemoveRaftVoterRequestV0, ADD_RAFT_VOTER_API_KEY, REMOVE_RAFT_VOTER_API_KEY,
    };
    use crate::codec::{Decoder, Encoder};

    fn listener() -> RaftVoterListener {
        RaftVoterListener {
            name: "CONTROLLER".to_owned(),
            host: "controller".to_owned(),
            port: 9093,
        }
    }

    #[test]
    fn encodes_add_raft_voter_v0_wire_shape() {
        let request = AddRaftVoterRequestV0 {
            correlation_id: 7,
            client_id: Some("kafrust".to_owned()),
            cluster_id: Some("cluster".to_owned()),
            timeout_ms: 30_000,
            voter_id: 4,
            voter_directory_id: [9; 16],
            listeners: vec![listener()],
        };
        let bytes = request.encode().unwrap();
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.read_i16().unwrap(), ADD_RAFT_VOTER_API_KEY);
        assert_eq!(decoder.read_i16().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 7);
        assert_eq!(
            decoder.read_nullable_string().unwrap().as_deref(),
            Some("kafrust")
        );
        decoder.read_tagged_fields().unwrap();
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap().as_deref(),
            Some("cluster")
        );
        assert_eq!(decoder.read_i32().unwrap(), 30_000);
        assert_eq!(decoder.read_i32().unwrap(), 4);
        assert_eq!(decoder.read_uuid().unwrap(), [9; 16]);
        let listeners = decoder
            .read_compact_array("listeners", |decoder| {
                let listener = (
                    decoder.read_compact_string()?,
                    decoder.read_compact_string()?,
                    decoder.read_i16()? as u16,
                );
                decoder.read_tagged_fields()?;
                Ok(listener)
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            listeners,
            vec![("CONTROLLER".to_owned(), "controller".to_owned(), 9093)]
        );
        decoder.read_tagged_fields().unwrap();
        assert!(decoder.is_empty());
    }

    #[test]
    fn encodes_add_raft_voter_v1_ack_flag() {
        let request = AddRaftVoterRequestV1 {
            correlation_id: 8,
            client_id: None,
            cluster_id: None,
            timeout_ms: 1,
            voter_id: 2,
            voter_directory_id: [0; 16],
            listeners: Vec::new(),
            ack_when_committed: true,
        };
        let bytes = request.encode().unwrap();
        assert_eq!(&bytes[..4], &[0, ADD_RAFT_VOTER_API_KEY as u8, 0, 1]);
        assert_eq!(*bytes.last().unwrap(), 0);
        assert!(bytes.windows(2).any(|window| window == [1, 0]));
    }

    #[test]
    fn encodes_remove_raft_voter_v0_wire_shape() {
        let request = RemoveRaftVoterRequestV0 {
            correlation_id: 9,
            client_id: None,
            cluster_id: Some("cluster".to_owned()),
            voter_id: 2,
            voter_directory_id: [3; 16],
        };
        let bytes = request.encode().unwrap();
        let mut decoder = Decoder::new(&bytes);
        assert_eq!(decoder.read_i16().unwrap(), REMOVE_RAFT_VOTER_API_KEY);
        assert_eq!(decoder.read_i16().unwrap(), 0);
        assert_eq!(decoder.read_i32().unwrap(), 9);
        assert_eq!(decoder.read_nullable_string().unwrap(), None);
        decoder.read_tagged_fields().unwrap();
        assert_eq!(
            decoder.read_compact_nullable_string().unwrap().as_deref(),
            Some("cluster")
        );
        assert_eq!(decoder.read_i32().unwrap(), 2);
        assert_eq!(decoder.read_uuid().unwrap(), [3; 16]);
        decoder.read_tagged_fields().unwrap();
        assert!(decoder.is_empty());
    }

    #[test]
    fn decodes_raft_voter_response() {
        let mut encoder = Encoder::new();
        encoder.write_i32(12);
        encoder.write_i16(0);
        encoder.write_compact_nullable_string(Some("ok")).unwrap();
        encoder.write_empty_tagged_fields();
        let bytes = encoder.into_bytes();
        let mut decoder = Decoder::new(&bytes);
        let response = AddRaftVoterResponse::decode_body(&mut decoder).unwrap();
        assert_eq!(response.throttle_time_ms, 12);
        assert_eq!(response.error_code, 0);
        assert_eq!(response.error_message.as_deref(), Some("ok"));
        assert!(decoder.is_empty());
    }
}
