#!/usr/bin/env python3
"""Forward Kafka TCP traffic and drop the first EndTxn response."""

import argparse
import asyncio
from pathlib import Path
import struct
from typing import Optional


END_TXN_API_KEY = 26


async def read_frame(reader: asyncio.StreamReader) -> Optional[bytes]:
    header = await reader.readexactly(4)
    (length,) = struct.unpack(">i", header)
    if length < 0:
        raise RuntimeError(f"invalid Kafka frame length {length}")
    return await reader.readexactly(length)


async def write_frame(writer: asyncio.StreamWriter, body: bytes) -> None:
    writer.write(struct.pack(">i", len(body)))
    writer.write(body)
    await writer.drain()


async def proxy_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
    drop_marker: Path,
) -> None:
    peer = client_writer.get_extra_info("peername")
    print(f"accepted client {peer}", flush=True)
    target_reader, target_writer = await asyncio.open_connection(target_host, target_port)
    print(f"connected to target {target_host}:{target_port}", flush=True)
    request_api_keys: asyncio.Queue[int] = asyncio.Queue()

    async def forward_requests() -> None:
        while True:
            body = await read_frame(client_reader)
            if body is None:
                return
            if len(body) < 4:
                raise RuntimeError("Kafka request body is shorter than its header")
            api_key = struct.unpack(">h", body[:2])[0]
            print(f"request api_key={api_key} length={len(body)}", flush=True)
            await request_api_keys.put(api_key)
            await write_frame(target_writer, body)

    async def forward_responses() -> None:
        while True:
            body = await read_frame(target_reader)
            if body is None:
                return
            api_key = await request_api_keys.get()
            print(f"response api_key={api_key} length={len(body)}", flush=True)
            if api_key == END_TXN_API_KEY and claim_drop_marker(drop_marker):
                print("forwarded EndTxn request and dropped its response", flush=True)
                return
            await write_frame(client_writer, body)

    request_task = asyncio.create_task(forward_requests())
    response_task = asyncio.create_task(forward_responses())
    done, pending = await asyncio.wait(
        (request_task, response_task), return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    for task in done:
        exception = task.exception()
        if exception is not None and not isinstance(exception, asyncio.IncompleteReadError):
            print(f"proxy connection failed: {exception}", flush=True)
    target_writer.close()
    client_writer.close()
    await target_writer.wait_closed()
    await client_writer.wait_closed()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--target-host", default="127.0.0.1")
    parser.add_argument("--target-port", type=int, required=True)
    parser.add_argument("--drop-marker", type=Path, required=True)
    args = parser.parse_args()

    async def accept_connection(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await proxy_connection(
                reader,
                writer,
                args.target_host,
                args.target_port,
                args.drop_marker,
            )
        except (ConnectionError, asyncio.IncompleteReadError) as error:
            print(f"proxy connection closed: {error}", flush=True)
            writer.close()
            await writer.wait_closed()
        except Exception as error:
            print(f"proxy connection failed: {error!r}", flush=True)
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(
        accept_connection, args.listen_host, args.listen_port
    )
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or ())
    print(f"listening on {addresses}, forwarding to {args.target_host}:{args.target_port}", flush=True)
    async with server:
        await server.serve_forever()


def claim_drop_marker(path: Path) -> bool:
    try:
        path.touch(exist_ok=False)
    except FileExistsError:
        return False
    return True


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
