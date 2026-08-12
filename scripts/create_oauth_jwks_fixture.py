#!/usr/bin/env python3
"""Create a deterministic local RSA/JWKS fixture for live Kafka OAuth tests."""

import argparse
import base64
import json
import re
import subprocess
import time
from pathlib import Path


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--jwks-uri", required=True)
    parser.add_argument("--audience", required=True)
    parser.add_argument("--subject", required=True)
    parser.add_argument("--token-ttl-seconds", type=int, default=3600)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    key_path = args.output_dir / "signing.key"
    subprocess.run(
        ["openssl", "genrsa", "-out", str(key_path), "2048"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    key_text = subprocess.check_output(
        ["openssl", "rsa", "-in", str(key_path), "-text", "-noout"],
        text=True,
        stderr=subprocess.STDOUT,
    )
    modulus_match = re.search(r"modulus:\s*(.*?)(?:\npublicExponent:)", key_text, re.S)
    exponent_match = re.search(r"publicExponent:\s*(\d+)", key_text)
    if modulus_match is None or exponent_match is None:
        raise RuntimeError("unable to extract RSA public parameters from openssl")

    modulus_hex = re.sub(r"[^0-9a-fA-F]", "", modulus_match.group(1))
    if modulus_hex.startswith("00"):
        modulus_hex = modulus_hex[2:]
    modulus = bytes.fromhex(modulus_hex)
    exponent = int(exponent_match.group(1))
    exponent_bytes = exponent.to_bytes((exponent.bit_length() + 7) // 8, "big")

    key_id = "kafrust-live-rs256"
    now = int(time.time())
    header = {"alg": "RS256", "kid": key_id, "typ": "JWT"}
    payload = {
        "aud": args.audience,
        "exp": now + args.token_ttl_seconds,
        "iat": now,
        "iss": args.issuer,
        "sub": args.subject,
    }
    encoded_header = b64url(json.dumps(header, separators=(",", ":")).encode())
    encoded_payload = b64url(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
    signature = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", str(key_path)],
        check=True,
        input=signing_input,
        stdout=subprocess.PIPE,
    ).stdout
    token = f"{encoded_header}.{encoded_payload}.{b64url(signature)}"

    jwks = {
        "keys": [
            {
                "alg": "RS256",
                "e": b64url(exponent_bytes),
                "kid": key_id,
                "kty": "RSA",
                "n": b64url(modulus),
                "use": "sig",
            }
        ]
    }
    discovery = {
        "issuer": args.issuer,
        "jwks_uri": args.jwks_uri,
        "token_endpoint": f"{args.issuer.rstrip('/')}/token",
    }
    (args.output_dir / "jwks.json").write_text(
        json.dumps(jwks, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    (args.output_dir / "openid-configuration").write_text(
        json.dumps(discovery, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    (args.output_dir / "oauth-token").write_text(token + "\n", encoding="utf-8")
    print(f"created OAuth JWKS fixture in {args.output_dir}")


if __name__ == "__main__":
    main()
