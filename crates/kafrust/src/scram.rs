use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::distributions::Alphanumeric;
use rand::Rng;
use sha2::{Digest, Sha256, Sha512};

const CHANNEL_BINDING: &str = "biws";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScramHash {
    Sha256,
    Sha512,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClientFirst {
    pub message: String,
    pub bare: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClientFinal {
    pub message: String,
    pub expected_server_signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScramError {
    MissingAttribute(&'static str),
    InvalidAttribute(&'static str),
    NonceMismatch,
    InvalidServerSignature,
    Crypto,
}

impl ScramError {
    pub(crate) fn safe_reason(&self) -> &'static str {
        match self {
            Self::MissingAttribute(attribute) => match *attribute {
                "r" => "missing nonce",
                "s" => "missing salt",
                "i" => "missing iteration count",
                "v" => "missing server signature",
                _ => "missing SCRAM attribute",
            },
            Self::InvalidAttribute(attribute) => match *attribute {
                "s" => "invalid salt",
                "i" => "invalid iteration count",
                "v" => "invalid server signature",
                "m" => "unsupported SCRAM extension",
                _ => "invalid SCRAM attribute",
            },
            Self::NonceMismatch => "server nonce did not extend client nonce",
            Self::InvalidServerSignature => "server signature did not match",
            Self::Crypto => "SCRAM crypto operation failed",
        }
    }
}

pub(crate) fn generate_nonce() -> String {
    rand::thread_rng()
        .sample_iter(Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

pub(crate) fn client_first(username: &str, nonce: &str) -> ClientFirst {
    let bare = format!("n={},r={nonce}", escape_username(username));
    ClientFirst {
        message: format!("n,,{bare}"),
        bare,
    }
}

pub(crate) fn client_final(
    hash: ScramHash,
    password: &str,
    client_first_bare: &str,
    client_nonce: &str,
    server_first: &str,
) -> Result<ClientFinal, ScramError> {
    let server_first = parse_server_first(server_first, client_nonce)?;
    let client_final_without_proof = format!("c={CHANNEL_BINDING},r={}", server_first.nonce);
    let auth_message = format!(
        "{client_first_bare},{},{}",
        server_first.raw, client_final_without_proof
    );

    let salted_password = salted_password(hash, password.as_bytes(), &server_first)?;
    let client_key = hmac(hash, &salted_password, b"Client Key")?;
    let stored_key = hash_bytes(hash, &client_key);
    let client_signature = hmac(hash, &stored_key, auth_message.as_bytes())?;
    let client_proof = xor_bytes(&client_key, &client_signature);
    let server_key = hmac(hash, &salted_password, b"Server Key")?;
    let server_signature = hmac(hash, &server_key, auth_message.as_bytes())?;

    Ok(ClientFinal {
        message: format!(
            "{client_final_without_proof},p={}",
            BASE64.encode(client_proof)
        ),
        expected_server_signature: server_signature,
    })
}

pub(crate) fn verify_server_final(
    expected_server_signature: &[u8],
    server_final: &str,
) -> Result<(), ScramError> {
    let encoded = find_attribute(server_final, "v")?.ok_or(ScramError::MissingAttribute("v"))?;
    let actual = BASE64
        .decode(encoded)
        .map_err(|_| ScramError::InvalidAttribute("v"))?;
    if actual == expected_server_signature {
        Ok(())
    } else {
        Err(ScramError::InvalidServerSignature)
    }
}

fn escape_username(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

#[derive(Debug)]
struct ServerFirst {
    raw: String,
    nonce: String,
    salt: Vec<u8>,
    iterations: u32,
}

fn parse_server_first(message: &str, client_nonce: &str) -> Result<ServerFirst, ScramError> {
    if find_attribute(message, "m")?.is_some() {
        return Err(ScramError::InvalidAttribute("m"));
    }

    let nonce = find_attribute(message, "r")?.ok_or(ScramError::MissingAttribute("r"))?;
    if !nonce.starts_with(client_nonce) {
        return Err(ScramError::NonceMismatch);
    }

    let salt = find_attribute(message, "s")?.ok_or(ScramError::MissingAttribute("s"))?;
    let salt = BASE64
        .decode(salt)
        .map_err(|_| ScramError::InvalidAttribute("s"))?;

    let iterations = find_attribute(message, "i")?.ok_or(ScramError::MissingAttribute("i"))?;
    let iterations = iterations
        .parse::<u32>()
        .map_err(|_| ScramError::InvalidAttribute("i"))?;
    if iterations == 0 {
        return Err(ScramError::InvalidAttribute("i"));
    }

    Ok(ServerFirst {
        raw: message.to_owned(),
        nonce: nonce.to_owned(),
        salt,
        iterations,
    })
}

fn find_attribute<'a>(message: &'a str, key: &str) -> Result<Option<&'a str>, ScramError> {
    let mut found = None;
    for attribute in message.split(',') {
        let (attribute_key, value) = attribute
            .split_once('=')
            .ok_or(ScramError::InvalidAttribute("attribute"))?;
        if attribute_key == key {
            if found.is_some() {
                return Err(ScramError::InvalidAttribute("duplicate attribute"));
            }
            found = Some(value);
        }
    }
    Ok(found)
}

fn salted_password(
    hash: ScramHash,
    password: &[u8],
    server_first: &ServerFirst,
) -> Result<Vec<u8>, ScramError> {
    Ok(derive_salted_password(
        hash,
        password,
        &server_first.salt,
        server_first.iterations,
    ))
}

pub(crate) fn derive_salted_password(
    hash: ScramHash,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> Vec<u8> {
    match hash {
        ScramHash::Sha256 => {
            let mut output = [0u8; 32];
            pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut output);
            output.to_vec()
        }
        ScramHash::Sha512 => {
            let mut output = [0u8; 64];
            pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut output);
            output.to_vec()
        }
    }
}

fn hmac(hash: ScramHash, key: &[u8], message: &[u8]) -> Result<Vec<u8>, ScramError> {
    match hash {
        ScramHash::Sha256 => {
            let mut mac =
                <Hmac<Sha256> as Mac>::new_from_slice(key).map_err(|_| ScramError::Crypto)?;
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        ScramHash::Sha512 => {
            let mut mac =
                <Hmac<Sha512> as Mac>::new_from_slice(key).map_err(|_| ScramError::Crypto)?;
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        }
    }
}

fn hash_bytes(hash: ScramHash, bytes: &[u8]) -> Vec<u8> {
    match hash {
        ScramHash::Sha256 => Sha256::digest(bytes).to_vec(),
        ScramHash::Sha512 => Sha512::digest(bytes).to_vec(),
    }
}

fn xor_bytes(left: &[u8], right: &[u8]) -> Vec<u8> {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| left ^ right)
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{client_final, client_first, verify_server_final, ScramError, ScramHash, BASE64};
    use base64::Engine as _;

    const CLIENT_NONCE: &str = "fyko+d2lbbFgONRv9qkxdawL";
    const SERVER_FIRST: &str =
        "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";

    #[test]
    fn builds_client_first_message_with_escaped_username() {
        let first = client_first("user,name=1", CLIENT_NONCE);

        assert_eq!(
            first.message,
            "n,,n=user=2Cname=3D1,r=fyko+d2lbbFgONRv9qkxdawL"
        );
        assert_eq!(first.bare, "n=user=2Cname=3D1,r=fyko+d2lbbFgONRv9qkxdawL");
    }

    #[test]
    fn builds_scram_sha256_client_final_message() {
        let first = client_first("user", CLIENT_NONCE);
        let final_message = client_final(
            ScramHash::Sha256,
            "pencil",
            &first.bare,
            CLIENT_NONCE,
            SERVER_FIRST,
        )
        .unwrap();

        assert_eq!(
            final_message.message,
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=qQRLRHGPDGjB+7iVAE7NNi5xEoHKHuLCHPNQ8BTmvds="
        );
        assert_eq!(
            BASE64.encode(final_message.expected_server_signature),
            "XKW6VuW1FANROQabnJBz1KaeCnQL/HZByQtX/iU+o30="
        );
    }

    #[test]
    fn builds_scram_sha512_client_final_message() {
        let first = client_first("user", CLIENT_NONCE);
        let final_message = client_final(
            ScramHash::Sha512,
            "pencil",
            &first.bare,
            CLIENT_NONCE,
            SERVER_FIRST,
        )
        .unwrap();

        assert_eq!(
            final_message.message,
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=VdS8LkrURiej1tG6iX+fqCXQfUnBb//d9llXYaH+ylUbDwBUz9geyR9fC4TewskRUM2tlYSalhAT4Aay1Q5dTA=="
        );
        assert_eq!(
            BASE64.encode(final_message.expected_server_signature),
            "14PAAuavk9hxBEkgB0brDxUhvWu+N16meYk+qxVNFqchR8QPohM09Y4Z6WaTCuX4C6nqMB9KIJTDm6RpSM990g=="
        );
    }

    #[test]
    fn verifies_server_final_signature() {
        let first = client_first("user", CLIENT_NONCE);
        let final_message = client_final(
            ScramHash::Sha256,
            "pencil",
            &first.bare,
            CLIENT_NONCE,
            SERVER_FIRST,
        )
        .unwrap();

        verify_server_final(
            &final_message.expected_server_signature,
            "v=XKW6VuW1FANROQabnJBz1KaeCnQL/HZByQtX/iU+o30=",
        )
        .unwrap();
    }

    #[test]
    fn rejects_nonce_mismatch() {
        let first = client_first("user", CLIENT_NONCE);
        let error = client_final(
            ScramHash::Sha256,
            "pencil",
            &first.bare,
            CLIENT_NONCE,
            "r=other,s=QSXCR+Q6sek8bf92,i=4096",
        )
        .unwrap_err();

        assert_eq!(error, ScramError::NonceMismatch);
    }

    #[test]
    fn rejects_bad_server_signature() {
        let error = verify_server_final(&[1, 2, 3], "v=AAAA").unwrap_err();

        assert_eq!(error, ScramError::InvalidServerSignature);
    }
}
