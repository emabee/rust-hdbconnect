use super::authenticator::Authenticator;
use super::crypto_util::*;
use byteorder::{BigEndian, WriteBytesExt};
use protocol::parts::authfields::AuthFields;
use protocol::util;
use rand::{thread_rng, RngCore};
use secstr::SecStr;
use std::io;
use {HdbError, HdbResult};

const CLIENT_PROOF_SIZE: usize = 32;

pub struct ScramSha256 {
    client_challenge: Vec<u8>,
    server_proof: Option<Vec<u8>>,
}
impl ScramSha256 {
    pub fn new() -> Box<Authenticator> {
        let mut client_challenge = [0u8; 64];
        let mut rng = thread_rng();
        rng.fill_bytes(&mut client_challenge);
        Box::new(ScramSha256 {
            client_challenge: client_challenge.to_vec(),
            server_proof: None,
        })
    }
}
impl Authenticator for ScramSha256 {
    fn name(&self) -> &str {
        "SCRAMSHA256"
    }

    fn name_as_bytes(&self) -> Vec<u8> {
        self.name().as_bytes().to_owned()
    }

    fn client_challenge(&self) -> &[u8] {
        &(self.client_challenge)
    }

    fn client_proof(&mut self, server_data: &[u8], password: &SecStr) -> HdbResult<Vec<u8>> {
        let (salt, server_nonce) = parse_first_server_data(server_data).unwrap();

        let (client_proof, server_proof) =
            scram_sha256(&salt, &server_nonce, &self.client_challenge, password)?;

        self.client_challenge.clear();
        self.server_proof = Some(server_proof);

        let buf = Vec::<u8>::with_capacity(3 + CLIENT_PROOF_SIZE);
        let mut w = io::Cursor::new(buf);
        w.write_u16::<BigEndian>(1_u16)?;

        w.write_u8(CLIENT_PROOF_SIZE as u8)?;
        util::serialize_bytes(&client_proof, &mut w)?;

        Ok(w.into_inner())
    }

    fn verify_server(&self, server_proof: &[u8]) -> HdbResult<()> {
        if server_proof.is_empty() {
            Ok(())
        } else {
            Err(HdbError::Impl(format!(
                "verify_server(): non-empty server_proof: {:?}",
                server_proof
            )))
        }
    }
}

// `server_data` is again an AuthFields; contains salt, and server_nonce
// key
fn parse_first_server_data(server_data: &[u8]) -> HdbResult<(Vec<u8>, Vec<u8>)> {
    let mut rdr = io::Cursor::new(server_data);
    let mut af = AuthFields::parse(&mut rdr)?;
    if af.len() != 2 {
        return Err(HdbError::Impl(format!(
            "got {} auth fields instead of 2",
            af.len()
        )));
    }
    let server_nonce = af.pop().unwrap();
    let salt = af.pop().unwrap();
    Ok((salt, server_nonce))
}

#[cfg(test)]
mod tests {
    use super::ScramSha256;
    use authentication::authenticator::Authenticator;
    use secstr::SecStr;

    // cargo
    // test authenticate::scram_sha256::tests::test_client_proof -- --nocapture
    #[test]
    fn test_client_proof() {
        info!("test calculation of client proof");
        let client_challenge: Vec<u8> = b"\xb5\xab\x3a\x90\xc5\xad\xb8\x04\x15\x27\
            \x37\x66\x54\xd7\x5c\x31\x94\xd8\x61\x50\
            \x3f\xe0\x8d\xff\x8b\xea\xd5\x1b\xc3\x5a\
            \x07\xcc\x63\xed\xbf\xa9\x5d\x03\x62\xf5\
            \x6f\x1a\x48\x2e\x4c\x3f\xb8\x32\xe4\x1c\
            \x89\x74\xf9\x02\xef\x87\x38\xcc\x74\xb6\
            \xef\x99\x2e\x8e"
            .to_vec();
        let server_challenge: Vec<u8> = b"\x02\x00\x10\x12\x41\xe5\x8f\x39\x23\x4e\
            \xeb\x77\x3e\x90\x90\x33\xe5\xcb\x6e\x30\
            \x1a\xce\xdc\xdd\x05\xc1\x90\xb0\xf0\xd0\
            \x7d\x81\x1a\xdb\x0d\x6f\xed\xa8\x87\x59\
            \xc2\x94\x06\x0d\xae\xab\x3f\x62\xea\x4b\
            \x16\x6a\xc9\x7e\xfc\x9a\x6b\xde\x4f\xe9\
            \xe5\xda\xcc\xb5\x0a\xcf\xce\x56"
            .to_vec();
        let password = SecStr::from("manager");
        let correct_client_proof: Vec<u8> = b"\x00\x01\x20\x17\x26\x25\xab\x29\x71\xd8\
            \x58\x74\x32\x5d\x21\xbc\x3d\x68\x37\x71\
            \x80\x5c\x9a\xfe\x38\xd0\x95\x1d\xad\x46\
            \x53\x00\x9c\xc9\x21"
            .to_vec();

        let mut a = ScramSha256 {
            client_challenge: client_challenge,
            server_proof: None,
        };
        let my_client_proof = a.client_proof(&server_challenge, &password).unwrap();

        assert_eq!(my_client_proof, correct_client_proof);
    }
}
