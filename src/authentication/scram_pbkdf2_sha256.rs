use super::authenticator::Authenticator;
use super::crypto_util::*;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use chrono::Local;
use protocol::parts::authfields::AuthFields;
use protocol::util;
use rand::{thread_rng, RngCore};
use secstr::SecStr;
use std::io;
use {HdbError, HdbResult};

const CLIENT_PROOF_SIZE: usize = 32;

pub struct ScramPbkdf2Sha256 {
    client_challenge: Vec<u8>,
    server_proof: Option<Vec<u8>>,
}
impl ScramPbkdf2Sha256 {
    pub fn new() -> Box<Authenticator> {
        let mut client_challenge = [0u8; 64];
        let mut rng = thread_rng();
        rng.fill_bytes(&mut client_challenge);
        Box::new(ScramPbkdf2Sha256 {
            client_challenge: client_challenge.to_vec(),
            server_proof: None,
        })
    }
}
impl Authenticator for ScramPbkdf2Sha256 {
    fn name(&self) -> &str {
        "SCRAMPBKDF2SHA256"
    }

    fn name_as_bytes(&self) -> Vec<u8> {
        self.name().as_bytes().to_owned()
    }

    fn client_challenge(&self) -> &[u8] {
        &(self.client_challenge)
    }

    fn client_proof(&mut self, server_data: &[u8], password: &SecStr) -> HdbResult<Vec<u8>> {
        let (salt, server_nonce, iterations) = parse_first_server_data(server_data).unwrap();

        let start = Local::now();
        let (client_proof, server_proof) = scram_pdkdf2_sha256(
            &salt,
            &server_nonce,
            &self.client_challenge,
            password,
            iterations,
        )?;
        debug!(
            "pbkdf2 took {} µs",
            Local::now()
                .signed_duration_since(start)
                .num_microseconds()
                .unwrap_or(-1)
        );

        self.client_challenge.clear();
        self.server_proof = Some(server_proof);

        let buf = Vec::<u8>::with_capacity(3 + CLIENT_PROOF_SIZE);
        let mut w = io::Cursor::new(buf);
        w.write_u16::<BigEndian>(1_u16)?;

        w.write_u8(CLIENT_PROOF_SIZE as u8)?;
        util::serialize_bytes(&client_proof, &mut w)?;

        Ok(w.into_inner())
    }

    fn verify_server(&self, server_data: &[u8]) -> HdbResult<()> {
        let mut af = AuthFields::parse(&mut io::Cursor::new(server_data))?;
        let srv_proof = af.pop().unwrap();

        if let Some(ref s_p) = self.server_proof {
            if s_p as &[u8] == &srv_proof as &[u8] {
                return Ok(());
            }
        }

        Err(HdbError::Impl(
            "Server proof failed - this can indicate a severe security issue with the server!"
                .to_string(),
        ))
    }
}

// `server_data` is again an AuthFields, contains salt, server_nonce, iterations
fn parse_first_server_data(server_data: &[u8]) -> HdbResult<(Vec<u8>, Vec<u8>, u32)> {
    let mut af = AuthFields::parse(&mut io::Cursor::new(server_data))?;
    if af.len() != 3 {
        return Err(HdbError::Impl(format!(
            "got {} auth fields instead of 3",
            af.len()
        )));
    }

    let iterations = {
        let mut rdr = io::Cursor::new(af.pop().unwrap());
        rdr.read_u32::<BigEndian>()?
    };
    let server_nonce = af.pop().unwrap();
    let salt = af.pop().unwrap();

    if iterations < 15_000 {
        Err(HdbError::Impl(format!(
            "too few iterations: {}",
            iterations
        )))
    } else if salt.len() < 16 {
        Err(HdbError::Impl(format!("too little salt: {}", salt.len())))
    } else {
        Ok((salt, server_nonce, iterations))
    }
}
