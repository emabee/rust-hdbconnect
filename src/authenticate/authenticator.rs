use hdb_error::HdbResult;
use secstr::SecStr;

pub trait Authenticator {
    fn name(&self) -> &str;
    fn name_as_bytes(&self) -> Vec<u8>;
    fn client_challenge(&self) -> &[u8];
    fn client_proof(&self, server_challenge: Vec<u8>, password: &SecStr) -> HdbResult<Vec<u8>>;
}
