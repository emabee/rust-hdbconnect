use secstr::SecStr;
use {HdbError, HdbResult};

pub trait Authenticator {
    fn name(&self) -> &str;
    fn name_as_bytes(&self) -> Vec<u8>;
    fn client_challenge(&self) -> &[u8];
    fn client_proof(
        &mut self,
        server_challenge_data: &[u8],
        password: &SecStr,
    ) -> HdbResult<Vec<u8>>;
    fn verify_server(&self, server_proof: &[u8]) -> HdbResult<()>;
    fn evaluate_second_response(&self, method: &[u8], server_proof: &[u8]) -> HdbResult<()> {
        if method != self.name().as_bytes() {
            Err(HdbError::Impl(format!(
                "Wrong method name detected: {}",
                String::from_utf8_lossy(method)
            )))
        } else {
            self.verify_server(server_proof)
        }
    }
}
