use crate::{HdbResult, impl_err};
use secstr::SecUtf8;

pub(crate) trait Authenticator {
    fn name(&self) -> &str;

    fn name_as_bytes(&self) -> Vec<u8>;

    fn client_challenge(&self) -> &[u8];

    fn client_proof(
        &mut self,
        server_challenge_data: &[u8],
        password: &SecUtf8,
    ) -> HdbResult<Vec<u8>>;
    fn verify_server(&self, server_proof: &[u8]) -> HdbResult<()>;

    fn evaluate_second_response(&self, method: &[u8], server_proof: &[u8]) -> HdbResult<()> {
        if method == self.name().as_bytes() {
            self.verify_server(server_proof)
        } else {
            Err(impl_err!(
                "Wrong method name detected: {}",
                String::from_utf8_lossy(method)
            ))
        }
    }
}
