mod a_sync;
mod sync;
mod tcp_client;

use sync::plain_tcp_client::PlainTcpClient;
use sync::tls_tcp_client::TlsTcpClient;

pub(crate) use tcp_client::TcpClient;
