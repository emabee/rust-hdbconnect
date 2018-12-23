mod test_utils;

use hdbconnect::{Connection, HdbResult, IntoConnectParams};
use log::{debug, info};
use serde_derive::{Deserialize, Serialize};

// cargo test --test test_012_tls -- --nocapture
#[test]
fn test_012_tls() -> HdbResult<()> {
    let _log_handle = test_utils::init_logger("info");
    info!("test tls");

    let mut url = test_utils::get_std_connect_url()?;
    url = url.replace("hdbsql", "hdbsqls");
    url.push_str("?tls_trust_anchor_dir=.%2F.private");
    debug!("url = {}", url);

    if cfg!(feature = "tls") {
        // debug!("not really trying tls ...");
        let conn_params = url.into_connect_params()?;
        let mut connection = Connection::new(conn_params)?;

        select_version_and_user(&mut connection)?;
    } else {
        assert!(url.into_connect_params().is_err());
        debug!("got error from trying tls, as expected");
    }

    Ok(())
}

fn select_version_and_user(connection: &mut Connection) -> HdbResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = r#"SELECT VERSION as "version", CURRENT_USER as "current_user" FROM SYS.M_DATABASE"#;
    debug!("calling connection.query(SELECT VERSION as ...)");
    let resultset = connection.query(stmt)?;
    let version_and_user: VersionAndUser = resultset.try_into()?;

    assert_eq!(
        &version_and_user.current_user,
        test_utils::get_std_connect_params()?.dbuser()
    );

    debug!("VersionAndUser: {:?}", version_and_user);
    Ok(())
}
