use anyhow::Context;
use hdbconnect::{ConnectParamsBuilder, Connection};
use log::debug;
use rand::Rng;
use std::{
    fmt::Display,
    io::{self, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

fn main() {
    // Explain purpose
    println!(
        r#"
This program prepares a HANA database for running the hdbconnect and hdbconnect_async tests
and creates a corresponding config file for the tests.
    
The program asks interactively for the information it needs for connecting to the database,
i.e. host, port, and the credentials of a highly privileged user
(e.g. SYSTEM in HANA 2 databases, and DBADMIN in HANA Cloud databases).

The program also creates a config file "./.private/test_<nick_name>.db",
which contains the connection parameters for the created users.

It then creates two users with necessary privileges and generated passwords for being
used by the tests:
- A standard user, which can be used for most tests.
- A UM user, which has the privilege to create users and which is used for some user management tests.

A warning will be displayed if the config file exists already, 
and you can decide to either use a different nick name or to overwrite the existing file.

If you decide to overwrite the existing file, then also the existing users will be dropped and recreated.
Otherwise, existing users will not be touched and let the program fail.

In order to use the created file with the hdbconnect tests, 
specify the nick name in the environment variable `HDBCONNECT_TEST_DB`.

Example:
export HDBCONNECT_TEST_DB=my_nickname
"#
    );

    let db_host = ask_for_input("DNS name of the Host", "".to_string());
    let db_port = ask_for_input("Port", 443);
    let setup_user = ask_for_input("User", "DBADMIN".to_string());
    let setup_pw = ask_for_input("Password", "".to_string());
    let use_tls = ask_for_input("Use TLS?", true);

    let (nick_name, config_file_path) = loop {
        let nick_name = ask_for_input("Nick name for the config file", "latest".to_string())
            .to_ascii_lowercase();
        let config_file_path = PathBuf::from(format!("./.private/test_{nick_name}.db"));
        if config_file_path.exists() {
            println!(
                "Warning: The config file '{}' already exists. It will be overwritten.",
                config_file_path.display()
            );

            let overwrite = ask_for_input(
                "Do you want to overwrite the existing file? (y/n)",
                "n".to_string(),
            );
            if overwrite.eq_ignore_ascii_case("y") {
                println!("Overwriting existing config file and users.");
                break (nick_name, config_file_path);
            }
        } else {
            break (nick_name, config_file_path);
        }
    };

    // Create our main connection to the database
    let copabu = ConnectParamsBuilder::new()
        .with_hostname(db_host.clone())
        .with_port(db_port)
        .with_tls_without_server_verification(use_tls);

    let sys_conn = Connection::new(
        copabu
            .clone()
            .with_dbuser(setup_user.clone())
            .with_password(setup_pw.clone()),
    )
    .context("connect to the database")
    .unwrap();
    debug!("Connected to the database.");

    let is_hana_cloud = sys_conn
        .query("select * from privileges where NAME = 'CREATE TENANT'")
        .context("check if HANA Cloud")
        .unwrap()
        .total_number_of_rows()
        .unwrap()
        > 0;

    // Create users for testing
    let std_user = format!("{}_STD_USER", nick_name.to_uppercase());
    let std_pw = generate_password(24);
    let um_user = format!("{}_UM_USER", nick_name.to_uppercase());
    let um_pw = generate_password(24);

    // cleanup
    sys_conn.multiple_statements_ignore_err(vec![
        format!("DROP USER {std_user}"),
        format!("DROP USER {um_user}"),
    ]);

    sys_conn
        .exec(format!(
            "CREATE USER {std_user} PASSWORD \"{std_pw}\" NO FORCE_FIRST_PASSWORD_CHANGE"
        ))
        .context(format!("create user {std_user}"))
        .unwrap();

    sys_conn
        .exec(format!(
            "CREATE USER {um_user} PASSWORD \"{um_pw}\" NO FORCE_FIRST_PASSWORD_CHANGE"
        ))
        .context(format!("create user {um_user}"))
        .unwrap();

    if is_hana_cloud {
        sys_conn
            .exec(format!("GRANT OPERATOR ON USERGROUP DEFAULT TO {um_user}"))
            .context(format!("grant um privilege to user {um_user}"))
            .unwrap();
    } else {
        // TODO maybe we need to do something for HANA 2 as well?
    }

    write_config_file(&config_file_path, copabu, std_user, std_pw, um_user, um_pw);
    println!("New config file created at: {}", config_file_path.display());
    println!(
        "In order to use it with the hdbconnect tests, \
        execute `export HDBCONNECT_TEST_DB={nick_name}`."
    );
}

// Helper method
fn ask_for_input<T>(prompt: &str, default: T) -> T::Owned
where
    T: FromStr + ToOwned + Display,
{
    print!("{prompt} [{default}]: ");
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).ok();

    if input.trim().is_empty() {
        default.to_owned()
    } else {
        input.trim().parse::<T>().unwrap_or(default).to_owned()
    }
}

fn generate_password(length: usize) -> String {
    let charset = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";
    let mut rng = rand::rng();
    (0..length)
        .map(|_| charset[rng.random_range(0..charset.len())] as char)
        .collect()
}

fn write_config_file(
    config_file_path: &Path,
    copabu: ConnectParamsBuilder,
    std_user: String,
    std_pw: String,
    um_user: String,
    um_pw: String,
) {
    let folder = config_file_path
        .parent()
        .context("determine directory for config file")
        .unwrap();
    std::fs::create_dir_all(folder)
        .context("create directory for config file")
        .unwrap();

    std::fs::write(
        config_file_path,
        format!(
            r#"
{{
    "direct_url":"{direct_url}",
    "redirect_url":"{redirect_url}",
    "std":{{"name":"{std_user}","pw":"{std_pw}"}},
    "um":{{"name":"{um_user}","pw":"{um_pw}"}}
}}
"#,
            direct_url = copabu,
            redirect_url = copabu.clone().with_dbname("H00"),
        ),
    )
    .context("write config file")
    .unwrap();
}
