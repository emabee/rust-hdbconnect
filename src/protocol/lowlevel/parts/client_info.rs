use super::typed_value::{serialize_length_and_string, string_length};

use HdbResult;

use std::collections::HashMap;
use std::io;

#[derive(Clone, Debug, Default)]
pub struct ClientInfo(HashMap<ClientInfoKey, String>);

impl ClientInfo {
    pub fn set_application(&mut self, application: &str) {
        self.set(ClientInfoKey::Application, application);
    }
    pub fn set_application_version(&mut self, application_version: &str) {
        self.set(ClientInfoKey::ApplicationVersion, application_version);
    }
    pub fn set_application_source(&mut self, application_source: &str) {
        self.set(ClientInfoKey::ApplicationSource, application_source);
    }
    pub fn set_application_user(&mut self, application_user: &str) {
        self.set(ClientInfoKey::ApplicationUser, application_user);
    }

    pub fn serialize(&self, w: &mut io::Write) -> HdbResult<()> {
        for (key, value) in &self.0 {
            serialize_length_and_string(key.get_string(), w)?;
            serialize_length_and_string(value, w)?;
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        let mut len = 0;
        for (key, value) in &self.0 {
            len += string_length(key.get_string()) + string_length(value);
        }
        len
    }
    pub fn count(&self) -> usize {
        self.0.len() * 2
    }

    fn set(&mut self, key: ClientInfoKey, value: &str) {
        let value = value.to_string();
        self.0.insert(key, value);
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ClientInfoKey {
    Application,
    ApplicationVersion,
    ApplicationSource,
    ApplicationUser,
}
impl ClientInfoKey {
    fn get_string(&self) -> &str {
        match &self {
            ClientInfoKey::Application => "APPLICATION",
            ClientInfoKey::ApplicationVersion => "APPLICATIONVERSION",
            ClientInfoKey::ApplicationSource => "APPLICATIONSOURCE",
            ClientInfoKey::ApplicationUser => "APPLICATIONUSER",
        }
    }
}
