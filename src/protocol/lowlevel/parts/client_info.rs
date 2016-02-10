use super::PrtResult;
use super::typed_value::{parse_length_and_string, serialize_length_and_string, string_length};

use std::collections::HashMap;
use std::io::{BufRead,Write};

#[derive(Debug)]
pub struct ClientInfo (HashMap<String,String>);

impl ClientInfo {
    pub fn serialize (&self, w: &mut Write)  -> PrtResult<()> {
        for (key, value) in &self.0 {
            try!(serialize_length_and_string(&key, w));
            try!(serialize_length_and_string(&value, w));
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        let mut len = 0;
        for (key, value) in &self.0 {
            len += string_length(&key) + string_length(&value);
        }
        len
    }
    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn parse(no_of_args: i32, rdr: &mut BufRead) -> PrtResult<ClientInfo> {
        let mut map = HashMap::new();
        for _ in 0..no_of_args {
            let key = try!(parse_length_and_string(rdr));
            let value = try!(parse_length_and_string(rdr));
            map.insert(key,value);
        }
        Ok(ClientInfo(map))
    }

    pub fn set(&mut self, key: ClientInfoKey, value: String) {
        match key {
            ClientInfoKey::Application          => self.0.insert(String::from("APPLICATION"),value),
            ClientInfoKey::ApplicationVersion   => self.0.insert(String::from("APPLICATIONVERSION"),value),
            ClientInfoKey::ApplicationSource    => self.0.insert(String::from("APPLICATIONSOURCE"),value),
            ClientInfoKey::ApplicationUser      => self.0.insert(String::from("APPLICATIONUSER"),value),
        };
    }
}

pub enum ClientInfoKey {
    Application,
    ApplicationVersion,
    ApplicationSource,
    ApplicationUser,
}
