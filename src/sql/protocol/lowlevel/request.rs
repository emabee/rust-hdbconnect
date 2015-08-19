// use super::dberr::*;
use super::init::*;
use super::message::*;

pub enum DbRequest {
    Init(InitRequest),
    Msg(Message),
}

pub enum DbResponse {
    Init(InitResponse),
    Msg(Message),
}
