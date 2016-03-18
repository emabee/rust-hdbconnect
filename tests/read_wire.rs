// #![cfg(test)]
//
// use protocol::lowlevel::message::{Message,Metadata, MsgType,parse_message_and_sequence_header};
// use protocol::lowlevel::argument::Argument;
// use protocol::lowlevel::part::Part;
// use protocol::lowlevel::partkind::PartKind;
//
// use flexi_logger;
// use std::fs::File;
// use std::io::{Cursor,BufRead,BufReader};
//
// // uncomment the next line, and then: cargo test --test read_wire -- --nocapture
// //#[test]
// pub fn read_wire() {
//     use flexi_logger::{LogConfig,detailed_format};
//     // hdbconnect::protocol::lowlevel::resultset::deserialize=info,\
//     // hdbconnect::protocol::lowlevel::resultset=debug,\
//     flexi_logger::init(
//         LogConfig {
//             log_to_file: true,
//             format: detailed_format,
//             .. LogConfig::new()
//         },
//         // hdbconnect::protocol::lowlevel::message=trace,\
//         // hdbconnect::protocol::lowlevel::part=trace,\
//         Some("trace,\
//         ".to_string())
//     ).unwrap();
//
//     let name = "on_the_wire/prepare.wire";
//     println!("Reading task from file {}", &name);
//
//     let f = File::open(name).unwrap();
//     for line in BufReader::new(f).lines() {
//         let line = line.unwrap();
//         let line = line.trim();
//         if line.len() == 0 {
//             println!("");
//         } else if line.as_bytes()[0] == b'#' {
//             println!("{}", line);
//         }
//         else {
//             // line contains the encoded bytes, encoded as ab:cd: etc
//             let mut reader = BufReader::new(Cursor::new(to_bytes(line)));
//
//             let (no_of_parts, msg) = parse_message_and_sequence_header(&mut reader).unwrap();
//             match msg {
//                 Message::Request(mut request) => {
//                     for _ in 0..no_of_parts {
//                         let part = Part::parse(
//                             MsgType::Request, &mut (request.parts), None, Metadata::None, &mut None, &mut reader
//                         ).unwrap();
//                         request.push(part);
//                     }
//                     println!("request = {:?}", request);
//                 },
//                 Message::Reply(mut reply) => {
//                     for _ in 0..no_of_parts {
//                         match Part::parse(
//                             MsgType::Reply, &mut (reply.parts), None, Metadata::None, &mut None, &mut reader
//                         ) {
//                             Ok(part) => reply.push(part),
//                             Err(e) => reply.push(Part::new(PartKind::Error, Argument::Dummy(e))),
//                         }
//                     }
//                     println!("reply = {:?}", reply);
//                 },
//     }}}
// }
//
// fn to_bytes(line: &str) -> Vec<u8> {
//     let mut bytes = Vec::<u8>::new();
//     for xx in line.split(":") {
//         let dd = xx.as_bytes();
//         let byte = scan_digit(dd[0])*16 + scan_digit(dd[1]);
//         bytes.push(byte);
//     }
//     bytes
// }
// fn scan_digit(digit: u8) -> u8 {
//     match digit {
//         48 ... 57 => digit - 48,
//         65 ... 70 => digit - 55,
//         97 ... 102 => digit - 87,
//         _ => panic!("illegal digit"),
//     }
// }
