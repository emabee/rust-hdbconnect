// commented out, as the docu says: not used by any client implementation

// use crate::protocol::parts::option_part::OptionId;
// use crate::protocol::parts::option_part::OptionPart;

// // An Options part that is used by the client when fetching result set lines;
// // the RESULTSETPOS field can be used to skip over entries.
// pub type FetchOptions = OptionPart<FetchOptionsId>;

// #[derive(Debug, Eq, PartialEq, Hash)]
// pub enum FetchOptionsId {
//     ResultSetPosition, // 1 // INT // Position for Fetch
//     __Unexpected__(u8),
// }

// impl OptionId<FetchOptionsId> for FetchOptionsId {
//     fn to_u8(&self) -> u8 {
//         match *self {
//             FetchOptionsId::ResultSetPosition => 1,
//             FetchOptionsId::__Unexpected__(val) => val,
//         }
//     }

//     fn from_u8(val: u8) -> FetchOptionsId {
//         match val {
//             1 => FetchOptionsId::ResultSetPosition,
//             val => {
//                 warn!("Unsupported value for FetchOptionsId received: {}", val);
//                 FetchOptionsId::__Unexpected__(val)
//             }
//         }
//     }
// }
