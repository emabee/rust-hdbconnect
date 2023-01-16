use crate::protocol::util;

pub(crate) enum LobWriteMode {
    //Offset(i64),
    Append,
    Last,
}

#[cfg(feature = "sync")]
pub(crate) fn utf8_to_cesu8_and_utf8_tail(
    mut utf8: Vec<u8>,
) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let tail_len = get_utf8_tail_len(&utf8)?;
    let tail = utf8.split_off(utf8.len() - tail_len);
    Ok((
        cesu8::to_cesu8(&String::from_utf8(utf8).map_err(util::io_error)?).to_vec(),
        tail,
    ))
}

pub(crate) fn get_utf8_tail_len(bytes: &[u8]) -> std::io::Result<usize> {
    match bytes.last() {
        None | Some(0..=127) => Ok(0),
        Some(0xC0..=0xDF) => Ok(1),
        Some(_) => {
            let len = bytes.len();
            for i in 0..len - 1 {
                let index = len - 2 - i;
                let utf8_char_start = get_utf8_char_start(&bytes[index..]);
                if let Some(char_len) = match utf8_char_start {
                    Utf8CharType::One => Some(1),
                    Utf8CharType::Two => Some(2),
                    Utf8CharType::Three => Some(3),
                    Utf8CharType::Four => Some(4),
                    Utf8CharType::NotAStart | Utf8CharType::Illegal | Utf8CharType::Empty => None,
                } {
                    return Ok(match (index + char_len).cmp(&len) {
                        std::cmp::Ordering::Greater => len - index,
                        std::cmp::Ordering::Equal => 0,
                        std::cmp::Ordering::Less => len - index - char_len,
                    });
                }
            }
            Err(util::io_error("no valid utf8 cutoff point found!"))
        }
    }
}

enum Utf8CharType {
    Empty,
    Illegal,
    NotAStart,
    One,   // ...plain ascii
    Two,   // ...two-byte char
    Three, // ...three-byte char
    Four,  // ...four-byte char
}

//   1: 0000_0000 to 0111_1111 (00 to 7F)
//cont: 1000_0000 to 1011_1111 (80 to BF)
//   2: 1100_0000 to 1101_1111 (C0 to DF)
//   3: 1110_0000 to 1110_1111 (E0 to EF)
//   4: 1111_0000 to 1111_0111 (F0 to F7)
// ill: 1111_1000 to 1111_1111 (F8 to FF)
fn get_utf8_char_start(bytes: &[u8]) -> Utf8CharType {
    match bytes.len() {
        0 => Utf8CharType::Empty,
        _ => match bytes[0] {
            0x00..=0x7F => Utf8CharType::One,
            0x80..=0xBF => Utf8CharType::NotAStart,
            0xC0..=0xDF => Utf8CharType::Two,
            0xE0..=0xEF => Utf8CharType::Three,
            0xF0..=0xF7 => Utf8CharType::Four,
            _ => Utf8CharType::Illegal,
        },
    }
}
