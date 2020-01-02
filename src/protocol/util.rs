use crate::protocol::util;
use crate::types_impl::lob::CharLobSlice;
use crate::{HdbError, HdbErrorKind, HdbResult};
use byteorder::ReadBytesExt;
use cesu8;
use failure::ResultExt;
use std::iter::repeat;

pub fn io_error<E>(error: E) -> std::io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    std::io::Error::new(std::io::ErrorKind::Other, error)
}

// Read n bytes from a `BufRead`, return as Vec<u8>
pub fn parse_bytes(len: usize, rdr: &mut dyn std::io::BufRead) -> std::io::Result<Vec<u8>> {
    let mut vec: Vec<u8> = repeat(255_u8).take(len).collect();
    {
        let rf: &mut [u8] = &mut vec;
        rdr.read_exact(rf)?;
    }
    Ok(vec)
}

pub fn skip_bytes(n: usize, rdr: &mut dyn std::io::BufRead) -> std::io::Result<()> {
    for _ in 0..n {
        rdr.read_u8()?;
    }
    Ok(())
}

// --- CESU8 Stuff --- //

// Consumes the cesu8 bytes, returns a String with minimal allocation
pub fn string_from_cesu8(bytes: Vec<u8>) -> Result<String, std::io::Error> {
    Ok(match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => cesu8::from_cesu8(e.as_bytes())
            .map_err(|e| util::io_error(e.to_string()))?
            .to_string(),
    })
}

// cesu-8 is identical to utf-8, except for high code points
// which consume 4 bytes in utf-8 and 6 in cesu-8;
// the first byte of such a code point in utf8 has the bit pattern 11110xxx
// (240 -247)
pub fn cesu8_length(s: &str) -> usize {
    let mut len = s.len();
    for b in s.as_bytes() {
        if *b >= 240_u8 {
            // 240 = b11110000
            len += 2;
        }
    }
    len
}

pub fn is_utf8_char_start(b: u8) -> bool {
    match b {
        0x00..=0x7F | 0xC0..=0xDF | 0xE0..=0xEF | 0xF0..=0xF7 => true,
        _ => false,
    }
}

pub fn count_1_2_3_sequence_starts(cesu8: &[u8]) -> usize {
    cesu8.iter().filter(|b| is_utf8_char_start(**b)).count()
}

pub fn to_string_and_surrogate(cesu8: Vec<u8>) -> HdbResult<(String, Option<Vec<u8>>)> {
    let (utf8, buffer_cesu8) = cesu8_to_string_and_tail(cesu8).unwrap(/* yes */);
    match buffer_cesu8.len() {
        0 => Ok((utf8, None)),
        3 => {
            debug!("to_string_and_surrogate() found a split surrogate pair");
            Ok((
                utf8,
                Some(vec![buffer_cesu8[0], buffer_cesu8[1], buffer_cesu8[2]]),
            ))
        }
        _ => Err(HdbError::imp_detailed(format!(
            "Unexpected buffer_cesu8 = {:?}",
            buffer_cesu8
        ))),
    }
}

pub fn cesu8_to_string_and_tail(mut cesu8: Vec<u8>) -> HdbResult<(String, Vec<u8>)> {
    let cesu8_length = cesu8.len();
    let start = match cesu8_length {
        0..=7 => 0,
        len => len - 7,
    };

    let tail_len = get_cesu8_tail_len(&cesu8[start..])?;
    let tail = cesu8.split_off(cesu8_length - tail_len);
    Ok((string_from_cesu8(cesu8).context(HdbErrorKind::Cesu8)?, tail))
}

pub fn utf8_to_cesu8_and_utf8_tail(mut utf8: Vec<u8>) -> HdbResult<(Vec<u8>, Vec<u8>)> {
    let utf8_length = utf8.len();
    let start = match utf8_length {
        0..=5 => 0,
        len => len - 5,
    };

    let tail_len = get_utf8_tail_len(&utf8[start..])?;
    let tail = utf8.split_off(utf8_length - tail_len);
    Ok((
        cesu8::to_cesu8(&String::from_utf8(utf8).context(HdbErrorKind::Cesu8)?).to_vec(),
        tail,
    ))
}

// determine how many of the last characters must be cut off to ensure the string ends with
// consistent cesu-8 that can be converted into utf-8
fn get_cesu8_tail_len(bytes: &[u8]) -> HdbResult<usize> {
    match bytes.last() {
        None | Some(0..=127) => Ok(0),
        Some(0xC0..=0xDF) => Ok(1),
        Some(_) => {
            let len = bytes.len();
            for i in 0..len - 1 {
                let index = len - 2 - i;
                let cesu8_char_start = get_cesu8_char_start(&bytes[index..]);
                if let Some(char_len) = match cesu8_char_start {
                    Cesu8CharType::One => Some(1),
                    Cesu8CharType::Two => Some(2),
                    Cesu8CharType::Three => Some(3),
                    Cesu8CharType::FirstHalfOfSurrogate => Some(6),
                    Cesu8CharType::SecondHalfOfSurrogate
                    | Cesu8CharType::NotAStart
                    | Cesu8CharType::TooShort
                    | Cesu8CharType::Empty => None,
                } {
                    return Ok(match (index + char_len).cmp(&len) {
                        std::cmp::Ordering::Greater => len - index,
                        std::cmp::Ordering::Equal => 0,
                        std::cmp::Ordering::Less => len - index - char_len,
                    });
                }
            }
            Err(HdbError::imp_detailed(format!(
                "no valid cesu8 cutoff point found for {:?}!",
                bytes,
            )))
        }
    }
}

fn get_utf8_tail_len(bytes: &[u8]) -> HdbResult<usize> {
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
            Err(HdbError::imp_detailed(format!(
                "no valid utf8 cutoff point found for {:?}!",
                bytes
            )))
        }
    }
}

// find first cesu8-start,
// find tail
// determine in-between (can be empty)
pub fn split_off_orphaned_bytes(cesu8: &[u8]) -> HdbResult<CharLobSlice> {
    let mut split = 0;
    for start in 0..cesu8.len() {
        split = match get_cesu8_char_start(&cesu8[start..]) {
            Cesu8CharType::One
            | Cesu8CharType::Two
            | Cesu8CharType::Three
            | Cesu8CharType::FirstHalfOfSurrogate
            | Cesu8CharType::Empty
            | Cesu8CharType::TooShort => start,
            Cesu8CharType::SecondHalfOfSurrogate => start + 3,
            Cesu8CharType::NotAStart => {
                continue;
            }
        };
        break;
    }
    let prefix = if split == 0 {
        None
    } else {
        Some(cesu8[0..split].to_vec())
    };
    let cesu8: Vec<u8> = cesu8[split..].to_vec();
    let (data, postfix) = cesu8_to_string_and_tail(cesu8).unwrap(/* yes */);
    let postfix = if postfix.is_empty() {
        None
    } else {
        Some(postfix)
    };
    Ok(CharLobSlice {
        prefix,
        data,
        postfix,
    })
}

pub fn split_off_orphaned_surrogates(cesu8: Vec<u8>) -> HdbResult<CharLobSlice> {
    let (prefix, cesu8) = match get_cesu8_char_start(&cesu8) {
        Cesu8CharType::One
        | Cesu8CharType::Two
        | Cesu8CharType::Three
        | Cesu8CharType::FirstHalfOfSurrogate
        | Cesu8CharType::Empty
        | Cesu8CharType::TooShort => (None, cesu8),
        Cesu8CharType::SecondHalfOfSurrogate => (
            Some(vec![cesu8[0], cesu8[1], cesu8[2]]),
            cesu8[3..].to_vec(),
        ),
        Cesu8CharType::NotAStart => {
            return Err(HdbError::imp("Unexpected value for NCLob"));
        }
    };

    let (data, postfix) = to_string_and_surrogate(cesu8)?;

    Ok(CharLobSlice {
        prefix,
        data,
        postfix,
    })
}

// First half:
//  11101101 10100000 10000000  to  11101101 10101111 10111111
//  E   D    A   0                  E   D    A   F
//
// Second half:
//  11101101 10110000 10000000  to  11101101 10111111 10111111
//  E   D    B   0					E   D    B   F
//
//  Any three byte sequence:
//  11100000 10000000 10000000  to  11101111 10111111 10111111
//  E   0    8   0                  E   F    B   F
//
fn get_cesu8_char_start(bytes: &[u8]) -> Cesu8CharType {
    match bytes.len() {
        0 => Cesu8CharType::Empty,
        1 => match bytes[0] {
            0x00..=0x7F => Cesu8CharType::One,
            0xC0..=0xDF => Cesu8CharType::Two,
            _ => Cesu8CharType::TooShort,
        },
        _ => match (bytes[0], bytes[1]) {
            (0x00..=0x7F, _) => Cesu8CharType::One,
            (0xC0..=0xDF, _) => Cesu8CharType::Two,
            (0xED, 0xA0..=0xAF) => Cesu8CharType::FirstHalfOfSurrogate,
            (0xED, 0xB0..=0xBF) => Cesu8CharType::SecondHalfOfSurrogate,
            (0xE0..=0xEF, 0x80..=0xBF) => Cesu8CharType::Three,
            (_, _) => Cesu8CharType::NotAStart,
        },
    }
}
#[derive(Debug)]
enum Cesu8CharType {
    Empty,
    TooShort,
    NotAStart,
    One,   // ...plain ascii
    Two,   // ...two-byte char
    Three, // ...non-surrogate three-byte char
    FirstHalfOfSurrogate,
    SecondHalfOfSurrogate,
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

enum Utf8CharType {
    Empty,
    Illegal,
    NotAStart,
    One,   // ...plain ascii
    Two,   // ...two-byte char
    Three, // ...three-byte char
    Four,  // ...four-byte char
}

#[cfg(test)]
mod tests {
    use super::cesu8_to_string_and_tail;
    use cesu8;

    #[test]
    fn check_tail_detection() {
        let s_utf8 =
            "¡Este código es editable y ejecutable! Ce code est modifiable et exécutable ! \
			Quest💩o codice è modificabile ed eseguibile! このコードは編集して実行出来ます！ \
            여기에서 코드를 수정하고 실행할 수 있습니다! Ten kod można edytować ora💩z uruchomić! \
            Este código é editável e execu💩💩t💩ável! Этот код можно отредактировать и запустить! \
            Bạn có thể edit và run code trực tiếp! 这段💩💩💩💩代💩💩码是可以编辑并且能够运行的！\
            Dieser Code kann bearbeitet und 💩💩💩💩💩ausgeführt werden! Den här koden kan \
			redigeras och köras! 💩T💩e💩n💩t💩o kód můžete upravit a spustit \
            این کد قابلیت ویرایش و اجرا دارد!โค้ดนี้สามารถแก้ไขได้และรัน";

        let v_cesu8 = cesu8::to_cesu8(&s_utf8);

        assert_eq!(s_utf8, cesu8::from_cesu8(&v_cesu8).unwrap());

        for i in 0..v_cesu8.len() {
            // forcefully split in two parts that may be invalid unicode
            let (first_cesu8, second_cesu8) = v_cesu8.split_at(i);

            // split the first part in valid unicode plus the tail
            let (mut f_utf8, mut tail_cesu8) =
                cesu8_to_string_and_tail(first_cesu8.to_vec()).unwrap();

            // make sure the tail is shorter than 6
            assert!(tail_cesu8.len() < 6);

            // make sure that the tail plus second are valid cesu8 again
            tail_cesu8.extend_from_slice(second_cesu8);
            let second_utf8 = String::from(cesu8::from_cesu8(&tail_cesu8).unwrap());

            // make sure that the concatenation is equal to s
            f_utf8.push_str(&second_utf8);
            assert_eq!(s_utf8, f_utf8);
        }
    }
}
