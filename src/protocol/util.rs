use crate::types_impl::lob::CharLobSlice;
use crate::{HdbError, HdbResult};

pub(crate) fn io_error<E>(error: E) -> std::io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    std::io::Error::new(std::io::ErrorKind::Other, error)
}

// --- CESU8 Stuff --- //

// Consumes the cesu8 bytes, returns a String with minimal allocation
pub(crate) fn string_from_cesu8(bytes: Vec<u8>) -> Result<String, cesu8::Cesu8DecodingError> {
    String::from_utf8(bytes).or_else(|e| Ok(cesu8::from_cesu8(e.as_bytes())?.to_string()))
}

// cesu-8 is identical to utf-8, except for high code points
// which consume 4 bytes in utf-8 and 6 in cesu-8;
// the first byte of such a code point in utf8 has the bit pattern 11110xxx
// (240 -247)
pub(crate) fn cesu8_length(s: &str) -> usize {
    let mut len = s.len();
    for b in s.as_bytes() {
        if *b >= 240_u8 {
            // 240 = b11110000
            len += 2;
        }
    }
    len
}

// determine how many of the last characters must be cut off to ensure the remaining bytes end with
// consistent cesu-8 that can be converted into utf-8
#[allow(clippy::ptr_arg)]
pub(crate) fn get_cesu8_tail_len<T>(bytes: &T, first: usize, last: usize) -> std::io::Result<usize>
where
    T: std::fmt::Debug + std::ops::Index<usize, Output = u8>,
{
    match bytes[last] {
        0..=127 => Ok(0),     // last byte is ASCII-7, no need to cut anything off
        0xC0..=0xDF => Ok(1), // last byte is start of two-byte sequence, cut off here
        _ => {
            for index in (first..=last).rev() {
                if let Some(char_len) = match cesu8_char_type(bytes, index, last) {
                    Cesu8CharType::One => Some(1),
                    Cesu8CharType::Two => Some(2),
                    Cesu8CharType::Three => Some(3),
                    Cesu8CharType::FirstHalfOfSurrogate => Some(6),
                    Cesu8CharType::SecondHalfOfSurrogate
                    | Cesu8CharType::NotAStart
                    | Cesu8CharType::TooShort
                    | Cesu8CharType::Empty => None,
                } {
                    return Ok(match (last - index + 1).cmp(&char_len) {
                        std::cmp::Ordering::Greater => last - index + 1 - char_len,
                        std::cmp::Ordering::Equal => 0,
                        std::cmp::Ordering::Less => last - index + 1,
                    });
                }
            }
            Err(io_error("no valid cesu8 cutoff point found"))
        }
    }
}

pub(crate) fn split_off_orphaned_surrogates(cesu8: Vec<u8>) -> HdbResult<CharLobSlice> {
    let (prefix, cesu8) = match cesu8_char_type(&cesu8, 0, cesu8.len()) {
        Cesu8CharType::One
        | Cesu8CharType::Two
        | Cesu8CharType::Three
        | Cesu8CharType::FirstHalfOfSurrogate
        | Cesu8CharType::Empty => (None, cesu8),
        Cesu8CharType::SecondHalfOfSurrogate => (
            Some(vec![cesu8[0], cesu8[1], cesu8[2]]),
            cesu8[3..].to_vec(),
        ),
        Cesu8CharType::NotAStart | Cesu8CharType::TooShort => {
            return Err(HdbError::Impl("Unexpected value for NCLob"));
        }
    };
    let (data, postfix) = cesu8_to_string_and_surrogate(cesu8)?;
    Ok(CharLobSlice {
        prefix,
        data,
        postfix,
    })
}

// find first cesu8-start,
// find tail
// determine in-between (can be empty)
#[allow(clippy::ptr_arg)]
pub(crate) fn split_off_orphaned_bytes(cesu8: &Vec<u8>) -> HdbResult<CharLobSlice> {
    let mut split = 0;
    for start in 0..cesu8.len() {
        split = match cesu8_char_type(cesu8, start, cesu8.len()) {
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

fn cesu8_to_string_and_surrogate(cesu8: Vec<u8>) -> HdbResult<(String, Option<Vec<u8>>)> {
    let (utf8, buffer_cesu8) = cesu8_to_string_and_tail(cesu8).unwrap(/* yes */);
    match buffer_cesu8.len() {
        0 => Ok((utf8, None)),
        3 => {
            debug!("cesu8_to_string_and_surrogate() found a split surrogate pair");
            Ok((
                utf8,
                Some(vec![buffer_cesu8[0], buffer_cesu8[1], buffer_cesu8[2]]),
            ))
        }
        _ => Err(HdbError::ImplDetailed(format!(
            "Unexpected buffer_cesu8 = {:?}",
            buffer_cesu8
        ))),
    }
}

fn cesu8_to_string_and_tail(mut cesu8: Vec<u8>) -> HdbResult<(String, Vec<u8>)> {
    let tail_len = if cesu8.is_empty() {
        0
    } else {
        get_cesu8_tail_len(&cesu8, 0, cesu8.len() - 1)?
    };
    let tail = cesu8.split_off(cesu8.len() - tail_len);
    Ok((string_from_cesu8(cesu8)?, tail))
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
fn cesu8_char_type<T>(cesu8: &T, first: usize, last: usize) -> Cesu8CharType
where
    T: std::ops::Index<usize, Output = u8>,
{
    if first == last {
        match cesu8[first] {
            0x00..=0x7F => Cesu8CharType::One,
            0xC0..=0xDF => Cesu8CharType::Two,
            _ => Cesu8CharType::TooShort,
        }
    } else {
        match (cesu8[first], cesu8[first + 1]) {
            (0x00..=0x7F, _) => Cesu8CharType::One,
            (0xC0..=0xDF, _) => Cesu8CharType::Two,
            (0xED, 0xA0..=0xAF) => Cesu8CharType::FirstHalfOfSurrogate,
            (0xED, 0xB0..=0xBF) => Cesu8CharType::SecondHalfOfSurrogate,
            (0xE0..=0xEF, 0x80..=0xBF) => Cesu8CharType::Three,
            (_, _) => Cesu8CharType::NotAStart,
        }
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

#[cfg(test)]
mod tests {
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
                super::cesu8_to_string_and_tail(first_cesu8.to_vec()).unwrap();

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
