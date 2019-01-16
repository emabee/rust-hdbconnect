use crate::HdbResult;
use byteorder::ReadBytesExt;
use cesu8;
use std::io;
use std::iter::repeat;

/// Read n bytes from a `BufRead`, return as Vec<u8>
pub fn parse_bytes(len: usize, rdr: &mut io::BufRead) -> HdbResult<Vec<u8>> {
    let mut vec: Vec<u8> = repeat(255u8).take(len).collect();
    {
        let rf: &mut [u8] = &mut vec;
        rdr.read_exact(rf)?;
    }
    Ok(vec)
}

pub fn skip_bytes(n: usize, rdr: &mut io::BufRead) -> HdbResult<()> {
    for _ in 0..n {
        rdr.read_u8()?;
    }
    Ok(())
}

// --- CESU8 Stuff --- //

// Consumes the cesu8 bytes, returns a String with minimal allocation
pub fn string_from_cesu8(bytes: Vec<u8>) -> HdbResult<String> {
    match String::from_utf8(bytes) {
        Ok(s) => Ok(s),
        Err(e) => Ok(cesu8::from_cesu8(e.as_bytes())?.to_string()),
    }
}

/// cesu-8 is identical to utf-8, except for high code points
/// which consume 4 bytes in utf-8 and 6 in cesu-8;
/// the first byte of such a code point in utf8 has the bit pattern 11110xxx
/// (240 -247)
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
        0x00...0x7F | 0xC0...0xDF | 0xE0...0xEF | 0xF0...0xF7 => true,
        _ => false,
    }
}

pub fn count_1_2_3_sequence_starts(cesu8: &[u8]) -> usize {
    cesu8.iter().filter(|b| is_utf8_char_start(**b)).count()
}

pub fn to_string_and_surrogate(cesu8: &[u8]) -> HdbResult<(String, Option<[u8; 3]>)> {
    let (utf8, buffer_cesu8) = to_string_and_tail(cesu8).unwrap(/* yes */);
    let surrogate_buf = match buffer_cesu8.len() {
        0 => None,
        3 => {
            debug!("to_string_and_surrogate() found a split surrogate pair");
            let mut buffer = [0_u8; 3];
            buffer[0] = buffer_cesu8[0];
            buffer[1] = buffer_cesu8[1];
            buffer[2] = buffer_cesu8[2];
            Some(buffer)
        }
        _ => panic!("Unexpected buffer_cesu8 = {:?}", buffer_cesu8),
    };
    Ok((utf8, surrogate_buf))
}

pub fn to_string_and_tail(cesu8: &[u8]) -> HdbResult<(String, Vec<u8>)> {
    let cesu8_length = cesu8.len();
    let start = match cesu8_length {
        0...7 => 0,
        len => len - 7,
    };

    let tail_len = get_tail_len(&cesu8[start..]);
    let (a, tail) = cesu8.split_at(cesu8_length - tail_len);
    Ok((cesu8::from_cesu8(a)?.into_owned(), Vec::from(tail)))
}

fn get_tail_len(bytes: &[u8]) -> usize {
    match bytes.last() {
        None => return 0,
        Some(0...127) => return 0,
        Some(0xC0...0xDF) => return 1,
        Some(_) => {}
    }

    let len = bytes.len();
    for i in 0..len - 1 {
        let index = len - 2 - i;
        if let Some(char_len) = get_cesu8_char_len(bytes[index], bytes[index + 1]) {
            if index + char_len > len {
                return len - index;
            } else if index + char_len == len {
                return 0;
            } else {
                return len - index - char_len;
            }
        }
    }
    panic!("no valid cutoff point found for {:?}!", bytes)
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
fn get_cesu8_char_len(b1: u8, b2: u8) -> Option<usize> {
    // start of, or equal to, ...
    match (b1, b2) {
        (0x00...0x7F, _) => Some(1),           // ...plain ascii
        (0xC0...0xDF, _) => Some(2),           // ...two-byte char
        (0xED, 0xA0...0xAF) => Some(6),        // ...first half of surrogate pair
        (0xED, 0xB0...0xBF) => None,           // ...second half of surrogate pair
        (0xE0...0xEF, 0x80...0xBF) => Some(3), // ...non-surrogate three-byte char
        (_, _) => None,                        // not a start
    }
}

#[cfg(test)]
mod tests {
    use super::to_string_and_tail;
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
            let (mut f_utf8, mut tail_cesu8) = to_string_and_tail(first_cesu8).unwrap();

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
