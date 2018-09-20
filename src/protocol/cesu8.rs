use HdbResult;

use cesu8::{from_cesu8, to_cesu8};

pub fn string_to_cesu8(s: &str) -> Vec<u8> {
    to_cesu8(s).to_vec()
}

pub fn cesu8_to_string(v: &[u8]) -> HdbResult<String> {
    let cow = from_cesu8(v)?;
    Ok(String::from(&*cow))
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

// ===== Stolen from crate cesu8, because the original used unstable features
// Copyright 2012-2014 The Rust Project Developers and Eric Kidd.  See the
// COPYRIGHT-RUST.txt file at the top-level directory of this distribution.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.

// ! A simple library implementing the [CESU-8 compatibility encoding
// ! scheme](http://www.unicode.org/reports/tr26/tr26-2.html).  This is a
// ! non-standard variant of UTF-8 that is used internally by some systems
// ! that need to represent UTF-16 data as 8-bit characters.  Yes, this is
// ! ugly.
// !
// ! Use of this encoding is discouraged by the Unicode Consortium.  It's OK
// ! for working with existing internal APIs, but it should not be used for
// ! transmitting or storing data.
// !
// ! ```
// ! use std::borrow::Cow;
// ! use cesu8::{from_cesu8, to_cesu8};
// !
// ! // 16-bit Unicode characters are the same in UTF-8 and CESU-8.
// ! assert_eq!(Cow::Borrowed("aé日".as_bytes()),
// !            to_cesu8("aé日"));
// ! assert_eq!(Cow::Borrowed("aé日"),
// !            from_cesu8("aé日".as_bytes()).unwrap());
// !
// ! // This string is CESU-8 data containing a 6-byte surrogate pair,
// ! // which decodes to a 4-byte UTF-8 string.
// ! let data = &[0xED, 0xA0, 0x81, 0xED, 0xB0, 0x81];
// ! assert_eq!(Cow::Borrowed("\u{10401}"),
// !            from_cesu8(data).unwrap());
// ! ```
// !
// ! ### A note about security
// !
// ! As a general rule, this library is intended to fail on malformed or
// ! unexpected input.  CESU-8 is supposed to be an internal-only format,
// ! and if we're seeing malformed data, we assume that it's either a bug in
// ! somebody's code, or an attacker is trying to improperly encode data to
// ! evade security checks.
// !
// ! If you have a use case for lossy conversion to UTF-8, or conversion
// ! from mixed UTF-8/CESU-8 data, please feel free to submit a pull request
// ! for `from_cesu8_lossy_permissive` with appropriate behavior.
// !
// ! ### Java and U+0000, and other variants
// !
// ! Java uses the CESU-8 encoding as described above, but with one
// ! difference: The null character U+0000 is represented as an overlong
// ! UTF-8 sequence.  This is not currently supported by this library, but
// ! pull requests to add `from_java_cesu8` and `to_java_cesu8` are welcome.
// !
// ! ### Surrogate pairs and UTF-8
// !
// ! The UTF-16 encoding uses "surrogate pairs" to represent Unicode code
// ! points in the range from U+10000 to U+10FFFF.  These are 16-bit numbers
// ! in the range 0xD800 to 0xDFFF.
// !
// ! * 0xD800 to 0xDBFF: First half of surrogate pair.  When encoded as
// !   CESU-8, these become **1110**1101 **10**100000 **10**000000 to
// !   **1110**1101 **10**101111 **10**111111.
// !
// ! * 0xDC00 to 0xDFFF: Second half of surrogate pair.  These become
// !   **1110**1101 **10**110000 **10**000000 to
// !   **1110**1101 **10**111111 **10**111111.
// !
// ! Wikipedia [explains](http://en.wikipedia.org/wiki/UTF-16) the
// ! code point to UTF-16 conversion process:
// !
// ! > Consider the encoding of U+10437 (𐐷):
// ! >
// ! > * Subtract 0x10000 from 0x10437. The result is 0x00437, 0000 0000 0100
// ! >   0011 0111.
// ! > * Split this into the high 10-bit value and the low 10-bit value:
// ! >   0000000001 and 0000110111.
// ! > * Add 0xD800 to the high value to form the high surrogate: 0xD800 +
// ! >   0x0001 = 0xD801.
// ! > * Add 0xDC00 to the low value to form the low surrogate: 0xDC00 +
// ! >   0x0037 = 0xDC37.

use std::slice;
use std::str;

#[cfg_attr(rustfmt, rustfmt_skip)]
static UTF8_CHAR_WIDTH: [u8; 256] = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 /* 0x1F */,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 /* 0x3F */,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 /* 0x5F */,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 /* 0x7F */,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 /* 0x9F */,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 /* 0xBF */,
    0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 /* 0xDF */,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 /* 0xEF */,
    4, 4, 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 /* 0xFF */
];

/// Given a first byte, determine how many bytes are in this UTF-8 character
#[inline]
pub fn utf8_char_width(b: u8) -> usize {
    UTF8_CHAR_WIDTH[b as usize] as usize
}

/// Mask of the value bits of a continuation byte.
const CONT_MASK: u8 = 0b0011_1111u8;
/// Value of the tag bits (tag mask is !`CONT_MASK`) of a continuation byte.
const TAG_CONT_U8: u8 = 0b1000_0000u8;

// Our internal decoder, based on Rust's is_utf8 implementation.
pub fn decode_from_iter(decoded: &mut Vec<u8>, iter: &mut slice::Iter<u8>) -> (bool, u64) {
    let mut byte_count = 0;

    macro_rules! err {
        () => {
            return (false, byte_count);
        };
    }
    macro_rules! next {
        () => {
            match iter.next() {
                Some(a) => *a,
                // We needed data, but there was none: error!
                None => err!(),
            }
        };
    }
    macro_rules! next_cont {
        () => {{
            let byte = next!();
            if (byte) & !CONT_MASK == TAG_CONT_U8 {
                byte
            } else {
                err!()
            }
        }};
    }

    loop {
        let first = match iter.next() {
            Some(&b) => b,
            // We're at the end of the iterator and a codepoint boundary at
            // the same time, so this string is valid.
            None => return (true, byte_count),
        };
        if first < 128 {
            // Pass ASCII through directly.
            decoded.push(first);
            byte_count += 1;
        } else {
            let w = utf8_char_width(first);
            let second = next_cont!();
            match w {
                // Two-byte sequences can be used directly.
                2 => {
                    decoded.extend([first, second].iter().cloned());
                    byte_count += 2;
                }
                3 => {
                    let third = next_cont!();
                    match (first, second) {
                        // These are valid UTF-8, so pass them through.
                        (0xE0, 0xA0...0xBF)
                        | (0xE1...0xEC, 0x80...0xBF)
                        | (0xED, 0x80...0x9F)
                        | (0xEE...0xEF, 0x80...0xBF) => {
                            decoded.extend([first, second, third].iter().cloned());
                            byte_count += 3;
                        }
                        // First half a surrogate pair, so decode.
                        (0xED, 0xA0...0xAF) => {
                            if next!() != 0xED {
                                err!()
                            }
                            let fifth = next_cont!();
                            if fifth < 0xB0 || 0xBF < fifth {
                                err!()
                            }
                            let sixth = next_cont!();
                            let s = dec_surrogates(second, third, fifth, sixth);
                            decoded.extend(s.iter().cloned());
                            byte_count += 6;
                        }
                        _ => err!(),
                    }
                }
                _ => err!(),
            }
        }
    }
}

/// Convert the two trailing bytes from a CESU-8 surrogate to a regular
/// surrogate value.
fn dec_surrogate(second: u8, third: u8) -> u32 {
    0xD000u32 | u32::from(second & CONT_MASK) << 6 | u32::from(third & CONT_MASK)
}

/// Convert the bytes from a CESU-8 surrogate pair into a valid UTF-8
/// sequence.  Assumes input is valid.
fn dec_surrogates(second: u8, third: u8, fifth: u8, sixth: u8) -> [u8; 4] {
    // Convert to a 32-bit code point.
    let s1 = dec_surrogate(second, third);
    let s2 = dec_surrogate(fifth, sixth);
    let c = 0x1_0000 + (((s1 - 0xD800) << 10) | (s2 - 0xDC00));
    // println!("{:0>8b} {:0>8b} {:0>8b} -> {:0>16b}", 0xEDu8, second, third, s1);
    // println!("{:0>8b} {:0>8b} {:0>8b} -> {:0>16b}", 0xEDu8, fifth, sixth, s2);
    // println!("-> {:0>32b}", c);
    assert!(0x01_0000 <= c && c <= 0x10_FFFF);

    // Convert to UTF-8.
    // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
    [
        0b1111_0000u8 | ((c & 0b1_1100_0000_0000_0000_0000) >> 18) as u8,
        TAG_CONT_U8 | ((c & 0b0_0011_1111_0000_0000_0000) >> 12) as u8,
        TAG_CONT_U8 | ((c & 0b0_0000_0000_1111_1100_0000) >> 6) as u8,
        TAG_CONT_U8 | (c & 0b0_0000_0000_0000_0011_1111) as u8,
    ]
}

/// String has something similar, we need it for byte
pub fn is_utf8_char_start(b: u8) -> bool {
    match b {
        0x00...0x7F | 0xC0...0xDF | 0xE0...0xEF | 0xF0...0xF7 => true,
        _ => false,
    }
}
