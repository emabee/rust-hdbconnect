// Last part in a sequence of parts (FETCH, array command EXECUTE)
const LAST_PACKET: u8 = 0b_0000_0001;

// Part in a sequence of parts
const NEXT_PACKET: u8 = 0b_0000_0010;

// First part in a sequence of parts
pub(crate) const FIRST_PACKET: u8 = 0b_0000_0100;

// Empty part, caused by “row not found” error
const ROW_NOT_FOUND: u8 = 0b_0000_1000;

// The resultset that produced this part is closed
const RESULTSET_IS_CLOSED: u8 = 0b_0001_0000;

// bit pattern for some attribute parts
#[derive(Clone)]
pub(crate) struct PartAttributes(u8);
impl PartAttributes {
    pub fn new(bits: u8) -> Self {
        Self(bits)
    }

    pub fn is_last_packet(&self) -> bool {
        (self.0 & LAST_PACKET) != 0
    }
    fn has_next_packet(&self) -> bool {
        (self.0 & NEXT_PACKET) != 0
    }
    fn is_first_packet(&self) -> bool {
        (self.0 & FIRST_PACKET) != 0
    }
    pub fn row_not_found(&self) -> bool {
        (self.0 & ROW_NOT_FOUND) != 0
    }
    pub fn resultset_is_closed(&self) -> bool {
        (self.0 & RESULTSET_IS_CLOSED) != 0
    }
}

impl std::fmt::Debug for PartAttributes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "0")
        } else {
            let mut b = false;
            write!(f, "(")?;
            if self.is_last_packet() {
                b = true;
                write!(f, "IS_LAST_PACKET")?;
            };
            if self.has_next_packet() {
                b = w_and(b, f)?;
                write!(f, "HAS_NEXT_PACKET")?;
            };
            if self.is_first_packet() {
                b = w_and(b, f)?;
                write!(f, "IS_FIRST_PACKET_IN_A_SEQUENCE")?;
            };
            if self.row_not_found() {
                b = w_and(b, f)?;
                write!(f, "ROW_NOT_FOUND")?;
            };
            if self.resultset_is_closed() {
                w_and(b, f)?;
                write!(f, "RESULTSET_CLOSED")?;
            };
            write!(f, ")")
        }
    }
}
impl std::fmt::Binary for PartAttributes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:b}", self.0)
    }
}

// write an ampersand (&) if required
fn w_and(b: bool, f: &mut std::fmt::Formatter) -> Result<bool, std::fmt::Error> {
    if b {
        write!(f, " & ")?;
    }
    Ok(true)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_part_attributes() {
        let no = PartAttributes::new(0);
        assert!(!no.has_next_packet());
        assert!(!no.is_first_packet());
        assert!(!no.is_last_packet());
        assert!(!no.resultset_is_closed());
        assert!(!no.row_not_found());

        let yes = PartAttributes::new(
            NEXT_PACKET & FIRST_PACKET & LAST_PACKET & RESULTSET_IS_CLOSED & ROW_NOT_FOUND,
        );
        assert!(yes.has_next_packet());
        assert!(yes.is_first_packet());
        assert!(yes.is_last_packet());
        assert!(yes.resultset_is_closed());
        assert!(yes.row_not_found());

        let s_no = format!("{no:?}");
        let s_yes = format!("{yes:?}");
        println!("{s_no}");
        println!("{s_yes}");
    }
}
