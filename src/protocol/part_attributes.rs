use std::fmt;

// bit pattern for some attribute parts
#[derive(Clone)]
pub(crate) struct PartAttributes(u8);
impl PartAttributes {
    pub fn new(bits: u8) -> Self {
        Self(bits)
    }

    // Last part in a sequence of parts (FETCH, array command EXECUTE):
    pub fn is_last_packet(&self) -> bool {
        (self.0 & 0b_0000_0001) != 0
    }

    // Part in a sequence of parts:
    fn has_next_packet(&self) -> bool {
        (self.0 & 0b_0000_0010) != 0
    }

    // First part in a sequence of parts:
    fn is_first_packet(&self) -> bool {
        (self.0 & 0b_0000_0100) != 0
    }

    // Empty part, caused by “row not found” error:
    pub fn row_not_found(&self) -> bool {
        (self.0 & 0b_0000_1000) != 0
    }

    // The resultset that produced this part is closed:
    pub fn resultset_is_closed(&self) -> bool {
        (self.0 & 0b_0001_0000) != 0
    }
}

impl fmt::Debug for PartAttributes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
impl fmt::Binary for PartAttributes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:b}", self.0)
    }
}

// write an ampersand (&) if required
fn w_and(b: bool, f: &mut fmt::Formatter) -> Result<bool, fmt::Error> {
    if b {
        write!(f, " & ")?;
    }
    Ok(true)
}

#[cfg(test)]
mod test {
    use super::PartAttributes;

    #[test]
    fn test_part_attributes() {
        let no = PartAttributes::new(0);
        assert!(!no.has_next_packet());
        assert!(!no.is_first_packet());
        assert!(!no.is_last_packet());
        assert!(!no.resultset_is_closed());
        assert!(!no.row_not_found());

        let yes = PartAttributes::new(0b_0001_1111);
        assert!(yes.has_next_packet());
        assert!(yes.is_first_packet());
        assert!(yes.is_last_packet());
        assert!(yes.resultset_is_closed());
        assert!(yes.row_not_found());

        let s_no = format!("{:?}", no);
        let s_yes = format!("{:?}", yes);
        println!("{}", s_no);
        println!("{}", s_yes);
    }
}
