#[inline]
pub fn escape_json(input: &str, target: &mut String) {
    for c in input.chars() {
        match c as u8 {
            b'\n' => {
                target.push('\\');
                target.push('n');
            }
            b'\r' => {
                target.push('\\');
                target.push('r');
            }
            b'\t' => {
                target.push('\\');
                target.push('t');
            }
            b'"' => {
                target.push('\\');
                target.push('"');
            }
            b'\\' => {
                target.push('\\');
                target.push('\\');
            }
            8 => {
                target.push('\\');
                target.push('b');
            }
            12 => {
                target.push('\\');
                target.push('f');
            }
            _ => target.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::json::utils::escape_json;

    #[test]
    fn test_json_escape_standard() {
        let input = "\r\n\t\\";
        let mut into = String::new();

        escape_json(input, &mut into);

        assert_eq!(into, "\\r\\n\\t\\\\");
    }

    #[test]
    fn test_json_escape_special() {
        let input = [8u8, 12u8];
        let mut into = String::new();

        let input = &unsafe { String::from_utf8_unchecked(input.to_vec()) };
        escape_json(input, &mut into);

        assert_eq!(into, "\\b\\f");
    }
}
