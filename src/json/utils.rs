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

    #[test]
    fn test_json_escape_low() {
        let mut input = [0u8; 32];
        let mut into = String::new();
        for i in 0..32 {
            input[i] = i as u8;
        }
        let input = &unsafe { String::from_utf8_unchecked(input.to_vec()) };
        escape_json(input, &mut into);

        assert_eq!(into, "\\u004848\\u004849\\u004850\\u004851\\u004852\\u004853\\u004854\\u004855\\b\\t\\n\\u004898\\f\\r\\u0048101\\u0048102\\u004948\\u004949\\u004950\\u004951\\u004952\\u004953\\u004954\\u004955\\u004956\\u004957\\u004997\\u004998\\u004999\\u0049100\\u0049101\\u0049102");
    }
}
