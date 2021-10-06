#[inline]
pub fn escape_json(input: &str, target: &mut String) {
    for c in input.chars() {
        match c {
            '\n' => {
                target.push('\\');
                target.push('n');
            }
            '\r' => {
                target.push('\\');
                target.push('r');
            }
            '\t' => {
                target.push('\\');
                target.push('t');
            }
            '"' => {
                target.push('\\');
                target.push('"');
            }
            '\\' => {
                target.push('\\');
                target.push('\\');
            }
            '\x08' => {
                target.push('\\');
                target.push('b');
            }
            '\x0c' => {
                target.push('\\');
                target.push('f');
            }
            other if c < ' ' => {
                target.push_str(&format!("\\u{:04x}", other as u8));
            }
            _ => target.push(c),
        }
    }
}

#[cfg(test)]
#[pgx_macros::pg_schema]
mod tests {
    use crate::json::utils::escape_json;

    #[test]
    fn test_json_escape_standard() {
        let input = "\r\n\t\\";
        let mut into = String::new();

        escape_json(input, &mut into);

        assert_eq!(into, r#"\r\n\t\\"#);
    }

    #[test]
    fn test_json_escape_special() {
        let input = [8u8, 12u8];
        let mut into = String::new();

        let input = std::str::from_utf8(&input[..]).expect("invalid utf-8");
        escape_json(input, &mut into);

        assert_eq!(into, r#"\b\f"#);
    }

    #[test]
    fn test_json_escape_control() {
        let input = &[14u8];
        let mut into = String::new();

        let input = std::str::from_utf8(&input[..]).expect("invalid utf-8");
        escape_json(input, &mut into);

        assert_eq!(into, r#"\u000e"#);
    }

    #[test]
    fn test_json_escape_unicode() {
        let input = "레";

        let mut into = String::new();
        escape_json(input, &mut into);

        assert_eq!(into, input);
    }
}
