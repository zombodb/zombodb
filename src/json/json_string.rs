use crate::json::utils::escape_json;
use pgx::{FromDatum, Json, JsonB};

pub trait JsonString: Send + Sync {
    fn push_json(&self, string: &mut String);
}

impl JsonString for i16 {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&*self.to_string());
    }
}

impl JsonString for i32 {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&*self.to_string());
    }
}

impl JsonString for i64 {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&*self.to_string());
    }
}

impl JsonString for f32 {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&*self.to_string());
    }
}

impl JsonString for f64 {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&*self.to_string());
    }
}

impl JsonString for u32 {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&*self.to_string());
    }
}

impl JsonString for u64 {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&*self.to_string());
    }
}

impl JsonString for bool {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&*self.to_string());
    }
}

impl JsonString for () {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str("null");
    }
}

impl JsonString for &str {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push('"');
        escape_json(*self, string);
        string.push('"');
    }
}

impl JsonString for String {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push('"');
        escape_json(&*self, string);
        string.push('"');
    }
}

impl JsonString for pgx::JsonString {
    #[inline]
    fn push_json(&self, string: &mut String) {
        // replace \r\n's to ensure it's all on one line.  It's otherwise supposed to be valid JSON
        // so we shouldn't be mistakenly replacing any \r\n's in actual values -- those should already
        // be properly escaped
        string.push_str(&*self.0.replace('\r', " ").replace('\n', " "));
    }
}

impl<T> JsonString for Vec<Option<T>>
where
    T: FromDatum<T> + JsonString,
{
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push('[');
        for (i, datum) in self.iter().enumerate() {
            if i > 0 {
                string.push(',');
            }
            match datum {
                Some(datum) => datum.push_json(string),
                None => string.push_str("null"),
            }
        }
        string.push(']');
    }
}

impl JsonString for Json {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&serde_json::to_string(&(*self).0).unwrap());
    }
}

impl JsonString for JsonB {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(&serde_json::to_string(&(*self).0).unwrap());
    }
}

impl JsonString for serde_json::Value {
    #[inline]
    fn push_json(&self, string: &mut String) {
        string.push_str(serde_json::to_string(self).unwrap().as_str())
    }
}

impl std::fmt::Debug for Box<dyn JsonString> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let mut s = String::new();
        self.push_json(&mut s);
        f.write_str(s.as_str())
    }
}
