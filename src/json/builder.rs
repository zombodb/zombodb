use crate::json::json_string::JsonString;
use crate::json::utils::escape_json;

#[derive(Debug)]
pub struct JsonBuilder {
    values: Vec<(String, Box<dyn JsonString>)>,
}

impl JsonBuilder {
    pub fn new(num_fields: usize) -> Self {
        JsonBuilder {
            values: Vec::with_capacity(num_fields),
        }
    }

    #[inline]
    pub fn add_value<T>(&mut self, attname: &str, value: T)
    where
        T: JsonString + Sized + 'static,
    {
        self.values.push((attname.to_owned(), Box::new(value)));
    }

    pub(crate) fn build(self) -> String {
        let mut json = String::with_capacity(8192);

        json.push('{');

        for (i, (attname, value)) in self.values.into_iter().enumerate() {
            if i > 0 {
                json.push(',');
            }

            json.push('"');
            escape_json(&attname, &mut json);
            json.push('"');
            json.push(':');
            value.push_json(&mut json);
        }

        json.push('}');

        json
    }
}
