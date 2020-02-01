use crate::json::json_string::JsonString;
use crate::json::utils::escape_json;

#[derive(Debug)]
pub struct JsonBuilder {
    attnames: Vec<String>,
    values: Vec<Box<dyn JsonString>>,
}

impl JsonBuilder {
    pub fn new(attnames: Vec<String>, values: Vec<Box<dyn JsonString>>) -> Self {
        JsonBuilder { attnames, values }
    }

    pub fn build(self) -> String {
        let mut json = String::with_capacity(8192);

        json.push('{');
        for (i, value) in self.values.into_iter().enumerate() {
            if i > 0 {
                json.push(',');
            }
            // keyname
            json.push('"');
            escape_json(&self.attnames.get(i).unwrap(), &mut json);
            json.push('"');

            json.push(':');

            // value
            value.push_json(&mut json);
        }
        json.push('}');

        json
    }
}
