use crate::json::json_string::JsonString;
use crate::json::utils::escape_json;
use pgx::JsonB;

#[derive(Debug)]
pub struct JsonBuilder {
    bool: Vec<(&'static str, bool)>,
    i16: Vec<(&'static str, i16)>,
    i32: Vec<(&'static str, i32)>,
    i64: Vec<(&'static str, i64)>,
    u32: Vec<(&'static str, u32)>,
    f32: Vec<(&'static str, f32)>,
    f64: Vec<(&'static str, f64)>,
    string: Vec<(&'static str, String)>,
    json_string: Vec<(&'static str, pgx::JsonString)>,
    jsonb: Vec<(&'static str, JsonB)>,

    bool_array: Vec<(&'static str, Vec<Option<bool>>)>,
    i16_array: Vec<(&'static str, Vec<Option<i16>>)>,
    i32_array: Vec<(&'static str, Vec<Option<i32>>)>,
    i64_array: Vec<(&'static str, Vec<Option<i64>>)>,
    u32_array: Vec<(&'static str, Vec<Option<u32>>)>,
    f32_array: Vec<(&'static str, Vec<Option<f32>>)>,
    f64_array: Vec<(&'static str, Vec<Option<f64>>)>,
    string_array: Vec<(&'static str, Vec<Option<String>>)>,
    json_string_array: Vec<(&'static str, Vec<Option<pgx::JsonString>>)>,
    jsonb_array: Vec<(&'static str, Vec<Option<JsonB>>)>,
}

impl JsonBuilder {
    pub fn new(num_fields: usize) -> Self {
        JsonBuilder {
            bool: Vec::with_capacity(num_fields),
            i16: Vec::with_capacity(num_fields),
            i32: Vec::with_capacity(num_fields),
            i64: Vec::with_capacity(num_fields),
            u32: Vec::with_capacity(num_fields),
            f32: Vec::with_capacity(num_fields),
            f64: Vec::with_capacity(num_fields),
            string: Vec::with_capacity(num_fields),
            json_string: Vec::with_capacity(num_fields),
            jsonb: Vec::with_capacity(num_fields),
            bool_array: Vec::with_capacity(num_fields),
            i16_array: Vec::with_capacity(num_fields),
            i32_array: Vec::with_capacity(num_fields),
            i64_array: Vec::with_capacity(num_fields),
            u32_array: Vec::with_capacity(num_fields),
            f32_array: Vec::with_capacity(num_fields),
            f64_array: Vec::with_capacity(num_fields),
            string_array: Vec::with_capacity(num_fields),
            json_string_array: Vec::with_capacity(num_fields),
            jsonb_array: Vec::with_capacity(num_fields),
        }
    }

    #[inline]
    pub fn add_bool(&mut self, attname: &'static str, value: bool) {
        self.bool.push((attname, value));
    }

    #[inline]
    pub fn add_i16(&mut self, attname: &'static str, value: i16) {
        self.i16.push((attname, value));
    }

    #[inline]
    pub fn add_i32(&mut self, attname: &'static str, value: i32) {
        self.i32.push((attname, value));
    }

    #[inline]
    pub fn add_i64(&mut self, attname: &'static str, value: i64) {
        self.i64.push((attname, value));
    }

    #[inline]
    pub fn add_u32(&mut self, attname: &'static str, value: u32) {
        self.u32.push((attname, value));
    }

    #[inline]
    pub fn add_f32(&mut self, attname: &'static str, value: f32) {
        self.f32.push((attname, value));
    }

    #[inline]
    pub fn add_f64(&mut self, attname: &'static str, value: f64) {
        self.f64.push((attname, value));
    }

    #[inline]
    pub fn add_string(&mut self, attname: &'static str, value: String) {
        self.string.push((attname, value));
    }

    #[inline]
    pub fn add_json_string(&mut self, attname: &'static str, value: pgx::JsonString) {
        self.json_string.push((attname, value));
    }

    #[inline]
    pub fn add_jsonb(&mut self, attname: &'static str, value: JsonB) {
        self.jsonb.push((attname, value));
    }

    #[inline]
    pub fn add_bool_array(&mut self, attname: &'static str, value: Vec<Option<bool>>) {
        self.bool_array.push((attname, value));
    }

    #[inline]
    pub fn add_i16_array(&mut self, attname: &'static str, value: Vec<Option<i16>>) {
        self.i16_array.push((attname, value));
    }

    #[inline]
    pub fn add_i32_array(&mut self, attname: &'static str, value: Vec<Option<i32>>) {
        self.i32_array.push((attname, value));
    }

    #[inline]
    pub fn add_i64_array(&mut self, attname: &'static str, value: Vec<Option<i64>>) {
        self.i64_array.push((attname, value));
    }

    #[inline]
    pub fn add_u32_array(&mut self, attname: &'static str, value: Vec<Option<u32>>) {
        self.u32_array.push((attname, value));
    }

    #[inline]
    pub fn add_f32_array(&mut self, attname: &'static str, value: Vec<Option<f32>>) {
        self.f32_array.push((attname, value));
    }

    #[inline]
    pub fn add_f64_array(&mut self, attname: &'static str, value: Vec<Option<f64>>) {
        self.f64_array.push((attname, value));
    }

    #[inline]
    pub fn add_string_array(&mut self, attname: &'static str, value: Vec<Option<String>>) {
        self.string_array.push((attname, value));
    }

    #[inline]
    pub fn add_json_string_array(
        &mut self,
        attname: &'static str,
        value: Vec<Option<pgx::JsonString>>,
    ) {
        self.json_string_array.push((attname, value));
    }

    #[inline]
    pub fn add_jsonb_array(&mut self, attname: &'static str, value: Vec<Option<JsonB>>) {
        self.jsonb_array.push((attname, value));
    }

    pub fn build(self) -> String {
        let mut cnt;
        let mut json = String::with_capacity(8192);

        json.push('{');

        cnt = self.encode(&mut json, &self.bool, 0);
        cnt = self.encode(&mut json, &self.i16, cnt);
        cnt = self.encode(&mut json, &self.i32, cnt);
        cnt = self.encode(&mut json, &self.i64, cnt);
        cnt = self.encode(&mut json, &self.u32, cnt);
        cnt = self.encode(&mut json, &self.f32, cnt);
        cnt = self.encode(&mut json, &self.f64, cnt);
        cnt = self.encode(&mut json, &self.string, cnt);
        cnt = self.encode(&mut json, &self.json_string, cnt);
        cnt = self.encode(&mut json, &self.jsonb, cnt);

        cnt = self.encode(&mut json, &self.bool_array, cnt);
        cnt = self.encode(&mut json, &self.i16_array, cnt);
        cnt = self.encode(&mut json, &self.i32_array, cnt);
        cnt = self.encode(&mut json, &self.i64_array, cnt);
        cnt = self.encode(&mut json, &self.u32_array, cnt);
        cnt = self.encode(&mut json, &self.f32_array, cnt);
        cnt = self.encode(&mut json, &self.f64_array, cnt);
        cnt = self.encode(&mut json, &self.string_array, cnt);
        cnt = self.encode(&mut json, &self.json_string_array, cnt);
        self.encode(&mut json, &self.jsonb_array, cnt);

        json.push('}');

        json
    }

    fn encode<T>(&self, json: &mut String, values: &Vec<(&'static str, T)>, mut cnt: usize) -> usize
    where
        T: JsonString,
    {
        for (attname, value) in values {
            if cnt > 0 {
                json.push(',');
            }

            json.push('"');
            escape_json(&attname, json);
            json.push('"');
            json.push(':');
            value.push_json(json);
            cnt += 1;
        }

        cnt
    }
}
