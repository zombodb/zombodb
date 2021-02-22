use crate::json::json_string::JsonString;
use pgx::{Date, JsonB, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone};

#[derive(Debug)]
#[allow(non_camel_case_types)]
enum JsonBuilderValue {
    bool(bool),
    i16(i16),
    i32(i32),
    i64(i64),
    u32(u32),
    u64(u64),
    f32(f32),
    f64(f64),
    time(Time),
    time_with_time_zone(TimeWithTimeZone),
    timestamp(Timestamp),
    timestamp_with_time_zone(TimestampWithTimeZone),
    date(Date),
    string(String),
    json_string(pgx::JsonString),
    jsonb(JsonB),
    json_value(serde_json::Value),

    bool_array(Vec<Option<bool>>),
    i16_array(Vec<Option<i16>>),
    i32_array(Vec<Option<i32>>),
    i64_array(Vec<Option<i64>>),
    u32_array(Vec<Option<u32>>),
    f32_array(Vec<Option<f32>>),
    f64_array(Vec<Option<f64>>),
    time_array(Vec<Option<Time>>),
    time_with_time_zone_array(Vec<Option<TimeWithTimeZone>>),
    timestamp_array(Vec<Option<Timestamp>>),
    timestamp_with_time_zone_array(Vec<Option<TimestampWithTimeZone>>),
    date_array(Vec<Option<Date>>),
    string_array(Vec<Option<String>>),
    json_string_array(Vec<Option<pgx::JsonString>>),
    jsonb_array(Vec<Option<JsonB>>),
}

#[derive(Debug)]
pub struct JsonBuilder<'a> {
    values: Vec<(&'a str, JsonBuilderValue)>,
}

impl<'a> JsonBuilder<'a> {
    pub fn new(num_fields: usize) -> Self {
        JsonBuilder {
            values: Vec::with_capacity(num_fields),
        }
    }

    #[inline]
    pub fn add_bool(&mut self, attname: &'a str, value: bool) {
        self.values.push((attname, JsonBuilderValue::bool(value)));
    }

    #[inline]
    pub fn add_i16(&mut self, attname: &'a str, value: i16) {
        self.values.push((attname, JsonBuilderValue::i16(value)));
    }

    #[inline]
    pub fn add_i32(&mut self, attname: &'a str, value: i32) {
        self.values.push((attname, JsonBuilderValue::i32(value)));
    }

    #[inline]
    pub fn add_i64(&mut self, attname: &'a str, value: i64) {
        self.values.push((attname, JsonBuilderValue::i64(value)));
    }

    #[inline]
    pub fn add_u32(&mut self, attname: &'a str, value: u32) {
        self.values.push((attname, JsonBuilderValue::u32(value)));
    }

    #[inline]
    pub fn add_u64(&mut self, attname: &'a str, value: u64) {
        self.values.push((attname, JsonBuilderValue::u64(value)));
    }

    #[inline]
    pub fn add_f32(&mut self, attname: &'a str, value: f32) {
        self.values.push((attname, JsonBuilderValue::f32(value)));
    }

    #[inline]
    pub fn add_f64(&mut self, attname: &'a str, value: f64) {
        self.values.push((attname, JsonBuilderValue::f64(value)));
    }

    #[inline]
    pub fn add_time(&mut self, attname: &'a str, value: Time) {
        self.values.push((attname, JsonBuilderValue::time(value)));
    }

    #[inline]
    pub fn add_time_with_time_zone(&mut self, attname: &'a str, value: TimeWithTimeZone) {
        self.values
            .push((attname, JsonBuilderValue::time_with_time_zone(value)));
    }

    #[inline]
    pub fn add_timestamp(&mut self, attname: &'a str, value: Timestamp) {
        self.values
            .push((attname, JsonBuilderValue::timestamp(value)));
    }

    #[inline]
    pub fn add_timestamp_with_time_zone(&mut self, attname: &'a str, value: TimestampWithTimeZone) {
        self.values
            .push((attname, JsonBuilderValue::timestamp_with_time_zone(value)));
    }

    #[inline]
    pub fn add_date(&mut self, attname: &'a str, value: Date) {
        self.values.push((attname, JsonBuilderValue::date(value)));
    }

    #[inline]
    pub fn add_string(&mut self, attname: &'a str, value: String) {
        self.values.push((attname, JsonBuilderValue::string(value)));
    }

    #[inline]
    pub fn add_json_string(&mut self, attname: &'a str, value: pgx::JsonString) {
        self.values
            .push((attname, JsonBuilderValue::json_string(value)));
    }

    #[inline]
    pub fn add_jsonb(&mut self, attname: &'a str, value: JsonB) {
        self.values.push((attname, JsonBuilderValue::jsonb(value)));
    }

    #[inline]
    pub fn add_json_value(&mut self, attname: &'a str, value: serde_json::Value) {
        self.values
            .push((attname, JsonBuilderValue::json_value(value)));
    }

    #[inline]
    pub fn add_bool_array(&mut self, attname: &'a str, value: Vec<Option<bool>>) {
        self.values
            .push((attname, JsonBuilderValue::bool_array(value)));
    }

    #[inline]
    pub fn add_i16_array(&mut self, attname: &'a str, value: Vec<Option<i16>>) {
        self.values
            .push((attname, JsonBuilderValue::i16_array(value)));
    }

    #[inline]
    pub fn add_i32_array(&mut self, attname: &'a str, value: Vec<Option<i32>>) {
        self.values
            .push((attname, JsonBuilderValue::i32_array(value)));
    }

    #[inline]
    pub fn add_i64_array(&mut self, attname: &'a str, value: Vec<Option<i64>>) {
        self.values
            .push((attname, JsonBuilderValue::i64_array(value)));
    }

    #[inline]
    pub fn add_u32_array(&mut self, attname: &'a str, value: Vec<Option<u32>>) {
        self.values
            .push((attname, JsonBuilderValue::u32_array(value)));
    }

    #[inline]
    pub fn add_f32_array(&mut self, attname: &'a str, value: Vec<Option<f32>>) {
        self.values
            .push((attname, JsonBuilderValue::f32_array(value)));
    }

    #[inline]
    pub fn add_f64_array(&mut self, attname: &'a str, value: Vec<Option<f64>>) {
        self.values
            .push((attname, JsonBuilderValue::f64_array(value)));
    }

    #[inline]
    pub fn add_time_array(&mut self, attname: &'a str, value: Vec<Option<Time>>) {
        self.values
            .push((attname, JsonBuilderValue::time_array(value)));
    }

    #[inline]
    pub fn add_time_with_time_zone_array(
        &mut self,
        attname: &'a str,
        value: Vec<Option<TimeWithTimeZone>>,
    ) {
        self.values
            .push((attname, JsonBuilderValue::time_with_time_zone_array(value)));
    }

    #[inline]
    pub fn add_timestamp_array(&mut self, attname: &'a str, value: Vec<Option<Timestamp>>) {
        self.values
            .push((attname, JsonBuilderValue::timestamp_array(value)));
    }

    #[inline]
    pub fn add_timestamp_with_time_zone_array(
        &mut self,
        attname: &'a str,
        value: Vec<Option<TimestampWithTimeZone>>,
    ) {
        self.values.push((
            attname,
            JsonBuilderValue::timestamp_with_time_zone_array(value),
        ));
    }

    #[inline]
    pub fn add_date_array(&mut self, attname: &'a str, value: Vec<Option<Date>>) {
        self.values
            .push((attname, JsonBuilderValue::date_array(value)));
    }

    #[inline]
    pub fn add_string_array(&mut self, attname: &'a str, value: Vec<Option<String>>) {
        self.values
            .push((attname, JsonBuilderValue::string_array(value)));
    }

    #[inline]
    pub fn add_json_string_array(&mut self, attname: &'a str, value: Vec<Option<pgx::JsonString>>) {
        self.values
            .push((attname, JsonBuilderValue::json_string_array(value)));
    }

    #[inline]
    pub fn add_jsonb_array(&mut self, attname: &'a str, value: Vec<Option<JsonB>>) {
        self.values
            .push((attname, JsonBuilderValue::jsonb_array(value)));
    }

    pub fn build(self) -> String {
        let mut json = String::with_capacity(16 * 1024);

        json.push('{');
        for (idx, (key, value)) in self.values.into_iter().enumerate() {
            if idx > 0 {
                json.push(',');
            }

            json.push('"');
            json.push_str(key);
            json.push('"');
            json.push(':');

            match value {
                JsonBuilderValue::bool(v) => v.push_json(&mut json),
                JsonBuilderValue::i16(v) => v.push_json(&mut json),
                JsonBuilderValue::i32(v) => v.push_json(&mut json),
                JsonBuilderValue::i64(v) => v.push_json(&mut json),
                JsonBuilderValue::u32(v) => v.push_json(&mut json),
                JsonBuilderValue::u64(v) => v.push_json(&mut json),
                JsonBuilderValue::f32(v) => v.push_json(&mut json),
                JsonBuilderValue::f64(v) => v.push_json(&mut json),
                JsonBuilderValue::time(v) => v.push_json(&mut json),
                JsonBuilderValue::time_with_time_zone(v) => v.push_json(&mut json),
                JsonBuilderValue::timestamp(v) => v.push_json(&mut json),
                JsonBuilderValue::timestamp_with_time_zone(v) => v.push_json(&mut json),
                JsonBuilderValue::date(v) => v.push_json(&mut json),
                JsonBuilderValue::string(v) => v.push_json(&mut json),
                JsonBuilderValue::json_string(v) => v.push_json(&mut json),
                JsonBuilderValue::jsonb(v) => v.push_json(&mut json),
                JsonBuilderValue::json_value(v) => v.push_json(&mut json),
                JsonBuilderValue::bool_array(v) => v.push_json(&mut json),
                JsonBuilderValue::i16_array(v) => v.push_json(&mut json),
                JsonBuilderValue::i32_array(v) => v.push_json(&mut json),
                JsonBuilderValue::i64_array(v) => v.push_json(&mut json),
                JsonBuilderValue::u32_array(v) => v.push_json(&mut json),
                JsonBuilderValue::f32_array(v) => v.push_json(&mut json),
                JsonBuilderValue::f64_array(v) => v.push_json(&mut json),
                JsonBuilderValue::time_array(v) => v.push_json(&mut json),
                JsonBuilderValue::time_with_time_zone_array(v) => v.push_json(&mut json),
                JsonBuilderValue::timestamp_array(v) => v.push_json(&mut json),
                JsonBuilderValue::timestamp_with_time_zone_array(v) => v.push_json(&mut json),
                JsonBuilderValue::date_array(v) => v.push_json(&mut json),
                JsonBuilderValue::string_array(v) => v.push_json(&mut json),
                JsonBuilderValue::json_string_array(v) => v.push_json(&mut json),
                JsonBuilderValue::jsonb_array(v) => v.push_json(&mut json),
            }
        }
        json.push('}');

        json
    }
}
