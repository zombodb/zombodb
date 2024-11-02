use crate::json::json_string::JsonString;
use crate::mapping::JsonStringWrapper;
use crate::misc::timestamp_support::{
    ZDBDate, ZDBTime, ZDBTimeWithTimeZone, ZDBTimestamp, ZDBTimestampWithTimeZone,
};
use pgrx::prelude::*;
use pgrx::JsonB;

#[derive(Debug)]
#[allow(non_camel_case_types)]
enum JsonBuilderValue {
    bool(bool),
    i16(i16),
    i32(i32),
    i64(i64),
    u32(u32),
    oid(pg_sys::Oid),
    u64(u64),
    f32(f32),
    f64(f64),
    time(ZDBTime),
    time_with_time_zone(ZDBTimeWithTimeZone),
    timestamp(ZDBTimestamp),
    timestamp_with_time_zone(ZDBTimestampWithTimeZone),
    date(ZDBDate),
    string(String),
    json_string(JsonStringWrapper),
    jsonb(JsonB),
    json_value(serde_json::Value),

    bool_array(Vec<Option<bool>>),
    i16_array(Vec<Option<i16>>),
    i32_array(Vec<Option<i32>>),
    i64_array(Vec<Option<i64>>),
    u32_array(Vec<Option<u32>>),
    oid_array(Vec<Option<pg_sys::Oid>>),
    f32_array(Vec<Option<f32>>),
    f64_array(Vec<Option<f64>>),
    time_array(Vec<Option<ZDBTime>>),
    time_with_time_zone_array(Vec<Option<ZDBTimeWithTimeZone>>),
    timestamp_array(Vec<Option<ZDBTimestamp>>),
    timestamp_with_time_zone_array(Vec<Option<ZDBTimestampWithTimeZone>>),
    date_array(Vec<Option<ZDBDate>>),
    string_array(Vec<Option<String>>),
    json_string_array(Vec<Option<JsonStringWrapper>>),
    jsonb_array(Vec<Option<JsonB>>),
}

#[derive(Debug)]
pub struct JsonBuilder {
    values: Vec<(String, JsonBuilderValue)>,
}

impl JsonBuilder {
    pub fn new(num_fields: usize) -> Self {
        JsonBuilder {
            values: Vec::with_capacity(num_fields + 5),
        }
    }

    #[inline]
    pub fn add_bool(&mut self, attname: String, value: bool) {
        self.values.push((attname, JsonBuilderValue::bool(value)));
    }

    #[inline]
    pub fn add_i16(&mut self, attname: String, value: i16) {
        self.values.push((attname, JsonBuilderValue::i16(value)));
    }

    #[inline]
    pub fn add_i32(&mut self, attname: String, value: i32) {
        self.values.push((attname, JsonBuilderValue::i32(value)));
    }

    #[inline]
    pub fn add_i64(&mut self, attname: String, value: i64) {
        self.values.push((attname, JsonBuilderValue::i64(value)));
    }

    #[inline]
    pub fn add_u32(&mut self, attname: String, value: u32) {
        self.values.push((attname, JsonBuilderValue::u32(value)));
    }

    #[inline]
    pub fn add_oid(&mut self, attname: String, value: pg_sys::Oid) {
        self.values
            .push((attname, JsonBuilderValue::u32(value.as_u32())));
    }

    #[inline]
    pub fn add_u64(&mut self, attname: String, value: u64) {
        self.values.push((attname, JsonBuilderValue::u64(value)));
    }

    #[inline]
    pub fn add_f32(&mut self, attname: String, value: f32) {
        self.values.push((attname, JsonBuilderValue::f32(value)));
    }

    #[inline]
    pub fn add_f64(&mut self, attname: String, value: f64) {
        self.values.push((attname, JsonBuilderValue::f64(value)));
    }

    #[inline]
    pub fn add_time(&mut self, attname: String, value: Time) {
        self.values
            .push((attname, JsonBuilderValue::time(value.into())));
    }

    #[inline]
    pub fn add_time_with_time_zone(&mut self, attname: String, value: TimeWithTimeZone) {
        self.values
            .push((attname, JsonBuilderValue::time_with_time_zone(value.into())));
    }

    #[inline]
    pub fn add_timestamp(&mut self, attname: String, value: Timestamp) {
        self.values
            .push((attname, JsonBuilderValue::timestamp(value.into())));
    }

    #[inline]
    pub fn add_timestamp_with_time_zone(&mut self, attname: String, value: TimestampWithTimeZone) {
        self.values.push((
            attname,
            JsonBuilderValue::timestamp_with_time_zone(value.into()),
        ));
    }

    #[inline]
    pub fn add_date(&mut self, attname: String, value: Date) {
        self.values
            .push((attname, JsonBuilderValue::date(value.into())));
    }

    #[inline]
    pub fn add_string(&mut self, attname: String, value: String) {
        self.values.push((attname, JsonBuilderValue::string(value)));
    }

    #[inline]
    pub fn add_json_string(&mut self, attname: String, value: JsonStringWrapper) {
        self.values
            .push((attname, JsonBuilderValue::json_string(value)));
    }

    #[inline]
    pub fn add_jsonb(&mut self, attname: String, value: JsonB) {
        self.values.push((attname, JsonBuilderValue::jsonb(value)));
    }

    #[inline]
    pub fn add_json_value(&mut self, attname: String, value: serde_json::Value) {
        self.values
            .push((attname, JsonBuilderValue::json_value(value)));
    }

    #[inline]
    pub fn add_bool_array(&mut self, attname: String, value: Vec<Option<bool>>) {
        self.values
            .push((attname, JsonBuilderValue::bool_array(value)));
    }

    #[inline]
    pub fn add_i16_array(&mut self, attname: String, value: Vec<Option<i16>>) {
        self.values
            .push((attname, JsonBuilderValue::i16_array(value)));
    }

    #[inline]
    pub fn add_i32_array(&mut self, attname: String, value: Vec<Option<i32>>) {
        self.values
            .push((attname, JsonBuilderValue::i32_array(value)));
    }

    #[inline]
    pub fn add_i64_array(&mut self, attname: String, value: Vec<Option<i64>>) {
        self.values
            .push((attname, JsonBuilderValue::i64_array(value)));
    }

    #[inline]
    pub fn add_u32_array(&mut self, attname: String, value: Vec<Option<u32>>) {
        self.values
            .push((attname, JsonBuilderValue::u32_array(value)));
    }

    #[inline]
    pub fn add_oid_array(&mut self, attname: String, value: Vec<Option<pg_sys::Oid>>) {
        self.values
            .push((attname, JsonBuilderValue::oid_array(value)));
    }

    #[inline]
    pub fn add_f32_array(&mut self, attname: String, value: Vec<Option<f32>>) {
        self.values
            .push((attname, JsonBuilderValue::f32_array(value)));
    }

    #[inline]
    pub fn add_f64_array(&mut self, attname: String, value: Vec<Option<f64>>) {
        self.values
            .push((attname, JsonBuilderValue::f64_array(value)));
    }

    #[inline]
    pub fn add_time_array(&mut self, attname: String, value: Vec<Option<Time>>) {
        let value = value.into_iter().map(|t| t.map(|t| t.into())).collect();
        self.values
            .push((attname, JsonBuilderValue::time_array(value)));
    }

    #[inline]
    pub fn add_time_with_time_zone_array(
        &mut self,
        attname: String,
        value: Vec<Option<TimeWithTimeZone>>,
    ) {
        let value = value.into_iter().map(|t| t.map(|t| t.into())).collect();
        self.values
            .push((attname, JsonBuilderValue::time_with_time_zone_array(value)));
    }

    #[inline]
    pub fn add_timestamp_array(&mut self, attname: String, value: Vec<Option<Timestamp>>) {
        self.values.push((
            attname,
            JsonBuilderValue::timestamp_array(
                value
                    .into_iter()
                    .map(|ts| match ts {
                        Some(ts) => Some(ts.into()),
                        None => None,
                    })
                    .collect(),
            ),
        ));
    }

    #[inline]
    pub fn add_timestamp_with_time_zone_array(
        &mut self,
        attname: String,
        value: Vec<Option<TimestampWithTimeZone>>,
    ) {
        self.values.push((
            attname,
            JsonBuilderValue::timestamp_with_time_zone_array(
                value
                    .into_iter()
                    .map(|tsz| tsz.map(|tsz| tsz.into()))
                    .collect(),
            ),
        ));
    }

    #[inline]
    pub fn add_date_array(&mut self, attname: String, value: Vec<Option<Date>>) {
        let value = value.into_iter().map(|d| d.map(|d| d.into())).collect();
        self.values
            .push((attname, JsonBuilderValue::date_array(value)));
    }

    #[inline]
    pub fn add_string_array(&mut self, attname: String, value: Vec<Option<String>>) {
        self.values
            .push((attname, JsonBuilderValue::string_array(value)));
    }

    #[inline]
    pub fn add_json_string_array(
        &mut self,
        attname: String,
        value: Vec<Option<JsonStringWrapper>>,
    ) {
        self.values
            .push((attname, JsonBuilderValue::json_string_array(value)));
    }

    #[inline]
    pub fn add_jsonb_array(&mut self, attname: String, value: Vec<Option<JsonB>>) {
        self.values
            .push((attname, JsonBuilderValue::jsonb_array(value)));
    }

    pub fn build(&self, json: &mut Vec<u8>) {
        json.push(b'{');
        for (idx, (key, value)) in self.values.iter().enumerate() {
            if idx > 0 {
                json.push(b',');
            }

            // key was pre-quoted during categorize_tupdesc
            json.extend_from_slice(key.as_bytes());
            json.push(b':');

            match value {
                JsonBuilderValue::bool(v) => v.push_json(json),
                JsonBuilderValue::i16(v) => v.push_json(json),
                JsonBuilderValue::i32(v) => v.push_json(json),
                JsonBuilderValue::i64(v) => v.push_json(json),
                JsonBuilderValue::u32(v) => v.push_json(json),
                JsonBuilderValue::oid(v) => v.push_json(json),
                JsonBuilderValue::u64(v) => v.push_json(json),
                JsonBuilderValue::f32(v) => v.push_json(json),
                JsonBuilderValue::f64(v) => v.push_json(json),
                JsonBuilderValue::time(v) => v.push_json(json),
                JsonBuilderValue::time_with_time_zone(v) => v.push_json(json),
                JsonBuilderValue::timestamp(v) => v.push_json(json),
                JsonBuilderValue::timestamp_with_time_zone(v) => v.push_json(json),
                JsonBuilderValue::date(v) => v.push_json(json),
                JsonBuilderValue::string(v) => v.push_json(json),
                JsonBuilderValue::json_string(v) => v.push_json(json),
                JsonBuilderValue::jsonb(v) => v.push_json(json),
                JsonBuilderValue::json_value(v) => v.push_json(json),
                JsonBuilderValue::bool_array(v) => v.push_json(json),
                JsonBuilderValue::i16_array(v) => v.push_json(json),
                JsonBuilderValue::i32_array(v) => v.push_json(json),
                JsonBuilderValue::i64_array(v) => v.push_json(json),
                JsonBuilderValue::u32_array(v) => v.push_json(json),
                JsonBuilderValue::oid_array(v) => v.push_json(json),
                JsonBuilderValue::f32_array(v) => v.push_json(json),
                JsonBuilderValue::f64_array(v) => v.push_json(json),
                JsonBuilderValue::time_array(v) => v.push_json(json),
                JsonBuilderValue::time_with_time_zone_array(v) => v.push_json(json),
                JsonBuilderValue::timestamp_array(v) => v.push_json(json),
                JsonBuilderValue::timestamp_with_time_zone_array(v) => v.push_json(json),
                JsonBuilderValue::date_array(v) => v.push_json(json),
                JsonBuilderValue::string_array(v) => v.push_json(json),
                JsonBuilderValue::json_string_array(v) => v.push_json(json),
                JsonBuilderValue::jsonb_array(v) => v.push_json(json),
            }
        }
        json.push(b'}');
    }
}
