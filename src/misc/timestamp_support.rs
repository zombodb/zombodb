use pgrx::{Date, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone, ToIsoString};
use serde::{Serialize, Serializer};

#[derive(Debug)]
#[repr(transparent)]
pub struct ZDBTimestamp(pub String);

#[derive(Debug)]
#[repr(transparent)]
pub struct ZDBTime(pub String);

#[derive(Debug)]
#[repr(transparent)]
pub struct ZDBTimeWithTimeZone(pub String);

#[derive(Debug)]
#[repr(transparent)]
pub struct ZDBDate(pub String);

#[derive(Debug)]
#[repr(transparent)]
pub struct ZDBTimestampWithTimeZone(pub String);

impl From<Timestamp> for ZDBTimestamp {
    fn from(ts: Timestamp) -> Self {
        ZDBTimestamp(ts.to_iso_string_with_timezone("UTC").unwrap() + "-00")
    }
}

impl From<TimestampWithTimeZone> for ZDBTimestampWithTimeZone {
    fn from(tsz: TimestampWithTimeZone) -> Self {
        ZDBTimestampWithTimeZone(tsz.to_iso_string_with_timezone("UTC").unwrap())
    }
}

impl From<Time> for ZDBTime {
    fn from(t: Time) -> Self {
        ZDBTime(t.to_iso_string_with_timezone("UTC").unwrap())
    }
}

impl From<TimeWithTimeZone> for ZDBTimeWithTimeZone {
    fn from(tz: TimeWithTimeZone) -> Self {
        let seconds = tz.second();
        let second_left = seconds as u64;
        let second_right = seconds.to_string();
        let mut parts = second_right.split('.');
        let _ = parts.next();
        let right = parts.next().unwrap_or("0");
        let right = &right[0..6.min(right.len())];
        let s = format!(
            "{:02}:{:02}:{:02}{}{}",
            tz.hour(),
            tz.minute(),
            second_left,
            if right.parse::<u64>().unwrap() > 0 {
                format!(".{:}", right)
            } else {
                "".to_string()
            },
            if tz.timezone_offset() == 0 {
                "Z".to_string()
            } else {
                let hour = tz.timezone_hour();
                let neg = hour < 0;
                let hour = hour.abs();
                format!("{}", if neg { "-" } else { "" })
                    + &format!("{:02}", hour)
                    + &format!("{:02}", tz.timezone_minute())
            }
        );
        ZDBTimeWithTimeZone(s)
    }
}

impl From<Date> for ZDBDate {
    fn from(t: Date) -> Self {
        ZDBDate(t.to_iso_string())
    }
}

macro_rules! serialize {
    ($t:ty) => {
        impl Serialize for $t {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }
    };
}

serialize!(ZDBTimestamp);
serialize!(ZDBTime);
serialize!(ZDBTimeWithTimeZone);
serialize!(ZDBDate);
serialize!(ZDBTimestampWithTimeZone);
