use pgx::{Timestamp, TimestampWithTimeZone};
use std::convert::TryInto;
use time::format_description::FormatItem;

#[derive(Debug)]
#[repr(transparent)]
pub struct ZDBTimestamp(time::PrimitiveDateTime);

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
pub struct ZDBTimestampWithTimeZone(time::OffsetDateTime);

impl From<Timestamp> for ZDBTimestamp {
    fn from(ts: Timestamp) -> Self {
        ZDBTimestamp(
            ts.try_into()
                .expect("failed to convert pgx::Timestamp to ZDBTimestamp"),
        )
    }
}

impl From<TimestampWithTimeZone> for ZDBTimestampWithTimeZone {
    fn from(tsz: TimestampWithTimeZone) -> Self {
        ZDBTimestampWithTimeZone(
            tsz.try_into()
                .expect("failed to convert pgx::TimestampWithTimeZone to ZDBTimestampWithTimeZone"),
        )
    }
}

impl serde::Serialize for ZDBTimestamp {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        if self.0.millisecond() > 0 {
            serializer.serialize_str(
                &self
                    .0
                    .format(
                        &time::format_description::parse(&format!(
                            "[year]-[month]-[day]T[hour]:[minute]:[second].{}-00",
                            self.0.millisecond()
                        ))
                        .map_err(|e| {
                            serde::ser::Error::custom(format!(
                                "Timestamp invalid format problem: {:?}",
                                e
                            ))
                        })?,
                    )
                    .map_err(|e| {
                        serde::ser::Error::custom(format!("Timestamp formatting problem: {:?}", e))
                    })?,
            )
        } else {
            serializer.serialize_str(&self.0.format(&DEFAULT_TIMESTAMP_FORMAT).map_err(|e| {
                serde::ser::Error::custom(format!("Timestamp formatting problem: {:?}", e))
            })?)
        }
    }
}

static DEFAULT_TIMESTAMP_FORMAT: &[FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]-00");

impl serde::Serialize for ZDBTimestampWithTimeZone {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        if self.0.millisecond() > 0 {
            serializer.serialize_str(
                &self
                    .0
                    .format(
                        &time::format_description::parse(&format!(
                            "[year]-[month]-[day]T[hour]:[minute]:[second].{}-00",
                            self.0.millisecond()
                        ))
                        .map_err(|e| {
                            serde::ser::Error::custom(format!(
                                "TimeStampWithTimeZone invalid format problem: {:?}",
                                e
                            ))
                        })?,
                    )
                    .map_err(|e| {
                        serde::ser::Error::custom(format!(
                            "TimeStampWithTimeZone formatting problem: {:?}",
                            e
                        ))
                    })?,
            )
        } else {
            serializer.serialize_str(
                &self
                    .0
                    .format(&DEFAULT_TIMESTAMP_WITH_TIMEZONE_FORMAT)
                    .map_err(|e| {
                        serde::ser::Error::custom(format!(
                            "TimeStampWithTimeZone formatting problem: {:?}",
                            e
                        ))
                    })?,
            )
        }
    }
}

static DEFAULT_TIMESTAMP_WITH_TIMEZONE_FORMAT: &[FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]-00");
