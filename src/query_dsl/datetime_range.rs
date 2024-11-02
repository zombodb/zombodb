#[pgrx::pg_schema]
mod pg_catalog {
    use pgrx::*;
    use serde::*;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum Relation {
        intersects,
        contains,
        within,
    }
}

#[pgrx::pg_schema]
mod dsl {
    use crate::misc::timestamp_support::{
        ZDBDate, ZDBTime, ZDBTimeWithTimeZone, ZDBTimestamp, ZDBTimestampWithTimeZone,
    };
    use crate::query_dsl::datetime_range::pg_catalog::*;
    use crate::zdbquery::ZDBQuery;
    use pgrx::prelude::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct DateTimeRange<T> {
        #[serde(skip_serializing_if = "Option::is_none")]
        lt: Option<T>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gt: Option<T>,
        #[serde(skip_serializing_if = "Option::is_none")]
        lte: Option<T>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gte: Option<T>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        relation: Option<Relation>,
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_date(
        field: &str,
        lt: default!(Option<Date>, NULL),
        gt: default!(Option<Date>, NULL),
        lte: default!(Option<Date>, NULL),
        gte: default!(Option<Date>, NULL),
        boost: default!(Option<f32>, NULL),
        relation: default!(Option<Relation>, "'intersects'"),
    ) -> ZDBQuery {
        let datetime_range = DateTimeRange::<ZDBDate> {
            lt: lt.map(|t| t.into()),
            lte: lte.map(|t| t.into()),
            gt: gt.map(|t| t.into()),
            gte: gte.map(|t| t.into()),
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_time(
        field: &str,
        lt: default!(Option<Time>, NULL),
        gt: default!(Option<Time>, NULL),
        lte: default!(Option<Time>, NULL),
        gte: default!(Option<Time>, NULL),
        boost: default!(Option<f32>, NULL),
        relation: default!(Option<Relation>, "'intersects'"),
    ) -> ZDBQuery {
        let datetime_range: DateTimeRange<ZDBTime> = DateTimeRange {
            lt: lt.map(|t| t.into()),
            lte: lte.map(|t| t.into()),
            gt: gt.map(|t| t.into()),
            gte: gte.map(|t| t.into()),
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_time_stamp(
        field: &str,
        lt: default!(Option<Timestamp>, NULL),
        gt: default!(Option<Timestamp>, NULL),
        lte: default!(Option<Timestamp>, NULL),
        gte: default!(Option<Timestamp>, NULL),
        boost: default!(Option<f32>, NULL),
        relation: default!(Option<Relation>, "'intersects'"),
    ) -> ZDBQuery {
        let datetime_range: DateTimeRange<ZDBTimestamp> = DateTimeRange {
            lt: lt.map(|ts| ts.into()),
            lte: lte.map(|ts| ts.into()),
            gt: gt.map(|ts| ts.into()),
            gte: gte.map(|ts| ts.into()),
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_timestamp_with_timezone(
        field: &str,
        lt: default!(Option<TimestampWithTimeZone>, NULL),
        gt: default!(Option<TimestampWithTimeZone>, NULL),
        lte: default!(Option<TimestampWithTimeZone>, NULL),
        gte: default!(Option<TimestampWithTimeZone>, NULL),
        boost: default!(Option<f32>, NULL),
        relation: default!(Option<Relation>, "'intersects'"),
    ) -> ZDBQuery {
        let datetime_range: DateTimeRange<ZDBTimestampWithTimeZone> = DateTimeRange {
            lt: lt.map(|tsz| tsz.into()),
            lte: lte.map(|tsz| tsz.into()),
            gt: gt.map(|tsz| tsz.into()),
            gte: gte.map(|tsz| tsz.into()),
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_time_with_timezone(
        field: &str,
        lt: default!(Option<TimeWithTimeZone>, NULL),
        gt: default!(Option<TimeWithTimeZone>, NULL),
        lte: default!(Option<TimeWithTimeZone>, NULL),
        gte: default!(Option<TimeWithTimeZone>, NULL),
        boost: default!(Option<f32>, NULL),
        relation: default!(Option<Relation>, "'intersects'"),
    ) -> ZDBQuery {
        let datetime_range: DateTimeRange<ZDBTimeWithTimeZone> = DateTimeRange {
            lt: lt.map(|t| t.into()),
            lte: lte.map(|t| t.into()),
            gt: gt.map(|t| t.into()),
            gte: gte.map(|t| t.into()),
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[inline]
    fn make_datetime_range_dsl<T: serde::Serialize>(
        field: &str,
        datetime_range: DateTimeRange<T>,
    ) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "range": {
                    field: datetime_range
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgrx::*;
    use serde_json::json;

    #[pg_test]
    fn test_datetime_range_date_with_inputs() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.datetime_range(
            'fieldname',
             CAST('05-09-2001' AS date),
             CAST('06-10-2002' AS date),
             CAST('07-11-2003' AS date),
             CAST('08-12-2004' AS date),
             42,
            'contains'
              );",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "fieldname": {
                            "lt": "2001-05-09",
                            "gt": "2002-06-10",
                            "lte": "2003-07-11",
                            "gte": "2004-08-12",
                            "boost": 42_f32,
                            "relation": "contains"
                        }
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_datetime_range_time_with_inputs() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.datetime_range(
            'fieldname',
             CAST('12:34:56' AS time),
             CAST('21:43:54' AS time),
             CAST('10:34:24' AS time),
             CAST('09:45:30' AS time),
             5.0,
            'contains'
              );",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "fieldname": {
                            "lt": "12:34:56",
                            "gt": "21:43:54",
                            "lte": "10:34:24",
                            "gte": "09:45:30",
                            "boost": 5.0_f32,
                            "relation": "contains"
                        }
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_datetime_range_time_with_timezone_with_inputs() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.datetime_range(
            'fieldname',
             CAST('12:34:56 -0700' AS time with time zone),
             CAST('21:43:54 -0700' AS time with time zone),
             CAST('10:34:24 -0700' AS time with time zone),
             CAST('09:45:30 -0700' AS time with time zone),
             5.0,
            'contains'
              );",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "fieldname": {
                            "lt": "12:34:56-0700",
                            "gt": "21:43:54-0700",
                            "lte": "10:34:24-0700",
                            "gte": "09:45:30-0700",
                            "boost": 5.0_f32,
                            "relation": "contains"
                        }
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_datetime_range_timestamp_with_inputs() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.datetime_range(
            'fieldname',
             CAST('01-02-2003 12:34:56' AS timestamp),
             CAST('04-05-2006 21:43:54' AS timestamp),
             CAST('07-08-2009 10:34:24' AS timestamp),
             CAST('10-11-2012 09:45:30' AS timestamp),
             5.0,
            'contains'
              );",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "fieldname": {
                            "lt": "2003-01-02T12:34:56-00",
                            "gt": "2006-04-05T21:43:54-00",
                            "lte": "2009-07-08T10:34:24-00",
                            "gte": "2012-10-11T09:45:30-00",
                            "boost": 5.0_f32,
                            "relation": "contains"
                        }
                    }
                }
            }
        );
    }

    #[pg_test]
    fn test_datetime_range_timestamp_with_timezones_with_inputs() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.datetime_range(
            'fieldname',
             CAST('01-02-2003 12:34:56 -0700' AS timestamp with time zone),
             CAST('04-05-2006 21:43:54 -0700' AS timestamp with time zone),
             CAST('07-08-2009 10:34:24 -0700' AS timestamp with time zone),
             CAST('10-11-2012 09:45:30 -0700' AS timestamp with time zone),
             5.0,
            'contains'
              );",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "fieldname": {
                            "lt": "2003-01-02T19:34:56+00:00",
                            "gt": "2006-04-06T04:43:54+00:00",
                            "lte": "2009-07-08T17:34:24+00:00",
                            "gte": "2012-10-11T16:45:30+00:00",
                            "boost": 5.0_f32,
                            "relation": "contains"
                        }
                    }
                }
            }
        );
    }

    #[pg_test(error = "function dsl.datetime_range(unknown) is not unique")]
    fn test_datetime_range_with_defaults() {
        Spi::get_one::<ZDBQuery>("SELECT dsl.datetime_range('fieldname');")
            .expect("SPI failed")
            .expect("SPI datum was NULL");
    }
}
