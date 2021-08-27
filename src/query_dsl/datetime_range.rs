#[pgx_macros::pg_schema]
mod pg_catalog {
    use pgx::*;
    use serde::*;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum Relation {
        intersects,
        contains,
        within,
    }
}

#[pgx_macros::pg_schema]
mod dsl {
    use crate::query_dsl::datetime_range::pg_catalog::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
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
        lt: Option<default!(Date, "NULL")>,
        gt: Option<default!(Date, "NULL")>,
        lte: Option<default!(Date, "NULL")>,
        gte: Option<default!(Date, "NULL")>,
        boost: Option<default!(f32, "NULL")>,
        relation: Option<default!(Relation, "'intersects'")>,
    ) -> ZDBQuery {
        let datetime_range = DateTimeRange {
            lt,
            lte,
            gt,
            gte,
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_time(
        field: &str,
        lt: Option<default!(Time, "NULL")>,
        gt: Option<default!(Time, "NULL")>,
        lte: Option<default!(Time, "NULL")>,
        gte: Option<default!(Time, "NULL")>,
        boost: Option<default!(f32, "NULL")>,
        relation: Option<default!(Relation, "'intersects'")>,
    ) -> ZDBQuery {
        let datetime_range: DateTimeRange<Time> = DateTimeRange {
            lt,
            lte,
            gt,
            gte,
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_time_stamp(
        field: &str,
        lt: Option<default!(Timestamp, "NULL")>,
        gt: Option<default!(Timestamp, "NULL")>,
        lte: Option<default!(Timestamp, "NULL")>,
        gte: Option<default!(Timestamp, "NULL")>,
        boost: Option<default!(f32, "NULL")>,
        relation: Option<default!(Relation, "'intersects'")>,
    ) -> ZDBQuery {
        let datetime_range: DateTimeRange<Timestamp> = DateTimeRange {
            lt,
            lte,
            gt,
            gte,
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_timestamp_with_timezone(
        field: &str,
        lt: Option<default!(TimestampWithTimeZone, "NULL")>,
        gt: Option<default!(TimestampWithTimeZone, "NULL")>,
        lte: Option<default!(TimestampWithTimeZone, "NULL")>,
        gte: Option<default!(TimestampWithTimeZone, "NULL")>,
        boost: Option<default!(f32, "NULL")>,
        relation: Option<default!(Relation, "'intersects'")>,
    ) -> ZDBQuery {
        let datetime_range: DateTimeRange<TimestampWithTimeZone> = DateTimeRange {
            lt,
            lte,
            gt,
            gte,
            boost,
            relation,
        };
        make_datetime_range_dsl(field, datetime_range)
    }

    #[pg_extern(immutable, parallel_safe, name = "datetime_range")]
    fn datetime_range_time_with_timezone(
        field: &str,
        lt: Option<default!(TimeWithTimeZone, "NULL")>,
        gt: Option<default!(TimeWithTimeZone, "NULL")>,
        lte: Option<default!(TimeWithTimeZone, "NULL")>,
        gte: Option<default!(TimeWithTimeZone, "NULL")>,
        boost: Option<default!(f32, "NULL")>,
        relation: Option<default!(Relation, "'intersects'")>,
    ) -> ZDBQuery {
        let datetime_range: DateTimeRange<TimeWithTimeZone> = DateTimeRange {
            lt,
            lte,
            gt,
            gte,
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
#[pgx_macros::pg_schema]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
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
        .expect("didn't get SPI return value");
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
                            "boost": 42 as f32,
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
        .expect("didn't get SPI return value");
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
                            "boost": 5.0 as f32,
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
        .expect("didn't get SPI return value");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "fieldname": {
                            "lt": "19:34:56-00",
                            "gt": "04:43:54-00",
                            "lte": "17:34:24-00",
                            "gte": "16:45:30-00",
                            "boost": 5.0 as f32,
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
        .expect("didn't get SPI return value");
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
                            "boost": 5.0 as f32,
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
        .expect("didn't get SPI return value");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "fieldname": {
                            "lt": "2003-01-02T19:34:56-00",
                            "gt": "2006-04-06T04:43:54-00",
                            "lte": "2009-07-08T17:34:24-00",
                            "gte": "2012-10-11T16:45:30-00",
                            "boost": 5.0 as f32,
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
            .expect("didn't get SPI return value");
    }
}
