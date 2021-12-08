#[macro_use]
extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{
    time::{HanaOffsetDateTime, HanaPrimitiveDateTime},
    Connection, HdbResult, ToHana, TypeId,
};
use log::{debug, info};
use time::{macros::format_description, Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

#[test] // cargo test --test test_022_seconddate
pub fn test_022_seconddate() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection()?;

    test_seconddate(&mut loghandle, &mut connection)?;

    test_utils::closing_info(connection, start)
}

// Test the conversion of timestamps
// - during serialization (input to prepared_statements)
// - during deserialization (result)
fn test_seconddate(_loghandle: &mut LoggerHandle, connection: &mut Connection) -> HdbResult<()> {
    info!(
        "verify that {{Primitive|Offset}}DateTime values match the expected string representation"
    );

    #[derive(Deserialize)]
    struct WithTs {
        #[serde(deserialize_with = "hdbconnect::time::to_primitive_date_time")]
        ts_p: PrimitiveDateTime,
        #[serde(deserialize_with = "hdbconnect::time::to_offset_date_time")]
        ts_o: OffsetDateTime,
    }

    debug!("test_seconddate: prepare the test data");
    // three different representations of the same timestamps
    let primitive_datetime_values: Vec<PrimitiveDateTime> = vec![
        PrimitiveDateTime::new(
            Date::from_calendar_date(1, Month::January, 1).unwrap(),
            Time::from_hms(0, 0, 0).unwrap(),
        ),
        PrimitiveDateTime::new(
            Date::from_calendar_date(1, Month::January, 1).unwrap(),
            Time::from_hms(0, 0, 0).unwrap(),
        ),
        PrimitiveDateTime::new(
            Date::from_calendar_date(2012, Month::February, 2).unwrap(),
            Time::from_hms(2, 2, 2).unwrap(),
        ),
        PrimitiveDateTime::new(
            Date::from_calendar_date(2013, Month::March, 3).unwrap(),
            Time::from_hms(3, 3, 3).unwrap(),
        ),
        PrimitiveDateTime::new(
            Date::from_calendar_date(2014, Month::April, 4).unwrap(),
            Time::from_hms(4, 4, 4).unwrap(),
        ),
    ];
    let offset_datetime_values: Vec<OffsetDateTime> = primitive_datetime_values
        .iter()
        .map(|pdt| OffsetDateTime::now_utc().replace_date_time(pdt.clone()))
        .collect();
    let string_values = vec![
        "0001-01-01 00:00:00",
        "0001-01-01 00:00:00",
        "2012-02-02 02:02:02",
        "2013-03-03 03:03:03",
        "2014-04-04 04:04:04",
    ];
    for i in 0..5 {
        assert_eq!(
            primitive_datetime_values[i]
                .format(format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                ))
                .unwrap(),
            string_values[i]
        );
        assert_eq!(
            offset_datetime_values[i]
                .format(format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                ))
                .unwrap(),
            string_values[i]
        );
    }

    // Insert the data such that the conversion "String -> SecondDate" is done on the
    // server side (we assume that this conversion is error-free).
    let insert_stmt = |n, d| {
        format!(
            "insert into TEST_SECONDDATE (number,mydate) values({}, '{}')",
            n, d
        )
    };
    connection.multiple_statements_ignore_err(vec!["drop table TEST_SECONDDATE"]);
    connection.multiple_statements(vec![
        "create table TEST_SECONDDATE (number INT primary key, mydate SECONDDATE)",
        &insert_stmt(13, string_values[0]),
        &insert_stmt(14, string_values[1]),
        &insert_stmt(15, string_values[2]),
        &insert_stmt(16, string_values[3]),
        &insert_stmt(17, string_values[4]),
    ])?;

    {
        info!(
            "Insert data such that the conversion \"String -> SecondDate\" \
               is done on the client side."
        );
        let mut prepared_stmt = connection
            .prepare("insert into TEST_SECONDDATE (number,mydate)  values(?, ?)")
            .unwrap();
        assert_eq!(
            prepared_stmt.parameter_descriptors()[1].type_id(),
            TypeId::SECONDDATE
        );
        prepared_stmt
            .execute(&(&18, &"2018-09-20 17:31:41"))
            .unwrap();
    }

    {
        info!("test the conversion {{Primitive|Offset}}DateTime.to_hana() -> SecondDate");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_SECONDDATE where mydate = ? or mydate = ?")?;
        assert_eq!(
            prep_stmt.parameter_descriptors()[0].type_id(),
            TypeId::SECONDDATE
        );
        assert_eq!(
            prep_stmt.parameter_descriptors()[1].type_id(),
            TypeId::SECONDDATE
        );

        let response = prep_stmt.execute(&(
            primitive_datetime_values[2].to_hana(),
            offset_datetime_values[3].to_hana(),
        ))?;
        assert_eq!(response.into_resultset()?.try_into::<i32>()?, 31);
    }

    {
        info!("test the conversion DB -> wire -> LongDate -> {{Primitive|Offset}}DateTime");
        debug!("Struct with field of type {{Offset|Primitive}}DateTime");
        let dates: Vec<WithTs> = connection.query(
            "select mydate as \"ts_p\", mydate as \"ts_o\" from TEST_SECONDDATE order by number asc"
        )?.try_into()?;
        for (date, tvd) in dates.iter().zip(primitive_datetime_values.iter()) {
            assert_eq!(date.ts_p, *tvd);
        }
        for (date, tvd) in dates.iter().zip(offset_datetime_values.iter()) {
            assert_eq!(date.ts_o, *tvd);
        }

        debug!("Vec<Hana{{Offset|Primitive}}DateTime>");
        let dates: Vec<HanaOffsetDateTime> = connection
            .query("select mydate from TEST_SECONDDATE order by number asc")?
            .try_into()?;
        for (date, tvd) in dates.iter().zip(offset_datetime_values.iter()) {
            assert_eq!(**date, *tvd);
        }
        let dates: Vec<HanaPrimitiveDateTime> = connection
            .query("select mydate from TEST_SECONDDATE order by number asc")?
            .try_into()?;
        for (date, tvd) in dates.iter().zip(primitive_datetime_values.iter()) {
            assert_eq!(**date, *tvd);
        }

        debug!("Hana{{Offset|Primitive}}DateTime as single field");
        let date: HanaOffsetDateTime = connection
            .query("select mydate from TEST_SECONDDATE where number = 15")?
            .try_into()?;
        assert_eq!(*date, offset_datetime_values[2]);
        let date: HanaPrimitiveDateTime = connection
            .query("select mydate from TEST_SECONDDATE where number = 15")?
            .try_into()?;
        assert_eq!(*date, primitive_datetime_values[2]);

        debug!("Tuple with fields of type Hana{{Offset|Primitive}}DateTime");
        let dates: Vec<(HanaPrimitiveDateTime, HanaOffsetDateTime)> = connection.query(
            "select mydate as \"ts_p\", mydate as \"ts_o\" from TEST_SECONDDATE order by number asc"
        )?.try_into()?;
        for (date, tvd) in dates.iter().zip(primitive_datetime_values.iter()) {
            assert_eq!(*date.0, *tvd);
        }
        for (date, tvd) in dates.iter().zip(offset_datetime_values.iter()) {
            assert_eq!(*date.1, *tvd);
        }
    }

    {
        info!("prove that '' is the same as '0001-01-01 00:00:00.000000000'");
        let rows_affected = connection.dml(&insert_stmt(77, ""))?;
        assert_eq!(rows_affected, 1);
        let dates: Vec<HanaPrimitiveDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 77 or number = 13")?
            .try_into()?;
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(*date, primitive_datetime_values[0]);
        }

        let dates: Vec<HanaOffsetDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 77 or number = 13")?
            .try_into()?;
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(*date, offset_datetime_values[0]);
        }
    }

    {
        info!("test null values");
        let q = "insert into TEST_SECONDDATE (number) values(2350)";

        let rows_affected = connection.dml(q)?;
        assert_eq!(rows_affected, 1);

        let date: Option<PrimitiveDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 2350")?
            .try_into()?;
        assert_eq!(date, None);

        let date: Option<OffsetDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 2350")?
            .try_into()?;
        assert_eq!(date, None);
    }

    Ok(())
}
