#[macro_use]
extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{
    Connection, HdbResult, ToHana, TypeId,
    time::{HanaOffsetDateTime, HanaPrimitiveDateTime},
};
use log::{debug, info};
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, macros::format_description};

#[tokio::test] // cargo test --test test_022_seconddate
pub async fn test_022_seconddate() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    test_seconddate(&mut loghandle, &connection).await?;

    test_utils::closing_info(connection, start).await
}

// Test the conversion of timestamps
// - during serialization (input to prepared_statements)
// - during deserialization (result)
async fn test_seconddate(_loghandle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    info!(
        "verify that {{Primitive|Offset}}DateTime values match the expected string representation"
    );

    #[derive(Deserialize)]
    struct WithTs {
        #[serde(deserialize_with = "hdbconnect_async::time::to_primitive_date_time")]
        ts_p: PrimitiveDateTime,
        #[serde(deserialize_with = "hdbconnect_async::time::to_offset_date_time")]
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
        .map(|pdt| OffsetDateTime::now_utc().replace_date_time(*pdt))
        .collect();
    let string_values = [
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
    let insert_stmt =
        |n, d| format!("insert into TEST_SECONDDATE (number,mydate) values({n}, '{d}')",);
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_SECONDDATE"])
        .await;
    connection
        .multiple_statements(vec![
            "create table TEST_SECONDDATE (number INT primary key, mydate SECONDDATE)",
            &insert_stmt(13, string_values[0]),
            &insert_stmt(14, string_values[1]),
            &insert_stmt(15, string_values[2]),
            &insert_stmt(16, string_values[3]),
            &insert_stmt(17, string_values[4]),
        ])
        .await?;

    {
        info!(
            "Insert data such that the conversion \"String -> SecondDate\" \
               is done on the client side."
        );
        let mut prepared_stmt = connection
            .prepare("insert into TEST_SECONDDATE (number,mydate)  values(?, ?)")
            .await
            .unwrap();
        assert_eq!(
            prepared_stmt.parameter_descriptors()[1].type_id(),
            TypeId::SECONDDATE
        );
        prepared_stmt
            .execute(&(&18, &"2018-09-20 17:31:41"))
            .await
            .unwrap();
    }

    {
        info!("test the conversion {{Primitive|Offset}}DateTime.to_hana() -> SecondDate");
        let mut prep_stmt = connection
            .prepare("select sum(number) from TEST_SECONDDATE where mydate = ? or mydate = ?")
            .await?;
        assert_eq!(
            prep_stmt.parameter_descriptors()[0].type_id(),
            TypeId::SECONDDATE
        );
        assert_eq!(
            prep_stmt.parameter_descriptors()[1].type_id(),
            TypeId::SECONDDATE
        );

        let response = prep_stmt
            .execute(&(
                primitive_datetime_values[2].to_hana(),
                offset_datetime_values[3].to_hana(),
            ))
            .await?;
        assert_eq!(response.into_result_set()?.try_into::<i32>().await?, 31);
    }

    {
        info!("test the conversion DB -> wire -> LongDate -> {{Primitive|Offset}}DateTime");
        debug!("Struct with field of type {{Offset|Primitive}}DateTime");
        let dates: Vec<WithTs> = connection.query(
            "select mydate as \"ts_p\", mydate as \"ts_o\" from TEST_SECONDDATE order by number asc"
        ).await?.try_into().await?;
        for (date, tvd) in dates.iter().zip(primitive_datetime_values.iter()) {
            assert_eq!(date.ts_p, *tvd);
        }
        for (date, tvd) in dates.iter().zip(offset_datetime_values.iter()) {
            assert_eq!(date.ts_o, *tvd);
        }

        debug!("Vec<Hana{{Offset|Primitive}}DateTime>");
        let dates: Vec<HanaOffsetDateTime> = connection
            .query("select mydate from TEST_SECONDDATE order by number asc")
            .await?
            .try_into()
            .await?;
        for (date, tvd) in dates.iter().zip(offset_datetime_values.iter()) {
            assert_eq!(**date, *tvd);
        }
        let dates: Vec<HanaPrimitiveDateTime> = connection
            .query("select mydate from TEST_SECONDDATE order by number asc")
            .await?
            .try_into()
            .await?;
        for (date, tvd) in dates.iter().zip(primitive_datetime_values.iter()) {
            assert_eq!(**date, *tvd);
        }

        debug!("Hana{{Offset|Primitive}}DateTime as single field");
        let date: HanaOffsetDateTime = connection
            .query("select mydate from TEST_SECONDDATE where number = 15")
            .await?
            .try_into()
            .await?;
        assert_eq!(*date, offset_datetime_values[2]);
        let date: HanaPrimitiveDateTime = connection
            .query("select mydate from TEST_SECONDDATE where number = 15")
            .await?
            .try_into()
            .await?;
        assert_eq!(*date, primitive_datetime_values[2]);

        debug!("Tuple with fields of type Hana{{Offset|Primitive}}DateTime");
        let dates: Vec<(HanaPrimitiveDateTime, HanaOffsetDateTime)> = connection.query(
            "select mydate as \"ts_p\", mydate as \"ts_o\" from TEST_SECONDDATE order by number asc"
        ).await?.try_into().await?;
        for (date, tvd) in dates.iter().zip(primitive_datetime_values.iter()) {
            assert_eq!(*date.0, *tvd);
        }
        for (date, tvd) in dates.iter().zip(offset_datetime_values.iter()) {
            assert_eq!(*date.1, *tvd);
        }
    }

    {
        info!("prove that '' is the same as '0001-01-01 00:00:00.000000000'");
        let rows_affected = connection.dml(&insert_stmt(77, "")).await?;
        assert_eq!(rows_affected, 1);
        let dates: Vec<HanaPrimitiveDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 77 or number = 13")
            .await?
            .try_into()
            .await?;
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(*date, primitive_datetime_values[0]);
        }

        let dates: Vec<HanaOffsetDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 77 or number = 13")
            .await?
            .try_into()
            .await?;
        assert_eq!(dates.len(), 2);
        for date in dates {
            assert_eq!(*date, offset_datetime_values[0]);
        }
    }

    {
        info!("test null values");
        let q = "insert into TEST_SECONDDATE (number) values(2350)";

        let rows_affected = connection.dml(q).await?;
        assert_eq!(rows_affected, 1);

        let date: Option<PrimitiveDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 2350")
            .await?
            .try_into()
            .await?;
        assert_eq!(date, None);

        let date: Option<OffsetDateTime> = connection
            .query("select mydate from TEST_SECONDDATE where number = 2350")
            .await?
            .try_into()
            .await?;
        assert_eq!(date, None);
    }

    Ok(())
}
