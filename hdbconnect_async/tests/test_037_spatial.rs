extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult, TypeId};
use log::{debug, info};
use serde_bytes::{ByteBuf, Bytes};

#[tokio::test] // cargo test --test test_046_spatial
async fn test_046_spatial() -> HdbResult<()> {
    let mut loghandle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;

    test_geometries(&mut loghandle, &mut connection).await?;
    test_points(&mut loghandle, &mut connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn test_geometries(
    _loghandle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("write and read GEOMETRY data");

    // Insert the data such that the conversion "String -> WKB" is done on the
    // server side (we assume that this conversion is error-free).
    connection
        .multiple_statements_ignore_err(vec!["drop table SpatialShapes"])
        .await;
    connection.multiple_statements(vec![
        "CREATE COLUMN TABLE SpatialShapes( \
		    ShapeID integer, \
		    shape 	ST_GEOMETRY \
	)",
    "INSERT INTO SpatialShapes VALUES(1, NEW ST_Point('Point(2.5 3.0)'))",
    "INSERT INTO SpatialShapes VALUES(2, NEW ST_Point('Point(3.0 4.5)'))",
    "INSERT INTO SpatialShapes VALUES(3, NEW ST_Point('Point(3.0 6.0)'))",
    "INSERT INTO SpatialShapes VALUES(4, NEW ST_Point('Point(4.0 6.0)'))",
    "INSERT INTO SpatialShapes VALUES(5, NEW ST_Point())",
    "INSERT INTO SpatialShapes VALUES(6, NEW ST_LineString('LineString(3.0 3.0, 5.0 4.0, 6.0 3.0)'))",
    "INSERT INTO SpatialShapes VALUES(7, NEW ST_LineString('LineString(4.0 4.0, 6.0 5.0, 7.0 4.0)'))",
    "INSERT INTO SpatialShapes VALUES(8, NEW ST_LineString('LineString(7.0 5.0, 9.0 7.0)'))",
    "INSERT INTO SpatialShapes VALUES(9, NEW ST_LineString('LineString(7.0 3.0, 8.0 5.0)'))",
    "INSERT INTO SpatialShapes VALUES(10, NEW ST_LineString())",
    "INSERT INTO SpatialShapes VALUES(11, NEW ST_Polygon('Polygon((6.0 7.0, 10.0 3.0, 10.0 10.0, 6.0 7.0))'))",
    "INSERT INTO SpatialShapes VALUES(12, NEW ST_Polygon('Polygon((4.0 5.0, 5.0 3.0, 6.0 5.0, 4.0 5.0))'))",
    "INSERT INTO SpatialShapes VALUES(13, NEW ST_Polygon('Polygon((1.0 1.0, 1.0 6.0, 6.0 6.0, 6.0 1.0, 1.0 1.0))'))",
    "INSERT INTO SpatialShapes VALUES(14, NEW ST_Polygon('Polygon((1.0 3.0, 1.0 4.0, 5.0 4.0, 5.0 3.0, 1.0 3.0))'))",
    "INSERT INTO SpatialShapes VALUES(15, NEW ST_Polygon())",
    "INSERT INTO SpatialShapes VALUES(16, NEW ST_MultiPoint('MultiPoint( (0 1), (2 2), (5 3), (7 2), (9 3), (8 4), (6 6), (6 9), (4 9), (1 5) )'))",
    "INSERT INTO SpatialShapes VALUES(17, NEW ST_MultiPoint('MultiPoint( (0 0), (1 1), (2 2), (3 3) )'))",
    ]).await?;

    debug!("select and deserialize (use serde)");
    let resultset = connection.query("select shape from SpatialShapes").await?;
    assert_eq!(resultset.metadata()[0].type_id(), TypeId::GEOMETRY);
    debug!("Resultset = {}", resultset);
    let shapes: Vec<ByteBuf> = resultset.try_into().await?;

    debug!("insert via parameters (use serde)");
    let mut stmt = connection
        .prepare("insert into SpatialShapes VALUES(?,?)")
        .await?;
    for (idx, shape) in shapes.iter().enumerate() {
        stmt.add_batch(&(idx + 100, shape))?;
    }
    stmt.execute_batch().await?;

    let count: u16 = connection
        .query("select count(*) from SpatialShapes")
        .await?
        .try_into()
        .await?;
    assert_eq!(count, 34);
    Ok(())
}

async fn test_points(_loghandle: &mut LoggerHandle, connection: &mut Connection) -> HdbResult<()> {
    info!("write and read POINT data");

    connection
        .multiple_statements_ignore_err(vec!["drop table Points"])
        .await;

    // SHAPE2 ST_Point BOUNDARY CHECK ON, \
    // SHAPE3 ST_Point BOUNDARY CHECK OFF, \
    // SHAPE4 ST_Point(1000004326), \
    // SHAPE5 ST_Point(1000004326) VALIDATION NONE, \
    // SHAPE6 ST_Point(4326) VALIDATION FULL \

    connection
        .multiple_statements(vec![
            "CREATE COLUMN TABLE Points( \
         ID integer, \
         SHAPE1 ST_Point \
         )",
            "INSERT INTO Points VALUES(1, NEW ST_Point('Point(2.5 3.0)'))",
            // "INSERT INTO Points VALUES(2, NEW ST_Point('Point(3.0 4.5)'))",
            // "INSERT INTO Points VALUES(3, NEW ST_Point('Point(3.0 6.0)'))",
            // "INSERT INTO Points VALUES(4, NEW ST_Point('Point(4.0 6.0)'))",
            // "INSERT INTO Points VALUES(5, NEW ST_Point())",
        ])
        .await?;

    debug!("select and deserialize (use serde)");
    let resultset = connection.query("select shape1 from Points").await?;
    assert_eq!(resultset.metadata()[0].type_id(), TypeId::POINT);
    let shapes: Vec<ByteBuf> = resultset.try_into().await?;

    debug!("insert via parameters (use serde)");
    let mut stmt = connection.prepare("insert into Points VALUES(?,?)").await?;
    for (idx, shape) in shapes.iter().enumerate() {
        stmt.add_batch(&(idx + 100, shape))?;
    }
    stmt.execute_batch().await?;

    let count: u16 = connection
        .query("select count(*) from Points")
        .await?
        .try_into()
        .await?;
    assert_eq!(count, 2);

    // btw: here we get parameter type id 31 = BLOCATOR:
    // so this seems to manipulate the statement itself !?!?
    let mut stmt = connection
        .prepare("insert into Points VALUES(?,NEW ST_POINT(?))")
        .await?;
    debug!("Descriptors: {:?}", stmt.parameter_descriptors());
    stmt.add_batch(&(22, Bytes::new(b"Point(2.5 3.0)")))?;
    stmt.execute_batch().await?;

    // here just a POINT
    // let mut stmt = connection.prepare("insert into Points VALUES(?,?)")?;
    // here we would have to use WKB

    // much better would be stmt.add_batch(&(1,"Point(2.5 3.0)"))?;

    // stmt.execute_batch()?;
    // let count: u16 = connection.query("select count(*) from Points")?.try_into()?;
    // assert_eq!(count, 3);
    Ok(())
}
