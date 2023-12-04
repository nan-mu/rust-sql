use std::fmt::Error;

#[test]
fn test_db_init() -> Result<(), Error> {
    use crate::db_option::{init_database, insert_item, Build, Item, PmAndYear};
    use rusqlite::Connection;
    use time::Date;
    init_database().unwrap();

    let conn = Connection::open("air_quality.db").expect("Could not open database");

    let sample_item = Item::new(
        "Sample Region",
        "Sample Subregion",
        "Sample Country",
        "Sample City",
        PmAndYear::Pm10(25.5, Date::from_ordinal_date(2023, 1).unwrap()),
        PmAndYear::Pm25(15.3, Date::from_ordinal_date(2023, 1).unwrap()),
    );

    match insert_item(&conn, &sample_item) {
        Ok(()) => println!("Item inserted successfully"),
        Err(err) => eprintln!("Error inserting item: {}", err),
    };
    Ok(())
}

#[test]
fn test_load_csv() {
    use crate::db_option::{init_database, load_from_csv};
    init_database().unwrap();
    load_from_csv().unwrap();
}

#[test]
fn test_query() {
    use crate::db_option::{Build, Item, PmAndYear};
    use rusqlite::Connection;
    use time::Date;
    let conn = Connection::open("air_quality.db").expect("Could not open database");
    let query = "select * from air_quality where 1=1 and id=1;".to_string();
    let mut stmt = conn.prepare(&query).unwrap();
    let item_iter = stmt
        .query_map([], |row| {
            println!("{:?}", row);
            Ok(Item::new(
                &(row.get::<usize, String>(1).unwrap()),
                &(row.get::<usize, String>(2).unwrap()),
                &(row.get::<usize, String>(3).unwrap()),
                &(row.get::<usize, String>(4).unwrap()),
                match row.get::<usize, f64>(5) {
                    Ok(num) => PmAndYear::Pm10(
                        num,
                        Date::from_ordinal_date(
                            match row.get::<usize, i64>(6) {
                                Ok(year) => year as i32,
                                Err(_) => row.get::<usize, i64>(8).unwrap() as i32,
                            },
                            1,
                        )
                        .unwrap(),
                    ),
                    Err(e) => {
                        println!("问题数据：{:?}；可能缺少pm2.5", row);
                        println!("{:?}", e);
                        PmAndYear::None
                    }
                },
                match row.get::<usize, f64>(7) {
                    Ok(num) => PmAndYear::Pm10(
                        num,
                        Date::from_ordinal_date(
                            match row.get::<usize, i64>(8) {
                                Ok(year) => year as i32,
                                Err(_) => row.get::<usize, i64>(6).unwrap() as i32,
                            },
                            1,
                        )
                        .unwrap(),
                    ),
                    Err(e) => {
                        println!("问题数据：{:?}；可能缺少pm10", row);
                        println!("{:?}", e);
                        PmAndYear::None
                    }
                },
            ))
        })
        .unwrap();
    let result: String = item_iter
        .map(|s| s.unwrap().to_string())
        .collect::<Vec<String>>()
        .join("\n");
    println!("{}", &result);
}
