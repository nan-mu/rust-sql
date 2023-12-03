use std::fs;

use csv;
use std::path::Path;
use time::Date;

use rusqlite::{params, Connection, Result};
#[derive(Debug)]
pub struct Item {
    region: String,
    subregion: String,
    country: String,
    city: String,
    pm10: f64,
    pm10_year: Date,
    pm25: f64,
    pm25_year: Date,
}

impl Item {
    pub fn new(
        region: &str,
        subregion: &str,
        country: &str,
        city: &str,
        pm10: f64,
        pm10_year: Date,
        pm25: f64,
        pm25_year: Date,
    ) -> Self {
        Item {
            region: region.to_string(),
            subregion: subregion.to_string(),
            country: country.to_string(),
            city: city.to_string(),
            pm10,
            pm10_year,
            pm25,
            pm25_year,
        }
    }
}

pub fn init_database() -> Result<()> {
    //假如数据库文件存在，则删除。
    let db_path = Path::new("air_quality.db");
    if db_path.exists() {
        fs::remove_file(db_path).unwrap();
    }
    let conn = Connection::open(db_path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS air_quality (
             id INTEGER PRIMARY KEY,
             region TEXT NOT NULL,
             subregion TEXT NOT NULL,
             country TEXT NOT NULL,
             city TEXT NOT NULL,
             pm10 REAL NOT NULL,
             pm10_year INTEGER NOT NULL,
             pm25 REAL NOT NULL,
             pm25_year INTEGER NOT NULL
         )",
        [],
    )?;

    Ok(())
}

pub fn insert_item(conn: &Connection, item: &Item) -> Result<()> {
    conn.execute(
        "INSERT INTO air_quality (region, subregion, country, city, pm10, pm10_year, pm25, pm25_year)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            item.region,
            item.subregion,
            item.country,
            item.city,
            item.pm10,
            item.pm10_year.year(),
            item.pm25,
            item.pm25_year.year()
        ],
    )?;

    Ok(())
}

pub fn load_from_csv(conn: &Connection) -> Result<()> {
    let csv_path = Path::new("res/9.world_pm25_pm10.csv");
    let file = fs::File::open(csv_path).unwrap();
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .double_quote(false)
        .from_reader(file);
    for result in rdr.records() {
        let record = result.unwrap();
        //println!("{:?}", record);//for debug
        let item = Item::new(
            record.get(0).ok_or("Missing region").unwrap(),
            match record.get(1) {
                Some(mes) => mes,
                None => {
                    println!("问题数据：{:?}", record);
                    "寄"
                }
            },
            match record.get(2) {
                Some(mes) => mes,
                None => {
                    println!("问题数据：{:?}", record);
                    "寄"
                }
            },
            record.get(3).ok_or("Missing city").unwrap(),
            match record.get(4) {
                Some(mes) => match mes.parse::<f64>() {
                    Ok(num) => num,
                    Err(e) => {
                        println!("问题数据：{:?}；特别出在数字转换上", record);
                        println!("{:?}", e);
                        0.0
                    }
                },
                None => {
                    println!("问题数据：{:?}", record);
                    0.0
                }
            },
            Date::from_ordinal_date(record.get(5).unwrap().parse().unwrap(), 1).unwrap(),
            record
                .get(6)
                .ok_or("Missing PM2.5")
                .unwrap()
                .parse::<f64>()
                .unwrap(),
            match record.get(7) {
                Some(mes) => Date::from_ordinal_date(mes.parse().unwrap(), 1).unwrap(),
                None => {
                    println!("问题数据：{:?}；缺省pm10year，使用pm2.5 year填充", record);
                    Date::from_ordinal_date(record.get(5).unwrap().parse().unwrap(), 1).unwrap()
                }
            },
        );
        insert_item(conn, &item).unwrap();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db_option::{init_database, insert_item, load_from_csv, Item};
    use rusqlite::Connection;
    use time::Date;

    #[test]
    fn test_db_init() {
        init_database().unwrap();

        let conn = Connection::open("air_quality.db").expect("Could not open database");

        let sample_item = Item::new(
            "Sample Region",
            "Sample Subregion",
            "Sample Country",
            "Sample City",
            25.5,
            Date::from_ordinal_date(2023, 1).unwrap(),
            15.3,
            Date::from_ordinal_date(2023, 1).unwrap(),
        );

        match insert_item(&conn, &sample_item) {
            Ok(()) => println!("Item inserted successfully"),
            Err(err) => eprintln!("Error inserting item: {}", err),
        }
    }

    #[test]
    fn test_load_csv() {
        init_database().unwrap();
        let conn = Connection::open("air_quality.db").expect("Could not open database");
        load_from_csv(&conn).unwrap();
    }
}
