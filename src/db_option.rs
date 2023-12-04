use csv;
use rusqlite::{types, Connection, Result};
use std::path::Path;
use std::{fmt, fs};
use time::Date;

#[derive(Debug)]
pub enum PmAndYear {
    Pm25(f64, Date),
    Pm10(f64, Date),
    None,
}

impl fmt::Display for PmAndYear {
    //服了，数据是大坑，有的数据缺少，需要写trait了
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PmAndYear::None => write!(f, "寄，缺省数据，错了"),
            PmAndYear::Pm25(pm, year) => write!(f, "pm2.5: {}, year: {}", pm, year.year()),
            PmAndYear::Pm10(pm, year) => write!(f, "pm10 : {}, year: {}", pm, year.year()),
        }
    }
}

#[derive(Debug)]
pub struct Item<'a> {
    region: &'a str,
    subregion: &'a str,
    country: &'a str,
    city: &'a str,
    pm10: PmAndYear,
    pm25: PmAndYear,
}

pub trait Build<'a> {
    fn new(
        region: &'a str,
        subregion: &'a str,
        country: &'a str,
        city: &'a str,
        pm10: PmAndYear,
        pm25: PmAndYear,
    ) -> Self;
}

impl<'a> Build<'a> for Item<'a> {
    fn new<'b>(
        region: &'b str,
        subregion: &'b str,
        country: &'b str,
        city: &'b str,
        pm10: PmAndYear,
        pm25: PmAndYear,
    ) -> Item<'b> {
        Item {
            region,
            subregion,
            country,
            city,
            pm10,
            pm25,
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
             pm10 REAL,
             pm10_year INTEGER,
             pm25 REAL,
             pm25_year INTEGER 
         )",
        [],
    )?;

    Ok(())
}

pub fn insert_item(conn: &Connection, item: &Item) -> Result<()> {
    conn.execute(
        "INSERT INTO air_quality (region, subregion, country, city, pm10, pm10_year, pm25, pm25_year)
         VALUES (:region, :subregion, :country, :city, :pm10, :pm10_year, :pm25, :pm25_year)",
         &[
            &item.region.to_string() as &dyn types::ToSql,
            &item.subregion.to_string() as &dyn types::ToSql,
            &item.country.to_string() as &dyn types::ToSql,
            &item.city.to_string() as &dyn types::ToSql,
            &match item.pm10 {
                PmAndYear::Pm25(pm, _) => Option::Some(pm),
                _ => Option::None,
            } as &dyn types::ToSql,
            &match item.pm10 {
                PmAndYear::Pm25(_, year) => Option::Some(year.year()),
                _ => Option::None,
            } as &dyn types::ToSql,
            &match item.pm25 {
                PmAndYear::Pm25(pm, _) => Option::Some(pm),
                _ => Option::None,
            } as &dyn types::ToSql,
            &match item.pm25 {
                PmAndYear::Pm25(_, year) => Option::Some(year.year()),
                _ => Option::None,
            } as &dyn types::ToSql,
        ]
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
                    Ok(num) => PmAndYear::Pm10(
                        num,
                        Date::from_ordinal_date(record.get(5).unwrap().parse().unwrap(), 1)
                            .unwrap(),
                    ),
                    Err(e) => {
                        println!("问题数据：{:?}；特别出在数字转换上", record);
                        println!("{:?}", e);
                        PmAndYear::None
                    }
                },
                None => {
                    println!("问题数据：{:?}", record);
                    PmAndYear::None
                }
            },
            match record.get(6) {
                Some(mes) => match mes.parse::<f64>() {
                    Ok(num) => PmAndYear::Pm10(
                        num,
                        Date::from_ordinal_date(record.get(7).unwrap().parse().unwrap(), 1)
                            .unwrap(),
                    ),
                    Err(e) => {
                        println!("问题数据：{:?}；特别出在数字转换上", record);
                        println!("{:?}", e);
                        PmAndYear::None
                    }
                },
                None => {
                    println!("问题数据：{:?}", record);
                    PmAndYear::None
                }
            },
        );
        insert_item(conn, &item).unwrap();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db_option::{init_database, insert_item, load_from_csv, Build, Item};
    use rusqlite::Connection;
    use time::Date;

    use super::PmAndYear;

    #[test]
    fn test_db_init() {
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
        }
    }

    #[test]
    fn test_load_csv() {
        init_database().unwrap();
        let conn = Connection::open("air_quality.db").expect("Could not open database");
        load_from_csv(&conn).unwrap();
    }
}
