use std::fs;

use chrono::NaiveDateTime;
use csv::ReaderBuilder;
use std::error::Error;

use rusqlite::{params, Connection, Result};

const DB_PATH: &str = "air_quality.db";
const CSV_PATH: &str = "res/9.world_pm25_pm10.csv";

#[derive(Debug)]
pub struct Item {
    region: String,
    subregion: String,
    country: String,
    city: String,
    pm10: f64,
    pm10_year: NaiveDateTime,
    pm25: f64,
    pm25_year: NaiveDateTime,
}

impl Item {
    pub fn new(
        region: &str,
        subregion: &str,
        country: &str,
        city: &str,
        pm10: f64,
        pm10_year: NaiveDateTime,
        pm25: f64,
        pm25_year: NaiveDateTime,
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

pub fn initialize_database() -> Result<()> {
    //假如数据库文件存在，则删除。
    if !fs::metadata(DB_PATH).is_ok() {
        fs::remove_file(DB_PATH).unwrap();
    }
    let conn = Connection::open(DB_PATH)?;
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
            item.pm10_year.timestamp(),
            item.pm25,
            item.pm25_year.timestamp()
        ],
    )?;

    Ok(())
}

pub fn load() -> Result<Vec<Item>, Box<dyn Error>> {
    let file = fs::File::open(CSV_PATH)?;
    let mut rdr = ReaderBuilder::new().delimiter(b'\t').from_reader(file);

    let mut items = Vec::new();

    for result in rdr.records() {
        let record = result;
        // let item = Item::new(
        //     &record.get(0).ok_or("Missing region")?.to_string(),
        //     &record.get(1).ok_or("Missing subregion")?.to_string(),
        //     &record.get(2).ok_or("Missing country")?.to_string(),
        //     &record.get(3).ok_or("Missing city")?.to_string(),
        //     &record.get(4).ok_or("Missing PM10")?.parse()?,
        //     NaiveDateTime::parse_from_str(
        //         &format!(
        //             "{}-01-01 00:00:00",
        //             &record.get(5).ok_or("Missing PM10 Year")?
        //         ),
        //         "%Y-%m-%d %H:%M:%S",
        //     )?,
        //     &record.get(6).ok_or("Missing PM2.5")?.parse()?,
        //     NaiveDateTime::parse_from_str(
        //         &format!(
        //             "{}-01-01 00:00:00",
        //             &record.get(7).ok_or("Missing PM2.5 Year")?
        //         ),
        //         "%Y-%m-%d %H:%M:%S",
        //     )?,
        // )?;
        items.push(item);
    }

    Ok(items)
}