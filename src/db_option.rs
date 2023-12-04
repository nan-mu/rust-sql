#[warn(dead_code)]
use csv;
use rusqlite::{types, Connection, Result};
use std::path::Path;
use std::{fmt, fs};
use time::Date;

pub enum PmAndYear {
    Pm25(f64, Date),
    Pm10(f64, Date),
    None,
}

impl fmt::Display for PmAndYear {
    //服了，数据是大坑，有的数据缺少，需要写trait了
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PmAndYear::None => write!(f, "缺省数据"),
            PmAndYear::Pm25(pm, year) => write!(f, "pm2.5: {}, year: {}", pm, year.year()),
            PmAndYear::Pm10(pm, year) => write!(f, "pm10 : {}, year: {}", pm, year.year()),
        }
    }
}

impl fmt::Display for Item {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<region {}> <subregion {}> <country {}> <city > {} | {} {}",
            self.region, self.subregion, self.country, self.city, self.pm10, self.pm25,
        )
    }
}

pub struct Item {
    region: String,
    subregion: String,
    country: String,
    city: String,
    pm10: PmAndYear,
    pm25: PmAndYear,
}

pub trait Build {
    fn new(
        region: &str,
        subregion: &str,
        country: &str,
        city: &str,
        pm10: PmAndYear,
        pm25: PmAndYear,
    ) -> Self;
}

impl Build for Item {
    fn new(
        region: &str,
        subregion: &str,
        country: &str,
        city: &str,
        pm10: PmAndYear,
        pm25: PmAndYear,
    ) -> Item {
        Item {
            region: region.to_string(),
            subregion: subregion.to_string(),
            country: country.to_string(),
            city: city.to_string(),
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
    let conn = Connection::open(db_path).unwrap();
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
    )
    .unwrap();
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
                PmAndYear::Pm10(pm, _) => Option::Some(pm),
                _ => Option::None,
            } as &dyn types::ToSql,
            &match item.pm10 {
                PmAndYear::Pm10(_, year) => Option::Some(year.year()),
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

pub fn load_from_csv() -> Result<()> {
    let conn = Connection::open("air_quality.db").expect("Could not open database");
    let csv_path = Path::new("res/9.world_pm25_pm10.csv");
    let file = fs::File::open(csv_path).unwrap();
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .double_quote(false)
        .from_reader(file);
    for result in rdr.records() {
        let record = result.unwrap();
        println!("{:?}", record); //for debug
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
            match record.get(4).unwrap().parse() {
                Ok(num) => PmAndYear::Pm10(
                    num,
                    Date::from_ordinal_date(
                        match record.get(5).unwrap().parse() {
                            Ok(year) => year,
                            Err(_) => record.get(7).unwrap().parse().unwrap(),
                        },
                        1,
                    )
                    .unwrap(),
                ),
                Err(e) => {
                    println!("问题数据：{:?}；可能缺少pm2.5", record);
                    println!("{:?}", e);
                    PmAndYear::None
                }
            },
            match record.get(6).unwrap().parse() {
                Ok(num) => PmAndYear::Pm25(
                    num,
                    Date::from_ordinal_date(
                        match record.get(7).unwrap().parse() {
                            Ok(year) => year,
                            Err(_) => record.get(5).unwrap().parse().unwrap(),
                        },
                        1,
                    )
                    .unwrap(),
                ),
                Err(e) => {
                    println!("问题数据：{:?}；可能缺少pm2.5", record);
                    println!("{:?}", e);
                    PmAndYear::None
                }
            },
        );
        insert_item(&conn, &item).unwrap();
    }

    Ok(())
}
