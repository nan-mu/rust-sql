mod db_option;
use crate::db_option::{init_database, load_from_csv, Build, Item, PmAndYear};
use rocket::{get, main, routes, Error};
use rusqlite::Connection;
use time::Date;

#[get("/query?<id>&<region>&<subregion>&<country>&<city>")]
fn query(
    id: Option<i64>,
    region: Option<&str>,
    subregion: Option<&str>,
    country: Option<&str>,
    city: Option<&str>,
) -> String {
    let conn = Connection::open("air_quality.db").expect("Could not open database");
    let mut query = "select * from air_quality where 1 = 1".to_string();

    if let Some(id_) = id {
        query.push_str(&(format!(" and id = {}", id_)));
    }
    if let Some(region_) = region {
        query.push_str(&(format!(" and region = {}", region_)));
    }
    if let Some(subregion_) = subregion {
        query.push_str(&(format!(" and subregion = {}", subregion_)));
    }
    if let Some(country_) = country {
        query.push_str(&(format!(" and country = {}", country_)));
    }
    if let Some(city_) = city {
        query.push_str(&(format!(" and city = {}", city_)));
    }
    query.push_str(";");

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
    result
}

#[main]
async fn main() -> Result<(), Error> {
    //初始化数据库
    init_database().unwrap();
    load_from_csv().unwrap();

    //启动服务器
    let _rocket = rocket::build().mount("/", routes![query]).launch().await?;

    Ok(())
}

#[cfg(test)]
mod tests;
