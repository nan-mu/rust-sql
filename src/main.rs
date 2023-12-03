mod db_option;
use crate::db_option::{init_database, insert_item, Item};
use rusqlite::Connection;
use time::Date;

fn main() {
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
