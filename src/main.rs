mod db_option;

use crate::db_option::{initialize_database, insert_item, Item};
use rusqlite::Connection;

fn main() {
    initialize_database().unwrap();

    let conn = Connection::open("air_quality.db").expect("Could not open database");

    let sample_item = Item::new(
        "Sample Region",
        "Sample Subregion",
        "Sample Country",
        "Sample City",
        25.5,
        chrono::Utc::now().naive_utc(),
        15.3,
        chrono::Utc::now().naive_utc(),
    );

    match insert_item(&conn, &sample_item) {
        Ok(()) => println!("Item inserted successfully"),
        Err(err) => eprintln!("Error inserting item: {}", err),
    }
}
