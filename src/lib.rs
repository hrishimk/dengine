//! Aims to create a common interface to use mysql and sqlite
//! databases.

extern crate deslite;
extern crate mysql;

extern crate serde;

extern crate chrono;
extern crate chrono_tz;

pub mod my_sql;
pub mod sqlite;
mod traits;
mod types;

pub use traits::*;
pub use types::*;

/// Result type
/// Err defaults to Error
pub type Desult<T> = Result<T, Error>;

#[derive(Debug)]
pub struct Affected {
    pub affected_rows: u64,
    pub last_insert_id: u64,
}

impl Affected {
    pub fn new(affected_rows: u64, last_insert_id: u64) -> Self {
        Self {
            affected_rows,
            last_insert_id,
        }
    }
}

#[derive(Debug)]
pub struct DbEngine;

impl DbEngine {
    pub fn new_mysql(
        host: String,
        user_name: String,
        password: String,
        db_name: String,
    ) -> my_sql::Connection {
        my_sql::Connection::new(host, user_name, password, db_name)
    }

    pub fn new_sqlite(db_name: &str) -> sqlite::Connection {
        sqlite::Connection::new(db_name).unwrap()
    }
}

/// Struct returned when the select method is used.
#[derive(Debug, PartialEq, Eq)]
pub struct SelectHolder<T> {
    pub data: Vec<T>,
    pub count: usize,
}

impl<T> SelectHolder<T> {
    pub fn new(data: Vec<T>, count: usize) -> Self {
        Self { data, count }
    }
}

/// Error type for the lib
#[derive(Debug)]
pub enum Error {
    SQLErr(String),
    IndexOutOfBound(String),
    ConversionErr(String),
    LibErr(String),
    Unknown(String),
    ConnectionErr(String),
}

impl Error {
    pub fn date_conv_err(key: &str) -> Self {
        Error::ConversionErr(format!("Failed to convert {} to date string", key))
    }
}
