use mysql;
use std;
extern crate chrono;

pub struct Row(pub mysql::Row);

impl Row {
    pub fn get<T>(&self, key: &str) -> Option<T>
    where
        T: mysql::prelude::FromValue + std::fmt::Debug,
    {
        self.0.get::<T, &str>(key)
    }

    pub fn get_date_time(&self, key: &str) -> Option<chrono::DateTime<chrono::offset::Local>> {
        let a: mysql::Value = self.0.get(key).unwrap();
        let b: (i32, u32, u32, u32, u32, u32, u32) = match a {
            mysql::Value::Date(a, b, c, d, e, f, g) => (
                a as i32, b as u32, c as u32, d as u32, e as u32, f as u32, g as u32,
            ),
            _ => return None,
        };
        let date = chrono::NaiveDate::from_ymd(b.0, b.1, b.2);
        let time = chrono::NaiveTime::from_hms_milli(b.3, b.4, b.5, b.6);

        let date_time = chrono::NaiveDateTime::new(date, time);

        let offset = chrono::offset::FixedOffset::east(0);

        Some(chrono::DateTime::<chrono::offset::Local>::from_utc(
            date_time, offset,
        ))
    }

    pub fn get_date_string(&self, key: &str, format: &str) -> Option<String> {
        let a: mysql::Value = self.0.get(key).unwrap();
        let b: (i32, u32, u32, u32, u32, u32, u32) = match a {
            mysql::Value::Date(a, b, c, d, e, f, g) => (
                a as i32, b as u32, c as u32, d as u32, e as u32, f as u32, g as u32,
            ),
            _ => return None,
        };

        if b.1 <= 0 || b.2 <= 0 {
            return None;
        }

        let date = chrono::NaiveDate::from_ymd(b.0, b.1, b.2);

        let a = date.format(format);

        Some(format!("{}", a))
    }
}

pub trait Queryable {
    fn new(row: Row) -> Self;
}

///Need to implement for structs to be inserted
///
pub trait Insertable {
    ///List of fields of struct as &str
    fn fields() -> Vec<String>;

    ///List of values of struct as &str
    fn values(&self) -> Vec<String>;
}
