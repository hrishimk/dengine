use super::{Affected, Desult, Dypes, Params, SelectHolder};
use std;
extern crate chrono;

pub struct Row<'a>(&'a Rowable);

impl<'a> Row<'a> {
    pub fn new<T>(row: &'a T) -> Self
    where
        T: Rowable,
    {
        Row(row)
    }

    pub fn get<T>(&self, key: &str) -> Option<T>
    where
        Option<T>: std::convert::From<Dypes>,
    {
        if key == "user_profile_id" {
            println!("get user_profile_id {:?}", self.0.get_val(key));
        }

        match self.0.get_val(key) {
            Some(x) => <Option<T>>::from(x),
            None => None,
        }
    }

    pub fn get_date_string(&self, key: &str, format: &str) -> Desult<String> {
        self.0.get_date_string(key, format)
    }

    /*
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
    */
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

pub trait Rowable {
    ///Get value from a row
    fn get_val(&self, key: &str) -> Option<Dypes>;

    /// Get date string for row with column name -> key
    /// Format uses chrono format str
    fn get_date_string(&self, key: &str, format: &str) -> Desult<String>;
}

pub trait Connectionable {
    /// Executes a query with params
    fn execute<P>(&self, sql: &str, params: P) -> Desult<()>
    where
        P: std::clone::Clone,
        Params: std::convert::From<P>;

    /// Select sql query
    /// params- Individual value or Vec or Tuple
    /// calc_found_rows- If true count parameter in return structure is set to total number of calculated rows. Else return the number of rows returned
    fn select<T: Queryable + std::fmt::Debug, P: std::clone::Clone>(
        &self,
        sql: &str,
        params: P,
        calc_found_rows: bool,
    ) -> Desult<SelectHolder<T>>
    where
        Params: std::convert::From<P>;

    /// Returns scalar value
    /// colum: column name
    /// params: Params as in select
    fn value<T, R>(&self, sql: &str, colum: &str, params: R) -> Desult<T>
    where
        T: std::convert::From<Dypes>,
        R: std::clone::Clone,
        Params: std::convert::From<R>;

    /// Return a single row
    /// params: Same as in select
    fn row<T, R>(&self, sql: &str, params: R) -> Desult<T>
    where
        T: Queryable,
        R: std::clone::Clone,
        Params: std::convert::From<R>;

    /// Return vector of rows instead of struct with meta data
    fn array<T: Queryable + std::fmt::Debug, P: std::clone::Clone>(
        &self,
        sql: &str,
        params: P,
        calc_found_rows: bool,
    ) -> Desult<Vec<T>>
    where
        Params: std::convert::From<P>,
    {
        self.select(sql, params, calc_found_rows).map(|r| r.data)
    }

    /// Not tested
    fn insert_update<T: Insertable>(&self, table: &str, fields: Vec<T>) -> Desult<Affected>;

    fn gen_dupdate(colums: Vec<String>) -> String {
        let mut rt = Vec::new();
        for n in colums {
            rt.push(format!("{} = VALUES({}) ", n, n));
        }
        rt.join(&",")
    }

    /// Delete rows
    fn delete_ids<T>(
        &self,
        table: &str,
        id_colum: &str,
        id_values: Vec<T>,
        in_out: &str,
    ) -> Desult<Affected>
    where
        T: std::clone::Clone,
        Dypes: std::convert::From<T>;

    fn delete_wid<T>(&self, table: &str, id_colum: &str, id_values: Vec<T>) -> Desult<Affected>
    where
        T: std::clone::Clone,
        Dypes: std::convert::From<T>;

    fn delete_nwid<T>(&self, table: &str, id_colum: &str, id_values: Vec<T>) -> Desult<Affected>
    where
        T: std::clone::Clone,
        Dypes: std::convert::From<T>;

    fn concat_colums(colums: Vec<&str>) -> String;
}
