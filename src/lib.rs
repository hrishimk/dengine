#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
//#[macro_use]
extern crate mysql;

extern crate serde;

extern crate chrono;
extern crate chrono_tz;

use mysql as my;

pub struct Row(pub my::Row);

#[derive(Debug)]
pub struct Rnd2Ir(f64);

#[derive(Debug)]
pub struct Rnd2(f64);

fn round2(a: f64) -> f64 {
    (a * 100_f64).round() / 100_f64
}

impl mysql::prelude::ConvIr<Rnd2> for Rnd2Ir {
    fn new(v: mysql::Value) -> Result<Self, mysql::FromValueError> {
        match v {
            mysql::Value::Float(fl_val) => Ok(Rnd2Ir(round2(fl_val))),
            v => Err(mysql::FromValueError(v)),
        }
    }
    fn commit(self) -> Rnd2 {
        Rnd2(self.0)
    }
    fn rollback(self) -> mysql::Value {
        mysql::Value::Float(self.0)
    }
}

impl mysql::prelude::FromValue for Rnd2 {
    type Intermediate = Rnd2Ir;
}

impl serde::Serialize for Rnd2 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl Row {
    pub fn get<T>(&self, key: &str) -> Option<T>
    where
        T: mysql::prelude::FromValue + std::fmt::Debug,
    {
        println!("f64 val is {:?}", self.0.get::<T, &str>(key));
        self.0.get::<T, &str>(key)
    }

    pub fn get_date_time(&self, key: &str) -> Option<chrono::DateTime<chrono::offset::Local>> {
        let a: my::Value = self.0.get(key).unwrap();
        let b: (i32, u32, u32, u32, u32, u32, u32) = match a {
            my::Value::Date(a, b, c, d, e, f, g) => (
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
        let a: my::Value = self.0.get(key).unwrap();
        let b: (i32, u32, u32, u32, u32, u32, u32) = match a {
            my::Value::Date(a, b, c, d, e, f, g) => (
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

#[derive(Debug)]
pub struct DbEngine {
    host: String,
    user_name: String,
    password: String,
    db_name: String,
    pool: my::Pool,
}

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

impl DbEngine {
    pub fn new(host: String, user_name: String, password: String, db_name: String) -> Self {
        let con_str = format!(
            "mysql://{}:{}@{}:{}/{}",
            user_name, password, "localhost", "3306", db_name
        );

        let pool = my::Pool::new(con_str).unwrap();
        DbEngine {
            host,
            user_name,
            password,
            db_name,
            pool,
        }
    }

    pub fn select<T: Queryable + std::fmt::Debug>(
        &self,
        sql: &str,
        params: Vec<String>,
        calc_found_rows: bool,
    ) -> std::result::Result<SelectHolder<T>, &str> {
        let res: Vec<T> = self
            .pool
            .prep_exec(sql, &params)
            .map(|result| {
                result
                    .map(|x| x.unwrap())
                    .map(|row| T::new(Row(row)))
                    .collect()
            })
            .map_err(|e| {
                println!("Database error: {:?}", e);
                "Failed"
            })?;

        let res_len = res.len();

        if !calc_found_rows {
            Ok(SelectHolder {
                data: res,
                count: res_len,
            })
        } else {
            let start_index = match sql.find(" from ") {
                Some(x) => x,
                None => return Err("No from in sql"),
            };

            let end_index = match sql.to_lowercase().find(" limit ") {
                Some(x) => x,
                None => return Err("No limit in sql"),
            };

            let new_sql = "select count(*) as count ".to_string();

            let new_sql = new_sql + &sql[start_index..end_index];

            let count: Vec<usize> = self
                .pool
                .prep_exec(new_sql, &params)
                .map(|result| {
                    result
                        .map(|x| x.unwrap())
                        .map(|row| row.get("count").unwrap())
                        .collect()
                })
                .map_err(|e| "Count error")?;

            match count.get(0) {
                Some(x) => {
                    println!("inside get ",);
                    return Ok(SelectHolder {
                        data: res,
                        count: *x,
                    });
                }
                None => Err("Count get error"),
            }
        }
    }

    pub fn concat_colums(colums: Vec<&str>) -> String {
        let s = colums.join(",");
        format!("concat_ws(' ', {})", s)
    }
}
