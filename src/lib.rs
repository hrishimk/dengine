pub extern crate mysql;

extern crate serde;

extern crate chrono;
extern crate chrono_tz;

use mysql as my;

mod traits;
mod types;

pub use traits::*;
pub use types::*;

pub type Desult<T> = Result<T, String>;

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

#[derive(Debug)]
pub struct Affected {
    pub affected_rows: u64,
    pub last_insert_id: u64,
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

    pub fn value<T, R>(&self, sql: &str, colum: &str, params: R) -> Result<T, String>
    where
        T: mysql::prelude::FromValue,
        R: std::clone::Clone,
        mysql::Params: std::convert::From<R>,
    {
        let res: Option<mysql::Row> = self.pool.first_exec(sql, &params).map_err(|e| {
            println!("Database error: {:?}", e);
            "Failed"
        })?;

        let res = match res {
            Some(x) => Ok::<T, String>(x.get::<T, &str>(colum).unwrap()),
            None => return Err("Failed to run query".to_string()),
        };

        match res {
            Ok(x) => Ok(x),
            Err(_) => Err("Failed to unwrap value".to_string()),
        }
    }

    pub fn row<T, R>(&self, sql: &str, params: R) -> Result<T, String>
    where
        T: Queryable,
        R: std::clone::Clone,
        mysql::Params: std::convert::From<R>,
    {
        let res: Option<mysql::Row> = self.pool.first_exec(sql, &params).map_err(|e| {
            println!("Database error: {:?}", e);
            "Failed"
        })?;

        let res = match res {
            Some(x) => Ok::<T, String>(T::new(Row(x))),
            None => return Err("Failed to run query".to_string()),
        };

        match res {
            Ok(x) => Ok(x),
            Err(_) => Err("Failed to unwrap value".to_string()),
        }
    }

    pub fn array<T: Queryable + std::fmt::Debug, P: std::clone::Clone>(
        &self,
        sql: &str,
        params: P,
        calc_found_rows: bool,
    ) -> Result<Vec<T>, &str>
    where
        mysql::Params: std::convert::From<P>,
    {
        self.select(sql, params, calc_found_rows).map(|r| r.data)
    }

    pub fn select<T: Queryable + std::fmt::Debug, P: std::clone::Clone>(
        &self,
        sql: &str,
        params: P,
        calc_found_rows: bool,
    ) -> std::result::Result<SelectHolder<T>, &str>
    where
        mysql::Params: std::convert::From<P>,
    {
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

    pub fn insert_update<T: Insertable>(
        &self,
        table: &str,
        fields: Vec<T>,
    ) -> Result<Affected, String> {
        let mut c_arr = Vec::new();
        let mut q_arr = Vec::new();
        let mut a_arr = Vec::new();
        let data_fields = T::fields();
        let mut q_str: String = "".to_string();
        for (i, n) in fields.iter().enumerate() {
            q_arr.push(Vec::new());
            let values = n.values();
            for (j, m) in data_fields.iter().enumerate() {
                if i == 0 {
                    c_arr.push(m.to_string());
                }
                q_arr[i].push("?");
                a_arr.push(values[j].to_string());
            }
            if i != 0 {
                q_str.push(',');
            }
            q_str.push_str(format!("({})", q_arr[i].join(&",")).as_str());
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {} ON DUPLICATE KEY UPDATE {}",
            table,
            c_arr.join(&","),
            q_str,
            Self::gen_dupdate(c_arr)
        );

        println!("sql is {}", sql);
        let res: Affected = self
            .pool
            .prep_exec(sql, &a_arr)
            .map(|result| Affected {
                affected_rows: result.affected_rows(),
                last_insert_id: result.last_insert_id(),
            })
            .map_err(|e| {
                println!("Database error: {:?}", e);
                "Failed"
            })?;

        Ok(res)
    }

    fn gen_dupdate(colums: Vec<String>) -> String {
        let mut rt = Vec::new();
        for n in colums {
            rt.push(format!("{} = VALUES({}) ", n, n));
        }
        rt.join(&",")
    }

    fn delete_ids<T>(
        &self,
        table: &str,
        id_colum: &str,
        id_values: Vec<T>,
        in_out: &str,
    ) -> Result<Affected, String>
    where
        T: std::clone::Clone,
        mysql::Value: std::convert::From<T>,
    {
        let mut c_arr: Vec<char> =
            Vec::with_capacity(std::mem::size_of::<char>() * id_values.len());

        for _ in &id_values {
            c_arr.push('?');
        }

        let sql = format!(
            "delete from {} where {} {} ({})",
            table,
            id_colum,
            in_out,
            c_arr
                .iter()
                .enumerate()
                .map(|e| {
                    if e.0 != 0 {
                        format!(",{}", e.1)
                    } else {
                        e.1.to_string()
                    }
                })
                .collect::<String>()
        );

        println!("sql is {}", sql);
        let res: Affected = self
            .pool
            .prep_exec(sql, &id_values)
            .map(|result| Affected {
                affected_rows: result.affected_rows(),
                last_insert_id: result.last_insert_id(),
            })
            .map_err(|e| {
                println!("Database error: {:?}", e);
                "Failed"
            })?;

        Ok(res)
    }

    pub fn delete_wid<T>(
        &self,
        table: &str,
        id_colum: &str,
        id_values: Vec<T>,
    ) -> Result<Affected, String>
    where
        T: std::clone::Clone,
        mysql::Value: std::convert::From<T>,
    {
        self.delete_ids::<T>(table, id_colum, id_values, "IN")
    }

    pub fn delete_nwid<T>(
        &self,
        table: &str,
        id_colum: &str,
        id_values: Vec<T>,
    ) -> Result<Affected, String>
    where
        T: std::clone::Clone,
        mysql::Value: std::convert::From<T>,
    {
        self.delete_ids::<T>(table, id_colum, id_values, "NOT IN")
    }

    pub fn concat_colums(colums: Vec<&str>) -> String {
        let s = colums.join(",");
        format!("concat_ws(' ', {})", s)
    }
}
