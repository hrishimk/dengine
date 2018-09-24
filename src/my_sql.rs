use super::mysql;
use super::mysql::prelude::{ConvIr, FromValue};
use super::{
    chrono, round2, Affected, Connectionable, Desult, Dypes, Error, Insertable, Params, Queryable,
    Rnd2, Row, Rowable, SelectHolder,
};
use mysql::Value;
use std;
use std::collections::HashMap;

impl From<mysql::Error> for Error {
    fn from(val: mysql::Error) -> Self {
        match val {
            mysql::Error::IoError(x) => Error::LibErr(x.to_string()),
            mysql::Error::MySqlError(x) => Error::SQLErr(x.to_string()),
            mysql::Error::DriverError(x) => Error::LibErr(x.to_string()),
            mysql::Error::UrlError(x) => Error::LibErr(x.to_string()),
            mysql::Error::FromValueError(x) => {
                Error::ConversionErr(format!("From mysql Value: {:?}", x))
            }
            mysql::Error::FromRowError(x) => {
                Error::ConversionErr(format!("From mysql row: {:?}", x))
            }
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    host: String,
    user_name: String,
    password: String,
    db_name: String,
    con: mysql::Pool,
}

impl Connection {
    pub fn new(host: String, user_name: String, password: String, db_name: String) -> Self {
        let con_str = format!(
            "mysql://{}:{}@{}:{}/{}",
            user_name, password, "localhost", "3306", db_name
        );

        let con = mysql::Pool::new(con_str).unwrap();
        Connection {
            host,
            user_name,
            password,
            db_name,
            con,
        }
    }
}

impl Connectionable for Connection {
    fn execute<P>(&self, sql: &str, params: P) -> Desult<()>
    where
        P: std::clone::Clone,
        Params: std::convert::From<P>,
    {
        let params = Params::from(params);
        let params = mysql::Params::from(params);
        self.con.prep_exec(sql, &params).map_err(|e| {
            println!("Database error: {:?}", e);
            Error::from(e)
        })?;
        Ok(())
    }

    fn value<T, R>(&self, sql: &str, colum: &str, params: R) -> Desult<T>
    where
        T: std::convert::From<Dypes>,
        R: std::clone::Clone,
        Params: std::convert::From<R>,
    {
        let params = Params::from(params);
        let params = mysql::Params::from(params);
        let res: Option<mysql::Row> = self.con.first_exec(sql, &params).map_err(|e| {
            println!("Database error: {:?}", e);
            Error::from(e)
        })?;

        let res = match res {
            Some(x) => Ok::<Dypes, String>(x.get::<Dypes, &str>(colum).unwrap()),
            None => {
                return Err(Error::SQLErr(
                    "Failed to get result out of query".to_string(),
                ))
            }
        };

        match res {
            Ok(x) => Ok(T::from(x)),
            Err(_) => Err(Error::Unknown("Failed to unwrap value".to_string())),
        }
    }

    fn row<T, R>(&self, sql: &str, params: R) -> Desult<T>
    where
        T: Queryable,
        R: std::clone::Clone,
        Params: std::convert::From<R>,
    {
        let params = Params::from(params);
        let params = mysql::Params::from(params);
        let res: Option<mysql::Row> = self.con.first_exec(sql, &params).map_err(|e| {
            println!("Database error: {:?}", e);
            Error::from(e)
        })?;

        let res = match res {
            Some(x) => Ok::<T, String>(T::new(Row::new(&x))),
            None => {
                return Err(Error::SQLErr(
                    "Failed to get result out of query".to_string(),
                ))
            }
        };

        match res {
            Ok(x) => Ok(x),
            Err(_) => Err(Error::Unknown("Failed to unwrap value".to_string())),
        }
    }

    fn select<T: Queryable + std::fmt::Debug, P: std::clone::Clone>(
        &self,
        sql: &str,
        params: P,
        calc_found_rows: bool,
    ) -> Desult<SelectHolder<T>>
    where
        Params: std::convert::From<P>,
    {
        let params = Params::from(params);
        let params = mysql::Params::from(params);
        let res: Vec<T> = self
            .con
            .prep_exec(sql, &params)
            .map(|result| {
                result
                    .map(|x| x.unwrap())
                    .map(|row| T::new(Row::new(&row)))
                    .collect()
            }).map_err(|e| {
                println!("Database error: {:?}", e);
                Error::from(e)
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
                None => return Err(Error::SQLErr("No from in sql".to_string())),
            };

            let end_index = match sql.to_lowercase().find(" limit ") {
                Some(x) => x,
                None => return Err(Error::SQLErr("No limit in sql".to_string())),
            };

            let new_sql = "select count(*) as count ".to_string();

            let new_sql = new_sql + &sql[start_index..end_index];

            let count: Vec<usize> = self
                .con
                .prep_exec(new_sql, &params)
                .map(|result| {
                    result
                        .map(|x| x.unwrap())
                        .map(|row| row.get("count").unwrap())
                        .collect()
                }).map_err(|e| Error::from(e))?;

            match count.get(0) {
                Some(x) => {
                    println!("inside get ",);
                    return Ok(SelectHolder {
                        data: res,
                        count: *x,
                    });
                }
                None => Err(Error::LibErr("Count get error".to_string())),
            }
        }
    }

    fn insert_update<T: Insertable>(&self, table: &str, fields: Vec<T>) -> Desult<Affected> {
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
                a_arr.push(values[j].clone());
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
            .con
            .prep_exec(sql, &a_arr)
            .map(|result| Affected {
                affected_rows: result.affected_rows(),
                last_insert_id: result.last_insert_id(),
            }).map_err(|e| {
                println!("Database error: {:?}", e);
                Error::from(e)
            })?;

        Ok(res)
    }

    fn insert<T: Insertable>(&self, table: &str, fields: Vec<T>) -> Desult<Affected> {
        let colum_names: Vec<String> = T::fields();
        let values: Vec<Dypes> = fields.iter().fold(Vec::new(), |mut acc, x| {
            acc.append(&mut x.values());
            acc
        });

        let q_arr: Vec<String> = fields.iter().fold(Vec::new(), |mut acc, _x| {
            let single: Vec<&str> = std::iter::repeat("?").take(colum_names.len()).collect();
            acc.push(format!("({})", single.join(",")));
            acc
        });

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            colum_names.join(","),
            q_arr.join(",")
        );

        let res: Affected = self
            .con
            .prep_exec(sql, &values)
            .map(|result| Affected {
                affected_rows: result.affected_rows(),
                last_insert_id: result.last_insert_id(),
            }).map_err(|e| {
                println!("Database error: {:?}", e);
                Error::from(e)
            })?;

        Ok(res)
    }

    fn update<T: Insertable>(
        &self,
        table: &str,
        fields: Vec<T>,
        where_fields: HashMap<&str, Dypes>,
    ) -> Desult<Affected> {
        let mut values: Vec<Dypes> = fields.iter().fold(Vec::new(), |mut acc, x| {
            acc.append(&mut x.values());
            acc
        });

        let colum_names: Vec<String> = T::fields();

        let vars: Vec<String> = colum_names.iter().map(|x| format!("{} = ?", x)).collect();

        let where_str: Vec<String> = where_fields
            .iter()
            .map(|(key, _)| format!("{}?", key))
            .collect();

        for value in where_fields.values() {
            values.push(value.clone());
        }

        drop(where_fields);

        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            table,
            vars.join(","),
            where_str.join(" and ")
        );

        let res: Affected = self
            .con
            .prep_exec(sql, &values)
            .map(|result| Affected {
                affected_rows: result.affected_rows(),
                last_insert_id: result.last_insert_id(),
            }).map_err(|e| {
                println!("Database error: {:?}", e);
                Error::from(e)
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
    ) -> Desult<Affected>
    where
        T: std::clone::Clone,
        Dypes: std::convert::From<T>,
    {
        let id_values: Vec<mysql::Value> = id_values
            .into_iter()
            .map(|x| mysql::Value::from(Dypes::from(x)))
            .collect();
        //let params = mysql::Params::from(params);
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
                .map(|e| if e.0 != 0 {
                    format!(",{}", e.1)
                } else {
                    e.1.to_string()
                }).collect::<String>()
        );

        println!("sql is {}", sql);
        let res: Affected = self
            .con
            .prep_exec(sql, &id_values)
            .map(|result| Affected {
                affected_rows: result.affected_rows(),
                last_insert_id: result.last_insert_id(),
            }).map_err(|e| {
                println!("Database error: {:?}", e);
                Error::from(e)
            })?;

        Ok(res)
    }

    fn delete_wid<T>(&self, table: &str, id_colum: &str, id_values: Vec<T>) -> Desult<Affected>
    where
        T: std::clone::Clone,
        Dypes: std::convert::From<T>,
    {
        self.delete_ids::<T>(table, id_colum, id_values, "IN")
    }

    fn delete_nwid<T>(&self, table: &str, id_colum: &str, id_values: Vec<T>) -> Desult<Affected>
    where
        T: std::clone::Clone,
        Dypes: std::convert::From<T>,
    {
        self.delete_ids::<T>(table, id_colum, id_values, "NOT IN")
    }

    fn concat_colums(colums: Vec<&str>) -> String {
        let s = colums.join(",");
        format!("concat_ws(' ', {})", s)
    }
}

impl From<Dypes> for mysql::Value {
    fn from(x: Dypes) -> mysql::Value {
        match x {
            Dypes::Uint(x) => Value::UInt(x),
            Dypes::Int(x) => Value::Int(x),
            Dypes::Float(x) => Value::Float(x),
            Dypes::String(x) => Value::Bytes(x.into_bytes()),
            Dypes::Bytes(x) => Value::Bytes(x),
            Dypes::Null => Value::NULL,
        }
    }
}

#[derive(Debug)]
pub struct DypesIr {
    val: Dypes,
}

impl ConvIr<Dypes> for DypesIr {
    fn new(v: Value) -> Result<DypesIr, mysql::FromValueError> {
        let ir = DypesIr {
            val: Dypes::from(v),
        };
        Ok(ir)
    }
    fn commit(self) -> Dypes {
        self.val
    }
    fn rollback(self) -> Value {
        Value::from(self.val)
    }
}

impl FromValue for Dypes {
    type Intermediate = DypesIr;
}

impl From<Value> for Dypes {
    fn from(x: Value) -> Dypes {
        match x {
            Value::UInt(d) => Dypes::Uint(d),
            Value::Int(d) => Dypes::Int(d),
            Value::Float(d) => Dypes::Float(d),
            Value::Bytes(d) => Dypes::Bytes(d),
            Value::NULL => Dypes::Null,
            Value::Date(y, m, d, h, mm, s, ss) => {
                Dypes::String(date_to_string((y, m, d, h, mm, s, ss)))
            }
            Value::Time(n, days, hours, mins, secs, micro_secs) => {
                Dypes::String(time_to_string((n, days, hours, mins, secs, micro_secs)))
            }
        }
    }
}

fn date_to_string(date: (u16, u8, u8, u8, u8, u8, u32)) -> String {
    format!(
        "{}-{}-{} {}:{}:{}:{}",
        date.0, date.1, date.2, date.3, date.4, date.5, date.6
    )
}

fn time_to_string(time: (bool, u32, u8, u8, u8, u32)) -> String {
    let n = if time.0 { "+" } else { "-" };
    format!(
        "{}{}:{}:{}:{}:{}",
        n, time.1, time.2, time.3, time.4, time.5
    )
}

#[derive(Debug)]
pub struct Rnd2Ir(f64);

impl mysql::prelude::ConvIr<Rnd2> for Rnd2Ir {
    fn new(v: mysql::Value) -> Result<Self, mysql::FromValueError> {
        match v {
            mysql::Value::Float(fl_val) => Ok(Rnd2Ir(round2(fl_val))),
            v => Err(mysql::FromValueError(v)),
        }
    }
    fn commit(self) -> Rnd2 {
        Rnd2::new(self.0)
    }
    fn rollback(self) -> mysql::Value {
        mysql::Value::Float(self.0)
    }
}

impl mysql::prelude::FromValue for Rnd2 {
    type Intermediate = Rnd2Ir;
}

impl Rowable for mysql::Row {
    fn get_val(&self, key: &str) -> Option<Dypes> {
        self.get::<Value, &str>(key).map(|x| Dypes::from(x))
    }

    fn get_date_string(&self, key: &str, format: &str) -> Desult<String> {
        let a: mysql::Value = self.get(key).unwrap();
        let b: (i32, u32, u32, u32, u32, u32, u32) = match a {
            mysql::Value::Date(a, b, c, d, e, f, g) => (
                a as i32, b as u32, c as u32, d as u32, e as u32, f as u32, g as u32,
            ),
            _ => return Err(Error::date_conv_err(key)),
        };

        if b.1 <= 0 || b.2 <= 0 {
            return Err(Error::date_conv_err(key));
        }

        let date = chrono::NaiveDate::from_ymd(b.0, b.1, b.2);

        let a = date.format(format);

        Ok(format!("{}", a))
    }
}

impl From<Params> for mysql::Params {
    fn from(x: Params) -> mysql::Params {
        mysql::Params::from(x.values())
    }
}
