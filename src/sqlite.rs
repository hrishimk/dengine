use super::{
    chrono, deslite, round2, Affected, Connectionable, Desult, Dypes, Error, Insertable, Params,
    Queryable, Rnd2, Row, Rowable, SelectHolder,
};
use deslite::{SqliteCon, Stmt, Value};
use std;

impl From<deslite::Error> for Error {
    fn from(val: deslite::Error) -> Self {
        match val {
            deslite::Error::SqliteError(x) => Error::SQLErr(x),
            deslite::Error::Unknown(x) => Error::Unknown(x),
            deslite::Error::BindError(x) => Error::LibErr(x),
            deslite::Error::PrepareErr(x) => Error::LibErr(x),
            deslite::Error::IndexOutOfBounds(x) => Error::IndexOutOfBound(x),
            deslite::Error::Empty => Error::Unknown("Empty".to_string()),
            deslite::Error::ConnectionErr(x) => Error::ConnectionErr(x),
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    db_name: String,
    pub con: deslite::SqliteCon,
    attached: Vec<String>,
}

impl Connection {
    pub fn new(db_name: &str) -> Desult<Self> {
        let con = SqliteCon::new(db_name).map_err(|e| Error::from(e))?;

        Ok(Connection {
            db_name: db_name.to_string(),
            con,
            attached: Vec::new(),
        })
    }

    pub fn attach(&mut self, db_name: &str, db_as: &str) -> Desult<()> {
        let mut stmt = Stmt::init(&self.con);
        let sql = format!("ATTACH DATABASE ? AS {}", db_as);
        stmt.prepare(sql.as_str()).map_err(|e| Error::from(e))?;
        let params = vec![db_name];
        stmt.bind_values(&params).unwrap();
        stmt.execute().map_err(|e| Error::from(e))?;
        self.attached.push(db_as.to_string());
        Ok(())
    }
}

impl From<Dypes> for deslite::Value {
    fn from(x: Dypes) -> deslite::Value {
        use deslite::Value;
        match x {
            Dypes::Uint(x) => Value::Uint(x),
            Dypes::Int(x) => Value::Int(x),
            Dypes::Float(x) => Value::Float(x),
            Dypes::String(x) => Value::String(x),
            Dypes::Bytes(x) => Value::Bytes(x),
            Dypes::Null => Value::Null,
        }
    }
}

impl From<Value> for Dypes {
    fn from(x: Value) -> Dypes {
        match x {
            Value::Bytes(x) => Dypes::Bytes(x),
            Value::Float(x) => Dypes::Float(x),
            Value::Int(x) => Dypes::Int(x),
            Value::Null => Dypes::Null,
            Value::String(x) => Dypes::String(x),
            Value::Uint(x) => Dypes::Uint(x),
        }
    }
}

impl From<deslite::Value> for Rnd2 {
    fn from(val: deslite::Value) -> Self {
        use deslite::Value;
        match val {
            Value::Float(x) => Rnd2::new(round2(x)),
            _ => panic!("Failed to convert non float value to Rnd2"),
        }
    }
}

impl<'a> Rowable for deslite::Row<'a> {
    fn get_val(&self, key: &str) -> Option<Dypes> {
        match self.get::<Value, &str>(key) {
            Ok(x) => Some(Dypes::from(x)),
            Err(_) => None,
        }
    }

    fn get_date_string(&self, key: &str, format: &str) -> Desult<String> {
        let a: deslite::Value = self.get::<deslite::Value, &str>(key).unwrap();

        let date = match a {
            Value::String(x) => {
                chrono::NaiveDate::parse_from_str(&x, "%Y-%m-%d").map_err(|_e| {
                    Error::ConversionErr(format!("Failed to convert {} to date string", key))
                })?
            }

            _ => {
                return Err(Error::ConversionErr(format!(
                    "Failed to convert {} to Value + date string. ",
                    key
                )))
            }
        };

        Ok(format!("{}", date.format(format)))
    }
}

impl Connectionable for Connection {
    fn execute<P>(&self, sql: &str, params: P) -> Desult<()>
    where
        P: std::clone::Clone,
        Params: std::convert::From<P>,
    {
        let mut stmt = deslite::Stmt::init(&self.con);
        stmt.prepare(sql).map_err(|e| Error::from(e))?;

        let params = Params::from(params);
        stmt.bind_values(&params.0).map_err(|e| Error::from(e))?;

        stmt.execute().map_err(|e| Error::from(e))?;

        Ok(())
    }

    fn value<T, R>(&self, sql: &str, colum: &str, params: R) -> Desult<T>
    where
        T: std::convert::From<Dypes>,
        R: std::clone::Clone,
        Params: std::convert::From<R>,
    {
        let mut stmt = deslite::Stmt::init(&self.con);
        stmt.prepare(sql).map_err(|e| Error::from(e))?;
        let params = Params::from(params);
        stmt.bind_values(&params.0).map_err(|e| Error::from(e))?;

        let row: deslite::Row = stmt.get_row().map_err(|e| {
            println!("{:?}", e);
            Error::from(e)
        })?;

        Ok(T::from(row.get::<Dypes, &str>(colum).unwrap()))
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
        let mut stmt = deslite::Stmt::init(&self.con);
        stmt.prepare(sql).map_err(|e| Error::from(e))?;

        let params = Params::from(params);
        stmt.bind_values(&params.0).map_err(|e| Error::from(e))?;

        let res: Vec<T> = stmt
            .get_rows()
            .iter()
            .map(|row| T::new(Row::new(&row)))
            .collect();

        let res_len = res.len();

        if !calc_found_rows {
            return Ok(SelectHolder {
                data: res,
                count: res_len,
            });
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

            let mut stmt = deslite::Stmt::init(&self.con);
            stmt.prepare(&new_sql).map_err(|e| Error::from(e))?;
            stmt.bind_values(&params.0).map_err(|e| Error::from(e))?;

            let count: Vec<usize> = stmt
                .get_rows()
                .iter()
                .map(|row| row.get::<usize, &str>("count").unwrap())
                .collect();

            match count.get(0) {
                Some(x) => {
                    println!("inside get ",);
                    return Ok(SelectHolder {
                        data: res,
                        count: *x,
                    });
                }
                None => return Err(Error::LibErr("Count get error".to_string())),
            };
        }
    }

    fn row<T, R>(&self, sql: &str, params: R) -> Desult<T>
    where
        T: Queryable,
        R: std::clone::Clone,
        Params: std::convert::From<R>,
    {
        let mut stmt = deslite::Stmt::init(&self.con);
        stmt.prepare(sql).map_err(|e| Error::from(e))?;
        let params = Params::from(params);
        stmt.bind_values(&params.0).map_err(|e| Error::from(e))?;

        let row: deslite::Row = stmt.get_row().map_err(|e| {
            println!("{:?}", e);
            Error::from(e)
        })?;

        Ok(T::new(Row::new(&row)))
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

        let mut stmt = deslite::Stmt::init(&self.con);

        stmt.prepare(&sql).map_err(|e| Error::from(e))?;
        stmt.bind_values(&a_arr).map_err(|e| Error::from(e))?;

        stmt.execute().map_err(|e| {
            println!("{:?}", e);
            Error::from(e)
        })?;

        let res = Affected {
            affected_rows: self.con.affected_rows() as u64,
            last_insert_id: self.con.last_insert_id(),
        };

        Ok(res)
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
        let id_values: Vec<deslite::Value> = id_values
            .into_iter()
            .map(|x| deslite::Value::from(Dypes::from(x)))
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

        let mut stmt = deslite::Stmt::init(&self.con);

        stmt.prepare(&sql).map_err(|e| Error::from(e))?;
        stmt.bind_values(&id_values).map_err(|e| Error::from(e))?;

        stmt.execute().map_err(|e| {
            println!("{:?}", e);
            Error::from(e)
        })?;

        let res = Affected {
            affected_rows: self.con.affected_rows() as u64,
            last_insert_id: self.con.last_insert_id(),
        };

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
        let mut s = "(".to_string();

        for (i, n) in colums.into_iter().enumerate() {
            if i != 0 {
                s += " || ";
            }
            s += n;
        }

        s += " )";

        s
    }
}
