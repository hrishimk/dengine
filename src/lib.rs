#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
#[macro_use]
extern crate mysql;

use std::collections::HashMap;

use mysql as my;

pub struct Row(pub my::Row);

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

            let end_index = match sql.find(" limit ") {
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
}
