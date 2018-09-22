# dengine
A RUST API over MYSQL and SQLITE APIs.

## Libraries used
- Mysql => https://crates.io/crates/mysql
- Sqlite => https://crates.io/crates/deslite

## Usage
    // extern crate dengine;
    // #[macro_use]
    // extern crate dengine_derive;

    use dengine::Connectionable;
    use dengine::Queryable;
    use dengine::Row;
    type DbCon = dengine::sqlite::Connection;

    #[derive(Debug, Queryable)]
    struct User {
        id: u64,
        name: String,
    }

    let con = DbCon::new(":memory:").unwrap();

    let sql = "CREATE TABLE user (id INTEGER NOT NULL, name TEXT NOT NULL)";

    con.execute(sql, ()).unwrap();

    let sql = "INSERT INTO user (id, name) VALUES (?, ?), (?, ?)";
    con.execute(sql, (1, "name1", 2, "name2")).unwrap();

    let sql = "SELECT * from user";
    let res: Vec<User> = con.array(sql, (), false).unwrap();

    println!("{:?}", res);

    //[User { id: 1, name: "name1" }, User { id: 2, name: "name2" }]

**Important: Use https://github.com/diesel-rs/diesel instead of this lib**

This library is created only because I am too stupid too understand the diesel documentation.

