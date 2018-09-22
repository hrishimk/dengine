# dengine
A RUST API over MYSQL and SQLITE APIs.

## Libraries used
- Mysql => https://crates.io/crates/mysql
- Sqlite => https://crates.io/crates/deslite

## Usage
    type DbCon = dengine::sqlite::Connection;
    
    let con = DbCon::new(":memory:");

