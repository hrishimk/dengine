use mysql::Value;

#[derive(Clone)]
pub enum Dypes {
    Uint(u64),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl From<Dypes> for Value {
    fn from(x: Dypes) -> Value {
        match x {
            Dypes::Uint(x) => Value::UInt(x),
            Dypes::Int(x) => Value::Int(x),
            Dypes::Float(x) => Value::Float(x),
            Dypes::String(x) => Value::Bytes(x.into_bytes()),
            Dypes::Bytes(x) => Value::Bytes(x),
        }
    }
}
