use serde;
use std;

#[derive(Clone, Debug)]
pub enum Dypes {
    Uint(u64),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Null,
}

impl From<String> for Dypes {
    fn from(val: String) -> Self {
        Dypes::String(val)
    }
}

impl From<Dypes> for Option<String> {
    fn from(val: Dypes) -> Self {
        match val {
            Dypes::String(v) => Some(v),
            Dypes::Bytes(v) => match String::from_utf8(v) {
                Ok(x) => Some(x),
                Err(_) => None,
            },
            _ => None,
        }
    }
}

impl From<Dypes> for Option<bool> {
    fn from(val: Dypes) -> Self {
        match val {
            Dypes::Int(x) => Some(x != 0),
            Dypes::Uint(x) => Some(x != 0),
            _ => None,
        }
    }
}

macro_rules! impl_from_dypes_for_opt_int {
    ($i:ty) => {
        impl From<Dypes> for Option<$i> {
            fn from(val: Dypes) -> Self {
                match val {
                    Dypes::Int(x) => Some(x as $i),
                    Dypes::Uint(x) => Some(x as $i),
                    Dypes::Float(x) => Some(x as $i),
                    _ => None,
                }
            }
        }
    };
}

macro_rules! impl_to_dypes_for_uint {
    ($i:ty) => {
        impl From<$i> for Dypes {
            fn from(val: $i) -> Self {
                Dypes::Uint(val as u64)
            }
        }
    };
}

macro_rules! impl_to_dypes_for_int {
    ($i:ty) => {
        impl From<$i> for Dypes {
            fn from(val: $i) -> Self {
                Dypes::Int(val as i64)
            }
        }
    };
}

macro_rules! impl_to_dypes_for_float {
    ($i:ty) => {
        impl From<$i> for Dypes {
            fn from(val: $i) -> Self {
                Dypes::Float(val as f64)
            }
        }
    };
}

impl_to_dypes_for_uint!(u64);
impl_to_dypes_for_uint!(u32);
impl_to_dypes_for_int!(i64);
impl_to_dypes_for_int!(i32);
impl_to_dypes_for_float!(f64);
impl_to_dypes_for_float!(f32);

impl_from_dypes_for_opt_int!(u64);
impl_from_dypes_for_opt_int!(i64);
impl_from_dypes_for_opt_int!(u32);
impl_from_dypes_for_opt_int!(i32);
impl_from_dypes_for_opt_int!(f64);
impl_from_dypes_for_opt_int!(f32);

#[derive(Debug)]
pub struct Rnd2(f64);

impl Rnd2 {
    pub fn new(f: f64) -> Self {
        Rnd2(round2(f))
    }
}

pub fn round2(a: f64) -> f64 {
    (a * 100_f64).round() / 100_f64
}

impl serde::Serialize for Rnd2 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl From<Dypes> for Option<Rnd2> {
    fn from(val: Dypes) -> Self {
        match val {
            Dypes::Float(x) => Some(Rnd2::new(x)),
            _ => None,
        }
    }
}

pub struct Params(pub Vec<Dypes>);

impl Params {
    pub fn new(val: Vec<Dypes>) -> Self {
        Params(val)
    }

    pub fn values(self) -> Vec<Dypes> {
        self.0
    }
}

impl<T> From<T> for Params
where
    Dypes: std::convert::From<T>,
{
    fn from(x: T) -> Self {
        Params::new(vec![Dypes::from(x)])
    }
}

impl<T> From<Vec<T>> for Params
where
    Dypes: std::convert::From<T>,
{
    fn from(x: Vec<T>) -> Self {
        Params::new(x.into_iter().map(|v| Dypes::from(v)).collect())
    }
}

/**
 * Stolen from
 * https://github.com/blackbeam/rust-mysql-simple
 */

macro_rules! into_params_impl {
    ($([$A:ident,$a:ident]),*) => (
        impl<$($A,)*> From<($($A,)*)> for Params where $(Dypes: std::convert::From<$A>,)*{
            fn from(x: ($($A,)*)) -> Params {
                let ($($a,)*) = x;
                let mut params = Vec::new();
                $(params.push(Dypes::from($a));)*
                Params::new(params)
            }
        }
    );
}

into_params_impl!([A, a]);
into_params_impl!([A, a], [B, b]);
into_params_impl!([A, a], [B, b], [C, c]);
into_params_impl!([A, a], [B, b], [C, c], [D, d]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f]);
into_params_impl!([A, a], [B, b], [C, c], [D, d], [E, e], [F, f], [G, g]);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h]
);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h],
    [I, i]
);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h],
    [I, i],
    [J, j]
);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h],
    [I, i],
    [J, j],
    [K, k]
);
into_params_impl!(
    [A, a],
    [B, b],
    [C, c],
    [D, d],
    [E, e],
    [F, f],
    [G, g],
    [H, h],
    [I, i],
    [J, j],
    [K, k],
    [L, l]
);
