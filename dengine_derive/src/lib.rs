extern crate proc_macro;
extern crate proc_macro2;

extern crate syn;

#[macro_use]
extern crate quote;

use proc_macro2::TokenStream;
use syn::{DataStruct, DeriveInput};

#[proc_macro_derive(Insertable)]
pub fn insertable_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree
    let input: DeriveInput = syn::parse(input).unwrap();

    let extended = impl_insertable(&input);

    extended.into()
}

fn impl_insertable(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let funs;

    match ast.data {
        syn::Data::Struct(ref d) => funs = gen_funs(d),
        _ => panic!("Not as struct"),
    }

    quote!{
        impl Insertable for #name {
            #funs
        }
    }
}

fn gen_funs(data: &DataStruct) -> TokenStream {
    match data.fields {
        syn::Fields::Named(ref fnames) => {
            let mut fields: Vec<String> = Vec::new();

            for n in &fnames.named {
                let b = &n.ident;

                match b {
                    Some(x) => fields.push(x.to_string()),
                    _ => panic!("cannot unwrap()"),
                }
            }

            let fields = fields.iter();

            let fields2 = fnames.named.iter().map(|f| &f.ident);
            quote!{

                    fn fields()->Vec<String>{
                        //#fields
                        vec![#(#fields.to_string()),*]
                    }

                    fn values(&self)-> Vec<String>{
                        vec![#(Dypes::from(self.#fields2)),*]
                    }

            }
        }
        _ => panic!("Not named fields"),
    }
}

#[proc_macro_derive(Queryable)]
pub fn queryable_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Parse the input tokens into a syntax tree
    let input: DeriveInput = syn::parse(input).unwrap();

    let extended = impl_queryable(&input);

    extended.into()
}

fn impl_queryable(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let funs;

    match ast.data {
        syn::Data::Struct(ref d) => funs = gen_queryable_funs(d),
        _ => panic!("Not as struct"),
    }

    quote!{
        impl Queryable for #name {
            #funs
        }
    }
}

fn gen_queryable_funs(data: &DataStruct) -> TokenStream {
    match data.fields {
        syn::Fields::Named(ref fnames) => {
            let fields1 = fnames.named.iter().map(|f| &f.ident);
            let fields2 = fnames.named.iter().map(|f| &f.ident);
            quote!{

                fn new(row: Row) -> Self {
                    Self {
                        #(#fields1: row.get(stringify!(#fields2)).unwrap()),*
                    }
                }
            }
        }
        _ => panic!("Not named fields"),
    }
}
