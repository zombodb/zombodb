extern crate proc_macro;

use common::{build_arg_list, build_func_name, rewrite_extern_block};
use proc_macro::TokenStream;
use quote::quote;
use syn::export::ToTokens;
use syn::{parse_macro_input, Item, ItemFn, Visibility};

#[proc_macro_attribute]
pub fn pg_guard(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // get a usable token stream
    let ast = parse_macro_input!(item as syn::Item);

    match ast {
        Item::ForeignMod(block) => TokenStream::from(rewrite_extern_block(block)),
        Item::Fn(func) => TokenStream::from(rewrite_item_fn(func)),
        _ => {
            panic!("#[pg_guard] can only be applied to extern \"C\" blocks and top-level functions")
        }
    }
}

fn rewrite_item_fn(func: ItemFn) -> proc_macro2::TokenStream {
    let mut orig_func: ItemFn = func.clone();
    let mut sig = func.sig;
    let arg_list = build_arg_list(&sig);
    let func_name = build_func_name(&sig);

    orig_func.vis = Visibility::Inherited;
    sig.abi = Some(syn::parse_str("extern \"C\"").unwrap());
    let sig = sig.into_token_stream();

    proc_macro2::TokenStream::from(quote! {
        #[no_mangle]
        pub #sig {
            #orig_func

            use pg_bridge::guard;
            pg_bridge::guard::guard( | | unsafe { # func_name( # arg_list) })
        }
    })
}
