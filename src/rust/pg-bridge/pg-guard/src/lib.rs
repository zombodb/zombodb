extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use std::ops::Deref;
use std::str::FromStr;
use syn::export::ToTokens;
use syn::{
    parse_macro_input, FnArg, ForeignItem, ForeignItemFn, Item, ItemFn, ItemForeignMod, Pat,
    Signature, Visibility,
};

#[proc_macro_attribute]
pub fn pg_guard(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // get a usable token stream
    let ast = parse_macro_input!(item as syn::Item);

    match ast {
        Item::ForeignMod(block) => rewrite_extern_block(block),
        Item::Fn(func) => rewrite_item_fn(func),
        _ => {
            panic!("#[pg_guard] can only be applied to extern \"C\" blocks and top-level functions")
        }
    }
}

fn rewrite_extern_block(block: ItemForeignMod) -> TokenStream {
    let mut stream = TokenStream::new();

    for item in block.items.into_iter() {
        stream.extend(rewrite_foreign_item(item));
    }

    stream
}

fn rewrite_foreign_item(item: ForeignItem) -> TokenStream {
    match item {
        ForeignItem::Fn(func) => rewrite_foreign_item_fn(func),
        _ => panic!("#[pg_guard] can only be applied to extern blocks that only contain function declarations.  Offending line: {:?}", item)
    }
}

fn build_arg_list(sig: &Signature) -> proc_macro2::TokenStream {
    let mut arg_list = proc_macro2::TokenStream::new();

    for arg in &sig.inputs {
        match arg {
            FnArg::Typed(ty) => {
                if let Pat::Ident(ident) = ty.pat.deref() {
                    let name = ident.ident.to_token_stream();
                    arg_list.extend(quote! { #name, });
                }
            }
            FnArg::Receiver(_) => {
                panic!("#[pg_guard] doesn't support external functions with 'self' as the argument")
            }
        }
    }

    arg_list
}

fn build_func_name(sig: &Signature) -> proc_macro2::TokenStream {
    sig.ident.to_token_stream()
}

fn rewrite_item_fn(func: ItemFn) -> TokenStream {
    let mut orig_func: ItemFn = func.clone();
    let mut sig = func.sig;
    let arg_list = build_arg_list(&sig);
    let func_name = build_func_name(&sig);

    orig_func.vis = Visibility::Inherited;
    sig.abi = Some(syn::parse_str("extern \"C\"").unwrap());
    let sig = sig.into_token_stream();

    TokenStream::from(quote! {
    # [no_mangle]
    pub # sig {
    # orig_func

    use pg_bridge::guard;
    pg_bridge::guard::guard( | | unsafe { # func_name( # arg_list) })
    }
    })
}

fn rewrite_foreign_item_fn(func: ForeignItemFn) -> TokenStream {
    let func_name = build_func_name(&func.sig);
    let arg_list = build_arg_list(&func.sig);

    let body = quote! {
    {
    extern "C" {
    # func
    }

    use pg_bridge::guard;
    pg_bridge::guard::guard( | | unsafe { # func_name( # arg_list) })
    }
    };

    let mut tokens = TokenStream::new();
    let mut sig = format!("{}", func.clone().into_token_stream());
    if sig.starts_with("pub") {
        sig = sig.replace("pub ", "pub unsafe ");
    } else {
        sig = format!("unsafe {}", sig);
    }
    let sig = sig.replace(";", "");

    tokens.extend(TokenStream::from_str(
        format!("{} {}", sig, body.into_token_stream()).as_str(),
    ));

    tokens
}
