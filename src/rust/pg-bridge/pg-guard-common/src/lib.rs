use quote::quote;
use std::ops::Deref;
use std::str::FromStr;
use syn::export::{ToTokens, TokenStream2};
use syn::{FnArg, ForeignItem, ForeignItemFn, ItemForeignMod, Pat, Signature};

pub fn rewrite_extern_block(block: ItemForeignMod) -> proc_macro2::TokenStream {
    let mut stream = TokenStream2::new();

    for item in block.items.into_iter() {
        stream.extend(rewrite_foreign_item(item));
    }

    stream
}

pub fn rewrite_foreign_item(item: ForeignItem) -> proc_macro2::TokenStream {
    match item {
        ForeignItem::Fn(func) => {
            if func.sig.variadic.is_some() {
                return quote! { extern "C" { #func } };
            }

            rewrite_foreign_item_fn(func)
        }
        _ => quote! { extern "C" { #item } },
    }
}

pub fn rewrite_foreign_item_fn(func: ForeignItemFn) -> proc_macro2::TokenStream {
    let func_name = build_func_name(&func.sig);
    let arg_list = build_arg_list(&func.sig);
    let inner_func = func.clone();

    let body = quote! {
        {
            extern "C" {
                #inner_func
            }

            use pg_bridge::guard;
            pg_bridge::guard::guard(|| unsafe { #func_name( #arg_list) })
        }
    };

    let mut tokens = proc_macro2::TokenStream::new();
    let mut outer_func: ForeignItemFn = func.clone();
    outer_func.attrs.clear();
    let mut sig = format!("{}", outer_func.into_token_stream());
    if sig.starts_with("pub") {
        sig = sig.replace("pub ", "pub unsafe ");
    } else {
        sig = format!("unsafe {}", sig);
    }
    let sig = sig.replace(";", "");

    tokens.extend(proc_macro2::TokenStream::from_str(
        format!("{} {}", sig, body.into_token_stream()).as_str(),
    ));

    tokens
}

pub fn build_func_name(sig: &Signature) -> proc_macro2::TokenStream {
    sig.ident.to_token_stream()
}

pub fn build_arg_list(sig: &Signature) -> proc_macro2::TokenStream {
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
