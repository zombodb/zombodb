extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Item, ItemForeignMod, ForeignItem, ForeignItemFn, FnArg};
use syn::export::ToTokens;
use std::str::FromStr;

#[proc_macro_attribute]
pub fn elog_guard(
    _attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    // get a usable token stream
    let ast = parse_macro_input!(item as syn::Item);

    match ast {
        Item::ForeignMod(block) => rewrite_extern_block(block),
        _ => panic!("#[longjmp_guard] can only be applied to extern {{}} blocks")
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
        _ => panic!("#[longjmp_guard] can only be applied to extern blocks that only contain function declarations.  Offending line: {:?}", item)
    }
}

fn rewrite_foreign_item_fn(func: ForeignItemFn) -> TokenStream {
    let name = func.sig.ident.clone();
    let mut fn_call = String::new();

    let mut cnt = 0;
    fn_call.push('(');
    for arg in func.sig.inputs.iter() {
        match arg {
            FnArg::Typed(arg) => {
                if cnt > 0 {
                    fn_call.push(',');
                }

                // TODO:  HOW THE FUCK DO I JUST GET THE argument name/Ident?  I CANNOT FIGURE THIS OUT
                let mut arg = format!("{}", arg.into_token_stream());

                // TODO:  SO INSTEAD WE FORCE THE token stream TO A String AND JUST BLINDLY TRUNCATE IT AT THE FIRST ':'
                arg.truncate(arg.find(':').unwrap());

                fn_call.push_str(arg.as_str());
                cnt = cnt + 1;
            }
            _ => panic!("#[longjmp_guard] doesn't support external functions with 'self' as the argument"),
        }
    }
    fn_call.push(')');

    let fn_call = proc_macro2::TokenStream::from_str(fn_call.as_str()).unwrap();

    let body = quote! {
         {
            extern "C" {
                #func
            }

            // TODO:  preamble for setjmp/longjmp
            pg_bridge::jmp_wrapper(||unsafe { #name #fn_call })
            // TODO:  prologue for setjmp/longjmp
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

    tokens.extend(TokenStream::from_str(format!("{} {}", sig, body.into_token_stream()).as_str()));

    tokens
}