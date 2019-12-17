extern crate proc_macro;

use pg_guard_rewriter::{PgGuardRewriter, RewriteMode};
use proc_macro::TokenStream;
use syn::{parse_macro_input, Item};

#[proc_macro_attribute]
pub fn pg_guard(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // get a usable token stream
    let ast = parse_macro_input!(item as syn::Item);

    let rewriter = PgGuardRewriter::new(RewriteMode::RewriteFunctionWithWrapper);

    match ast {
        Item::ForeignMod(block) => TokenStream::from(rewriter.extern_block(block)),
        Item::Fn(func) => TokenStream::from(rewriter.item_fn(func)),
        _ => {
            panic!("#[pg_guard] can only be applied to extern \"C\" blocks and top-level functions")
        }
    }
}
