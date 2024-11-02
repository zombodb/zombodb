use cow_utils::*;
use std::borrow::Cow;
use std::iter::Peekable;
use unicode_segmentation::{UnicodeSegmentation, UnicodeWordIndices};

pub struct AnalyzedToken<'a> {
    pub token: Cow<'a, str>,
    pub position: usize,
    pub start: usize,
    pub end: usize,
    pub type_: &'static str,
}

fn tokenize<'a>(input: &'a str) -> Box<dyn Iterator<Item = AnalyzedToken<'a>> + 'a> {
    Box::new(
        Utf16WordIndices::new(input)
            .enumerate()
            .map(|(position, (byte_range, token))| {
                let token = token.cow_to_lowercase();
                let start = byte_range.start;
                let end = byte_range.end;
                AnalyzedToken {
                    token,
                    position,
                    start,
                    end,
                    type_: "<ALPHANUM>",
                }
            }),
    )
}

pub fn standard(input: &str) -> Standard {
    Standard {
        iter: tokenize(input),
    }
}

pub fn fulltext_with_shingles(input: &str) -> FulltextWithShingles {
    FulltextWithShingles {
        iter: tokenize(input).peekable(),
        shingle: None,
    }
}

pub struct Standard<'a> {
    iter: Box<dyn Iterator<Item = AnalyzedToken<'a>> + 'a>,
}

impl<'a> Iterator for Standard<'a> {
    type Item = AnalyzedToken<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct FulltextWithShingles<'a> {
    iter: Peekable<Box<dyn Iterator<Item = AnalyzedToken<'a>> + 'a>>,
    shingle: Option<AnalyzedToken<'a>>,
}

impl<'a> Iterator for FulltextWithShingles<'a> {
    type Item = AnalyzedToken<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(shingle) = self.shingle.take() {
            return Some(shingle);
        }

        if let Some(this) = self.iter.next() {
            if let Some(next) = self.iter.peek() {
                self.shingle = Some(AnalyzedToken {
                    token: Cow::Owned(format!("{}${}", this.token, next.token)),
                    position: this.position,
                    start: this.start,
                    end: next.end,
                    type_: "shingle",
                });
            }

            Some(this)
        } else {
            None
        }
    }
}

/// Returns character-offsets of words in UTF16 encoded text.
///
/// That is, if you have a UTF16-encoded Java `char[]` array, then these
/// indices would be valid for that array (it's a bit odd, yeah).
pub struct Utf16WordIndices<'a> {
    pos: usize,
    last_end: usize,
    text: &'a str,
    word_indices: UnicodeWordIndices<'a>,
}

impl<'a> Utf16WordIndices<'a> {
    #[inline]
    pub fn new(s: &'a str) -> Self {
        Self {
            pos: 0,
            last_end: 0,
            text: s,
            word_indices: s.unicode_word_indices(),
        }
    }
}

impl<'a> Iterator for Utf16WordIndices<'a> {
    type Item = (std::ops::Range<usize>, &'a str);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let (i, text) = self.word_indices.next()?;
        if i != self.last_end {
            let skipped = self.text[self.last_end..i].encode_utf16().count();
            self.pos += skipped;
        }
        let start = self.pos;
        let len = text.encode_utf16().count();
        self.pos += len;
        self.last_end = i + text.len();
        Some(((start)..(self.pos), text))
    }
}

// // the indices yielded by `Utf16WordByteIndices` are valid for this Vec.
// // Which is a bit weird, but matches `String.getBytes("UTF16")` (maybe?
// // unsure about what endianness that returns).
// pub fn utf16le_bytes(s: &str) -> Vec<u8> {
//     s.char_indices()
//         .flat_map(|(i, c)| {
//             s[i..(i + c.len_utf8())]
//                 .encode_utf16()
//                 .flat_map(|cu| cu.to_le_bytes())
//         })
//         .collect()
// }
