use cow_utils::*;
use std::borrow::Cow;
use std::iter::Peekable;
use unicode_segmentation::UnicodeSegmentation;

pub struct AnalyzedToken<'a> {
    pub token: Cow<'a, str>,
    pub position: usize,
    pub start: usize,
    pub end: usize,
    pub type_: &'static str,
}

fn tokenize<'a>(input: &'a str) -> Box<dyn Iterator<Item = AnalyzedToken<'a>> + 'a> {
    Box::new(
        input
            .unicode_word_indices()
            .enumerate()
            .map(|(position, (start, token))| {
                let token = token.cow_to_lowercase();
                let len = token.len();
                AnalyzedToken {
                    token,
                    position,
                    start,
                    end: start + len,
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
