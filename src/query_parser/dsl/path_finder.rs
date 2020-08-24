use crate::query_parser::ast::{IndexLink, QualifiedIndex};
use pathfinding::prelude::*;
use std::collections::{HashMap, HashSet};

struct PathFinder {
    links: HashSet<IndexLink>,
    relationships: HashMap<QualifiedIndex, Vec<(IndexLink, usize)>>,
    root: IndexLink,
}

impl PathFinder {
    pub fn new(root: &IndexLink) -> Self {
        PathFinder {
            relationships: HashMap::new(),
            links: HashSet::new(),
            root: root.clone(),
        }
    }

    fn find_link(&self, name: &str) -> Option<&IndexLink> {
        let name = Some(name.to_string());
        for link in &self.links {
            if name == link.name {
                return Some(link);
            }
        }
        None
    }

    pub fn push(&mut self, link: IndexLink) {
        self.links.insert(link);
    }

    fn define_relationships(&mut self) {
        let links = self
            .links
            .iter()
            .map(|v| v.clone())
            .collect::<Vec<IndexLink>>();
        for link in links {
            self.define_relationship(link);
        }
    }

    fn define_relationship(&mut self, link: IndexLink) {
        let mut source = None;
        let mut cost = 1;

        if let Some(left_field) = link.left_field.as_ref() {
            if left_field.contains('.') {
                if let Some(link_name) = left_field.split('.').next() {
                    match self.find_link(link_name) {
                        Some(link) => {
                            source = Some(link.clone());
                            cost += 1;
                        }
                        None => panic!("No index link named '{}'", link_name),
                    }
                }
            }
        }

        let source = source.unwrap_or(self.root.clone());

        // forward path from 'source' to the provided link
        self.relationships
            .entry(source.qualified_index.clone())
            .or_default()
            .push((link.clone(), cost));

        // reverse path from the provided link to 'source'
        let reverse = IndexLink {
            name: None,
            left_field: link.right_field.clone(),
            right_field: source.left_field.clone(),
            qualified_index: source.qualified_index,
        };
        self.relationships
            .entry(link.qualified_index.clone())
            .or_default()
            .push((reverse, cost));
    }

    fn find_path(
        &mut self,
        start: &IndexLink,
        end: &QualifiedIndex,
    ) -> Option<(Vec<IndexLink>, usize)> {
        self.relationships.clear();
        self.define_relationships();

        let empty = Vec::new();
        dijkstra(
            &start,
            |link| {
                self.relationships
                    .get(&link.qualified_index)
                    .unwrap_or(&empty)
                    .into_iter()
                    .map(|(l, c)| (l.clone(), *c))
            },
            |p| &p.qualified_index == end,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::query_parser::ast::IndexLink;
    use crate::query_parser::dsl::path_finder::PathFinder;

    #[test]
    fn test_find_path() {
        let main = IndexLink::parse("id=<main.idxmain>id");
        let ft = IndexLink::parse("id=<main_ft.idxmain_ft>ft_id");
        let other = IndexLink::parse("other:(id=<other.idxother>other_id)");
        let vol = IndexLink::parse("id=<vol.idxvol>vol_id");
        let junk = IndexLink::parse("junk:(other.other_id=<foo.idxfoo>foo_id)");
        let bar = IndexLink::parse("junk.junk_id=<bar.idxbar>bar_id");

        let mut pf = PathFinder::new(&main);
        pf.push(main.clone());
        pf.push(ft.clone());
        pf.push(other.clone());
        pf.push(vol.clone());
        pf.push(junk.clone());
        pf.push(bar.clone());

        assert_eq!(
            pf.find_path(&main, &main.qualified_index)
                .expect("no path found")
                .0,
            vec![main.clone()]
        );
        assert_eq!(
            pf.find_path(&main, &ft.qualified_index)
                .expect("no path found")
                .0,
            vec![main.clone(), ft.clone()]
        );
        assert_eq!(
            pf.find_path(&main, &junk.qualified_index)
                .expect("no path found")
                .0,
            vec![main.clone(), other.clone(), junk.clone()]
        );

        assert_eq!(
            pf.find_path(&ft, &vol.qualified_index)
                .expect("no path found")
                .0,
            vec![
                ft.clone(),
                IndexLink {
                    // this is a reverse path.  it goes goes through 'main', but the left field has
                    // to be that of `ft`'s right field
                    name: None,
                    left_field: ft.right_field.clone(),
                    qualified_index: main.qualified_index.clone(),
                    right_field: main.left_field.clone()
                },
                vol.clone()
            ]
        );
    }
}
