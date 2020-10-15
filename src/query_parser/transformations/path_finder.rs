use crate::query_parser::ast::{IndexLink, QualifiedIndex};
use pathfinding::prelude::*;
use std::collections::{HashMap, HashSet};

pub struct PathFinder {
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

    pub fn find_path(&mut self, start: &IndexLink, end: &IndexLink) -> Option<Vec<IndexLink>> {
        self.relationships.clear();
        self.define_relationships();

        let empty = Vec::new();
        match dijkstra(
            start,
            |link| {
                self.relationships
                    .get(&link.qualified_index)
                    .unwrap_or(&empty)
                    .into_iter()
                    .map(|(l, c)| (l.clone(), *c))
            },
            |p| {
                (p.name.is_some() && p.name == end.name)
                    || (end.name.is_none() && p.qualified_index == end.qualified_index)
            },
        ) {
            Some((paths, _)) => {
                if self.root.eq(paths.get(0).as_ref().unwrap()) {
                    // if the top node is our root node, we don't want to return it
                    Some(paths[1..].to_vec())
                } else {
                    Some(paths)
                }
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query_parser::ast::IndexLink;
    use crate::query_parser::transformations::path_finder::PathFinder;

    #[test]
    fn test_find_path() {
        let main = IndexLink::parse("id=<public.main.idxmain>id");
        let ft = IndexLink::parse("id=<public.main_ft.idxmain_ft>ft_id");
        let other = IndexLink::parse("other:(id=<public.other.idxother>other_id)");
        let vol = IndexLink::parse("id=<public.vol.idxvol>vol_id");
        let junk = IndexLink::parse("junk:(other.other_id=<public.foo.idxfoo>foo_id)");
        let bar = IndexLink::parse("junk.junk_id=<public.bar.idxbar>bar_id");

        let mut pf = PathFinder::new(&main);
        pf.push(main.clone());
        pf.push(ft.clone());
        pf.push(other.clone());
        pf.push(vol.clone());
        pf.push(junk.clone());
        pf.push(bar.clone());

        assert_eq!(pf.find_path(&main, &main).expect("no path found"), vec![]);
        assert_eq!(
            pf.find_path(&main, &ft).expect("no path found"),
            vec![ft.clone()]
        );
        assert_eq!(
            pf.find_path(&main, &junk).expect("no path found"),
            vec![other.clone(), junk.clone()]
        );

        assert_eq!(
            pf.find_path(&ft, &vol).expect("no path found"),
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

// use crate::query_parser::ast::{IndexLink, QualifiedField, QualifiedIndex};
// use crate::query_parser::transformations::field_finder::find_link_for_field;
// use std::collections::HashMap;
//
// #[derive(Debug, Eq, PartialEq)]
// pub struct PathEntry {
//     index: IndexLink,
//     destinations: Vec<PathEntry>,
// }
//
// #[derive(Debug)]
// pub struct PathFinder {
//     sources: Vec<PathEntry>,
// }
//
// impl PathEntry {
//     fn new(index: IndexLink, initial_destination: IndexLink) -> Self {
//         PathEntry {
//             index,
//             destinations: vec![PathEntry {
//                 index: initial_destination,
//                 destinations: Default::default(),
//             }],
//         }
//     }
// }
//
// impl PathFinder {
//     pub fn new() -> Self {
//         PathFinder {
//             sources: Default::default(),
//         }
//     }
//
//     pub fn push(
//         &mut self,
//         index_links: &Vec<IndexLink>,
//         source: IndexLink,
//         destination: IndexLink,
//     ) {
//         // forward link
//         self.push0(index_links, source.clone(), destination.clone());
//
//         // reverse link
//         self.push0(index_links, destination.clone(), source.clone());
//     }
//
//     fn push0(&mut self, index_links: &Vec<IndexLink>, source: IndexLink, destination: IndexLink) {
//         let source = find_link_for_field(
//             &QualifiedField {
//                 index: None,
//                 field: match &destination.left_field {
//                     Some(field) => field.clone(),
//                     None => source.right_field.clone().unwrap(),
//                 },
//             },
//             &source,
//             index_links,
//         )
//         .unwrap_or(source);
//
//         match self.find_entry(&source) {
//             Some(mut path) => {
//                 let idx = path.len() - 1;
//                 let entry = path.get_mut(idx).unwrap();
//                 entry.destinations.push(PathEntry {
//                     index: destination,
//                     destinations: Default::default(),
//                 });
//             }
//             None => self.sources.push(PathEntry::new(source, destination)),
//         }
//     }
//
//     pub fn calculate_path(&mut self, from: &IndexLink, to: &IndexLink) -> Vec<&IndexLink> {
//         let mut root = None;
//         for start in self.sources.iter_mut() {
//             if &start.index == from {
//                 root = Some(start);
//                 break;
//             }
//         }
//
//         match root {
//             Some(root) => {
//                 let mut path = Vec::new();
//                 if PathFinder::find_entry0(&mut root.destinations, to, &mut path) {
//                     let mut links = Vec::with_capacity(path.len());
//
//                     path.into_iter().for_each(|entry| links.push(&entry.index));
//
//                     links
//                 } else {
//                     vec![]
//                 }
//             }
//             None => vec![],
//         }
//     }
//
//     pub fn find_entry(&mut self, index: &IndexLink) -> Option<Vec<&mut PathEntry>> {
//         let mut path = Vec::new();
//         if PathFinder::find_entry0(&mut self.sources, index, &mut path) {
//             Some(path)
//         } else {
//             None
//         }
//     }
//
//     fn find_entry0<'a>(
//         root: &'a mut Vec<PathEntry>,
//         index: &IndexLink,
//         path: &mut Vec<&'a mut PathEntry>,
//     ) -> bool {
//         for entry in root.iter_mut() {
//             if &index.qualified_index == &entry.index.qualified_index {
//                 // we found the index in this entry
//                 path.push(entry);
//                 return true;
//             }
//
//             // need to check this entry's children
//             if PathFinder::find_entry0(&mut entry.destinations, index, path) {
//                 // we found it!
//                 return true;
//             }
//
//             // we didn't find it, so it's not down this entry
//             path.pop();
//         }
//
//         return false;
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::query_parser::ast::{IndexLink, QualifiedIndex};
//     use crate::query_parser::transformations::path_finder::PathFinder;
//
//     #[test]
//     fn test_path_finder() {
//         let root = IndexLink {
//             name: None,
//             left_field: None,
//             qualified_index: QualifiedIndex {
//                 schema: Some(String::from("public")),
//                 table: String::from("root"),
//                 index: String::from("idxroot"),
//             },
//             right_field: None,
//         };
//
//         let a = IndexLink::parse("root_id=<public.a.idxa>a_id");
//         let b = IndexLink::parse("root_id=<public.b.idxb>b_id");
//         let c = IndexLink::parse("b_id=<public.c.idxc>c_id");
//         let links = vec![root.clone(), a.clone(), b.clone(), c.clone()];
//
//         let mut pf = PathFinder::new();
//         pf.push(&links, root.clone(), a.clone());
//         pf.push(&links, root.clone(), b.clone());
//     }
// }
