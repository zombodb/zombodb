use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::{Debug, Error};
use std::ops::Deref;
use std::rc::Rc;
use std::str::FromStr;

use serde::export::Formatter;

use crate::query_parser::ast::{IndexLink, QualifiedIndex};

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone)]
pub struct NamedIndex {
    name: String,
    index: String,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone)]
pub struct NamedIndexParts {
    name: Option<String>,
    index: String,
    field_name: Option<String>,
}

impl From<NamedIndex> for NamedIndexParts {
    fn from(input: NamedIndex) -> Self {
        let name = if input.name == "null" {
            None
        } else {
            Some(input.name)
        };
        let mut index_parts = input.index.split(':');
        let (index, field_name) = (
            index_parts.next().unwrap().to_string(),
            index_parts.next().map_or(None, |v| Some(v.to_string())),
        );
        NamedIndexParts {
            name,
            index,
            field_name,
        }
    }
}

impl Debug for NamedIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}@{}", self.name, self.index)
    }
}

impl NamedIndex {
    fn new(name: &str, index: &str) -> Self {
        NamedIndex {
            name: name.to_string(),
            index: index.to_string(),
        }
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct Edge {
    target: Rc<RefCell<Vertex>>,
    weight: usize,
}

impl Edge {
    fn from_cell(target: Rc<RefCell<Vertex>>, weight: usize) -> Self {
        Edge { target, weight }
    }

    fn reset(&mut self) {
        match self.target.try_borrow_mut() {
            Ok(mut target) => target.reset(),
            Err(_) => {}
        }
        // if self.target.borrow().previous.is_some() {
        //     self.target.borrow_mut().reset()
        // }
    }
}

#[derive(Eq, Ord)]
pub struct Vertex {
    name: NamedIndex,
    adjacencies: Vec<Edge>,
    min_distance: usize,
    previous: Option<Rc<RefCell<Vertex>>>,
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl PartialOrd for Vertex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.min_distance.partial_cmp(&other.min_distance)
    }
}

impl Vertex {
    fn with_named_index(name: NamedIndex) -> Self {
        Vertex {
            name,
            adjacencies: Vec::new(),
            min_distance: std::usize::MAX,
            previous: None,
        }
    }

    pub fn add_vertex(&mut self, v: Rc<RefCell<Vertex>>, weight: usize) -> Rc<RefCell<Vertex>> {
        self.add_edge(Edge::from_cell(v, weight))
    }

    fn add_edge(&mut self, e: Edge) -> Rc<RefCell<Vertex>> {
        // if self.name == e.target.borrow().name {
        //     return Rc::new(RefCell::new(self));
        // } else {
        //     for edge in self.adjacencies.iter() {
        //         if edge.target.eq(&e.target) {
        //             return Rc::new(RefCell::new(self));
        //         }
        //     }
        // }

        self.adjacencies.push(e);
        let last = self.adjacencies.last().unwrap();
        last.target.clone()
    }

    fn reset(&mut self) {
        self.min_distance = std::usize::MAX;
        self.previous = None;
        for e in self.adjacencies.iter_mut() {
            e.reset();
        }
    }
}

pub struct Dijkstra {
    verticies: HashMap<NamedIndex, Rc<RefCell<Vertex>>>,
}

impl Dijkstra {
    pub fn new() -> Self {
        Dijkstra {
            verticies: HashMap::new(),
        }
    }

    fn vertex(&mut self, name: &str, index: &str) -> Rc<RefCell<Vertex>> {
        let ni = NamedIndex::new(name, index);
        self.verticies
            .entry(ni.clone())
            .or_insert(Rc::new(RefCell::new(Vertex::with_named_index(ni))))
            .clone()
    }

    fn compute_paths(&mut self, source: Rc<RefCell<Vertex>>) {
        for v in self.verticies.values_mut() {
            v.borrow_mut().reset()
        }
        source.borrow_mut().min_distance = 0;
        let mut vertex_queue = BinaryHeap::new();
        vertex_queue.push(source);

        while !vertex_queue.is_empty() {
            let u = vertex_queue.pop().unwrap();

            let u_min_distance = u.borrow().min_distance;
            for e in u.borrow().adjacencies.iter() {
                let v = e.target.clone();

                let weight = e.weight;
                let distance_through_u = u_min_distance + weight;
                if distance_through_u < v.borrow().min_distance {
                    // vertex_queue.remove(v);
                    {
                        let mut new_vertex_queue = BinaryHeap::new();
                        for i in vertex_queue {
                            if !std::ptr::eq(i.deref().as_ptr(), v.deref().as_ptr()) {
                                new_vertex_queue.push(i);
                            }
                        }
                        vertex_queue = new_vertex_queue;
                    }

                    v.borrow_mut().min_distance = distance_through_u;
                    v.borrow_mut().previous = Some(u.clone());
                    vertex_queue.push(v);
                }
            }
        }
    }

    pub fn get_shortest_path(
        &mut self,
        source_name: &str,
        source_index: &str,
        destination_name: &str,
        destination_index: &str,
    ) -> Vec<Rc<RefCell<Vertex>>> {
        let source = NamedIndex::new(source_name, source_index);
        let destination = NamedIndex::new(destination_name, destination_index);

        if !self.verticies.contains_key(&source) {
            panic!("no such source vertex: {:?}", source);
        } else if !(self.verticies.contains_key(&destination)) {
            panic!("no such destination vertex: {:?}", destination);
        }

        let source_vertex = self.vertex(source_name, source_index);
        let dest_vertex = self.vertex(destination_name, destination_index);
        self.compute_paths(source_vertex.clone());

        let mut path = Vec::new();
        let mut vertex = Some(dest_vertex);
        while vertex.is_some() {
            let v = vertex.unwrap();
            vertex = v.borrow().previous.clone();
            path.push(v);
        }

        path
    }
}

pub struct RelationshipManager {
    d: Dijkstra,
}

impl RelationshipManager {
    pub fn new() -> Self {
        RelationshipManager { d: Dijkstra::new() }
    }

    pub fn add_relationship(
        &mut self,
        source: &IndexLink,
        source_field: &str,
        dest: &IndexLink,
        dest_field: &str,
    ) {
        self.add_relationship0(
            &source.name.clone().unwrap_or("null".into()),
            &source.qualified_index.qualified_name(),
            source_field,
            &dest.name.clone().unwrap_or("null".into()),
            &dest.qualified_index.qualified_name(),
            dest_field,
        );
    }

    fn add_relationship0(
        &mut self,
        source_name: &str,
        source_index: &str,
        source_field: &str,
        destination_name: &str,
        destination_index: &str,
        destination_field: &str,
    ) {
        let source = self.d.vertex(source_name, source_index);
        let dest = self.d.vertex(destination_name, destination_index);

        let src_field = self
            .d
            .vertex(source_name, &format!("{}:{}", source_index, source_field));
        let dest_field = self.d.vertex(
            destination_name,
            &format!("{}:{}", destination_index, destination_field),
        );

        source
            .borrow_mut()
            .add_vertex(src_field.clone(), 1)
            .borrow_mut()
            .add_vertex(dest_field.clone(), 1)
            .borrow_mut()
            .add_vertex(dest.clone(), 2);

        dest.borrow_mut()
            .add_vertex(dest_field, 1)
            .borrow_mut()
            .add_vertex(src_field, 1)
            .borrow_mut()
            .add_vertex(source, 2);
    }

    pub fn calc_path(&mut self, source: &IndexLink, dest: &IndexLink) -> Vec<IndexLink> {
        let path: Vec<NamedIndexParts> = self
            .calc_path0(
                &source.name.clone().unwrap_or("null".into()),
                &source.qualified_index.qualified_name(),
                &dest.name.clone().unwrap_or("null".into()),
                &dest.qualified_index.qualified_name(),
            )
            .into_iter()
            .rev()
            .map(|ni| NamedIndexParts::from(ni))
            .filter(|nip| nip.field_name.is_some())
            .collect();

        let mut reduced_path = Vec::new();
        let mut iter = path.into_iter();
        while let Some(current) = iter.next() {
            if let Some(next) = iter.next() {
                let left_field = current.field_name;
                let right_field = next.field_name;
                let name = next.name;
                let index = next.index;
                reduced_path.push(IndexLink {
                    name,
                    left_field,
                    qualified_index: QualifiedIndex::from_str(&index)
                        .unwrap_or_else(|_| panic!("invalid index: {}", index)),
                    right_field,
                })
            } else {
                panic!("incomplete path from {} to {}", source, dest);
            }
        }

        reduced_path
    }

    fn calc_path0(
        &mut self,
        source_name: &str,
        source_index: &str,
        dest_name: &str,
        dest_index: &str,
    ) -> Vec<NamedIndex> {
        let path = self
            .d
            .get_shortest_path(source_name, source_index, dest_name, dest_index);
        let mut path_names = Vec::with_capacity(path.len());
        for p in path {
            path_names.push(p.borrow().name.clone());
        }
        path_names
    }
}

#[cfg(test)]
mod tests {
    use crate::query_parser::ast::{IndexLink, QualifiedIndex};
    use crate::query_parser::transformations::dijkstra::RelationshipManager;

    fn case_profile() -> IndexLink {
        let case_profile = IndexLink {
            name: None,
            left_field: Some("cp_id".into()),
            qualified_index: QualifiedIndex {
                schema: None,
                table: "case_profile".into(),
                index: "idxcase_profile".into(),
            },
            right_field: Some("cp_id".into()),
        };
        case_profile
    }

    fn docs() -> IndexLink {
        let docs = IndexLink {
            name: None,
            left_field: Some("docs_id".into()),
            qualified_index: QualifiedIndex {
                schema: None,
                table: "docs".into(),
                index: "idxdocs".into(),
            },
            right_field: Some("docs_id".into()),
        };
        docs
    }

    fn main_vol() -> IndexLink {
        let main_vol = IndexLink {
            name: None,
            left_field: Some("vol_id".into()),
            qualified_index: QualifiedIndex {
                schema: None,
                table: "main_vol".into(),
                index: "idxmain_vol".into(),
            },
            right_field: Some("vol_id".into()),
        };
        main_vol
    }

    fn main_other() -> IndexLink {
        let main_other = IndexLink {
            name: None,
            left_field: Some("other_id".into()),
            qualified_index: QualifiedIndex {
                schema: None,
                table: "main_other".into(),
                index: "idxmain_other".into(),
            },
            right_field: Some("ft_id".into()),
        };
        main_other
    }

    fn main_ft() -> IndexLink {
        let main_ft = IndexLink {
            name: None,
            left_field: Some("ft_id".into()),
            qualified_index: QualifiedIndex {
                schema: None,
                table: "main_ft".into(),
                index: "idxmain_ft".into(),
            },
            right_field: Some("ft_id".into()),
        };
        main_ft
    }

    fn main() -> IndexLink {
        let main = IndexLink {
            name: None,
            left_field: Some("id".into()),
            qualified_index: QualifiedIndex {
                schema: None,
                table: "main".into(),
                index: "idxmain".into(),
            },
            right_field: Some("id".into()),
        };
        main
    }

    fn setup() -> RelationshipManager {
        let mut irm = RelationshipManager::new();
        let main = main();
        let main_ft = main_ft();
        let main_other = main_other();
        let main_vol = main_vol();
        let docs = docs();
        let case_profile = case_profile();

        irm.add_relationship(&main, "id", &main_ft, "ft_id");
        irm.add_relationship(&main, "id", &main_other, "other_id");
        irm.add_relationship(&main, "id", &main_vol, "vol_id");
        irm.add_relationship(&main_other, "custodian", &docs, "custodian");
        irm.add_relationship(&docs, "case_profile", &case_profile, "case_profile");

        irm
    }

    #[test]
    fn test_main_ft_to_case_profile() {
        let mut irm = setup();
        let path = irm.calc_path(&main_ft(), &case_profile());
        eprintln!("{:#?}", path);
    }

    #[test]
    fn test_case_profile_to_main_ft() {
        let mut irm = setup();
        let path = irm.calc_path(&case_profile(), &main_ft());
        eprintln!("{:#?}", path);
    }

    #[test]
    fn test_main_to_main_ft() {
        let mut irm = setup();
        let path = irm.calc_path(&main(), &main_ft());
        eprintln!("{:#?}", path);
    }
}
