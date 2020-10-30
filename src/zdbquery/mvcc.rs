use crate::elasticsearch::Elasticsearch;
use crate::executor_manager::get_executor_manager;
use crate::gucs::ZDB_IGNORE_VISIBILITY;
use crate::zdbquery::{ZDBPreparedQuery, ZDBQuery, ZDBQueryClause};
use pgx::*;
use serde_json::json;

#[pg_extern]
fn internal_visibility_clause(index_relation: PgRelation) -> Json {
    Json(apply_visibility_clause(
        &Elasticsearch::new(&index_relation),
        ZDBQuery::default().prepare(&index_relation, None).0,
        true,
    ))
}

#[pg_extern]
fn wrap_with_visibility_clause(index_relation: PgRelation, query: ZDBQuery) -> ZDBQuery {
    let prepared = query.clone().prepare(&index_relation, None).0;
    let clause = apply_visibility_clause(&Elasticsearch::new(&index_relation), prepared, true);
    query.set_query_dsl(Some(ZDBQueryClause::opaque(clause)))
}

pub fn apply_visibility_clause(
    elasticsearch: &Elasticsearch,
    query: ZDBPreparedQuery,
    force: bool,
) -> serde_json::Value {
    if ZDB_IGNORE_VISIBILITY.get() && !force {
        // if we're configured to ignore visibility, then we simply return the
        // query_dsl of the provided query
        return query.take_query_dsl();
    }

    let dsl = query.query_dsl();
    let clause = build_visibility_clause(elasticsearch.index_name());
    json! {
        {
            "bool": {
                "must": [dsl],
                "filter": [clause]
            }
        }
    }
}

pub fn build_visibility_clause(index_name: &str) -> serde_json::Value {
    let snapshot: PgBox<pg_sys::SnapshotData> =
        PgBox::from_pg(unsafe { pg_sys::GetTransactionSnapshot() });

    let command_id = unsafe { pg_sys::GetCurrentCommandId(false) };
    let xmax = xid_to_64bit(snapshot.xmax);
    let used_xids = get_executor_manager().used_xids();
    let active_xids = {
        let xips = unsafe { std::slice::from_raw_parts(snapshot.xip, snapshot.xcnt as usize) };
        xips.iter()
            .map(|xid| xid_to_64bit(*xid))
            .collect::<Vec<u64>>()
    };

    json! {

        {
          "bool": {
            "must": [
              {
                "bool": {
                  "must_not": [
                    {
                      "query_string": {
                        "query": "_id:zdb_aborted_xids"
                      }
                    }
                  ]
                }
              },
              {
                "bool": {
                  "should": [
                    {
                      "bool": {
                        "must": [
                          {
                            "terms": {
                              "zdb_xmin": used_xids
                            }
                          },
                          {
                            "range": {
                              "zdb_cmin": {
                                "lt": command_id
                              }
                            }
                          },
                          {
                            "bool": {
                              "should": [
                                {
                                  "bool": {
                                    "must_not": [
                                      {
                                        "exists": {
                                          "field": "zdb_xmax"
                                        }
                                      }
                                    ]
                                  }
                                },
                                {
                                  "bool": {
                                    "must": [
                                      {
                                        "terms": {
                                          "zdb_xmax": used_xids
                                        }
                                      },
                                      {
                                        "range": {
                                          "zdb_cmax": {
                                            "gte": command_id
                                          }
                                        }
                                      }
                                    ]
                                  }
                                }
                              ]
                            }
                          }
                        ]
                      }
                    },
                    {
                      "bool": {
                        "must": [
                          {
                            "bool": {
                              "must": [
                                {
                                  "bool": {
                                    "must_not": [
                                      {
                                        "terms": {
                                          "zdb_xmin": {
                                            "index": index_name,
                                            "path": "zdb_aborted_xids",
                                            "id": "zdb_aborted_xids"
                                          }
                                        }
                                      }
                                    ]
                                  }
                                },
                                {
                                  "bool": {
                                    "must_not": [
                                      {
                                        "terms": {
                                          "zdb_xmin": active_xids
                                        }
                                      }
                                    ]
                                  }
                                },
                                {
                                  "bool": {
                                    "must_not": [
                                      {
                                        "range": {
                                          "zdb_xmin": {
                                            "gte": xmax
                                          }
                                        }
                                      }
                                    ]
                                  }
                                },
                                {
                                  "bool": {
                                    "should": [
                                      {
                                        "bool": {
                                          "must_not": [
                                            {
                                              "exists": {
                                                "field": "zdb_xmax"
                                              }
                                            }
                                          ]
                                        }
                                      },
                                      {
                                        "bool": {
                                          "must": [
                                            {
                                              "terms": {
                                                "zdb_xmax": used_xids
                                              }
                                            },
                                            {
                                              "range": {
                                                "zdb_cmax": {
                                                  "gte": command_id
                                                }
                                              }
                                            }
                                          ]
                                        }
                                      },
                                      {
                                        "bool": {
                                          "must": [
                                            {
                                              "bool": {
                                                "must_not": [
                                                  {
                                                    "terms": {
                                                      "zdb_xmax": used_xids
                                                    }
                                                  }
                                                ]
                                              }
                                            },
                                            {
                                              "bool": {
                                                "should": [
                                                  {
                                                    "terms": {
                                                      "zdb_xmax": {
                                                        "index": index_name,
                                                        "path": "zdb_aborted_xids",
                                                        "id": "zdb_aborted_xids"
                                                      }
                                                    }
                                                  },
                                                  {
                                                    "terms": {
                                                      "zdb_xmax": active_xids
                                                    }
                                                  },
                                                  {
                                                    "range": {
                                                      "zdb_xmax": {
                                                        "gte": xmax
                                                      }
                                                    }
                                                  }
                                                ]
                                              }
                                            }
                                          ]
                                        }
                                      }
                                    ]
                                  }
                                }
                              ]
                            }
                          }
                        ]
                      }
                    }
                  ]
                }
              }
            ]
          }
        }

    }
}
