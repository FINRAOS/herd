INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.searchable.fields.ngrams', '{
  "columns.description.ngrams": "5.0",
  "columns.name.ngrams": "20.0",
  "description.ngrams": "5.0",
  "descriptiveBusinessObjectFormat.schemaColumns.name.ngrams": "20.0",
  "displayName.ngrams": "50.0",
  "name.ngrams": "25.0",
  "name.keyword": "30000.0",
  "namespace.code.ngrams": "20.0",
  "subjectMatterExperts.userId.ngrams": "1.0"
}', NULL);


INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.searchable.fields.shingles', '{
  "columns.name.shingles": "30.0",
  "descriptiveBusinessObjectFormat.schemaColumns.name.shingles": "30.0",
  "displayName.shingles": "500.0",
  "name.shingles": "500.0",
  "namespace.code.shingles": "300.0",
  "subjectMatterExperts.userId.shingles": "1.0"
}', NULL);


INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.searchable.fields.stemmed', '{
  "columns.description.stemmed": "15.0",
  "columns.name.stemmed": "30.0",
  "description.stemmed": "10.0",
  "descriptiveBusinessObjectFormat.schemaColumns.name.stemmed": "30.0",
  "displayName.stemmed": "50.0",
  "name.stemmed": "50.0",
  "namespace.code.stemmed": "30.0",
  "subjectMatterExperts.userId.stemmed": "1.0"
}', NULL);


INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.column.match.highlight.fields', '{
  "highlightFields": [
    {
      "fieldName": "columns.name",
      "fragmentSize": 100,
      "matchedFields": [
        "columns.name",
        "columns.name.stemmed",
        "columns.name.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "descriptiveBusinessObjectFormat.schemaColumns.name",
      "fragmentSize": 100,
      "matchedFields": [
        "descriptiveBusinessObjectFormat.schemaColumns.name",
        "descriptiveBusinessObjectFormat.schemaColumns.name.stemmed",
        "descriptiveBusinessObjectFormat.schemaColumns.name.ngrams"
      ],
      "numOfFragments": 5
    }
  ]
}', NULL);


INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.highlight.fields', '{
  "highlightFields": [
    {
      "fieldName": "businessObjectDefinitionTags.tag.displayName",
      "fragmentSize": 100,
      "matchedFields": [
        "businessObjectDefinitionTags.tag.displayName",
        "businessObjectDefinitionTags.tag.displayName.stemmed",
        "businessObjectDefinitionTags.tag.displayName.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "columns.description",
      "fragmentSize": 100,
      "matchedFields": [
        "columns.description",
        "columns.description.stemmed",
        "columns.description.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "columns.name",
      "fragmentSize": 100,
      "matchedFields": [
        "columns.name",
        "columns.name.stemmed",
        "columns.name.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "description",
      "fragmentSize": 100,
      "matchedFields": [
        "description",
        "description.stemmed",
        "description.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "descriptiveBusinessObjectFormat.schemaColumns.name",
      "fragmentSize": 100,
      "matchedFields": [
        "descriptiveBusinessObjectFormat.schemaColumns.name",
        "descriptiveBusinessObjectFormat.schemaColumns.name.stemmed",
        "descriptiveBusinessObjectFormat.schemaColumns.name.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "displayName",
      "fragmentSize": 100,
      "matchedFields": [
        "displayName",
        "displayName.stemmed",
        "displayName.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "name",
      "fragmentSize": 100,
      "matchedFields": [
        "name",
        "name.stemmed",
        "name.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "namespace.code",
      "fragmentSize": 100,
      "matchedFields": [
        "namespace.code",
        "namespace.code.stemmed",
        "namespace.code.ngrams"
      ],
      "numOfFragments": 5
    },
    {
      "fieldName": "subjectMatterExperts.userId",
      "fragmentSize": 100,
      "matchedFields": [
        "subjectMatterExperts.userId",
        "subjectMatterExperts.userId.stemmed",
        "subjectMatterExperts.userId.ngrams"
      ],
      "numOfFragments": 5
    }
  ]
}', NULL);


INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.bdef.settings.json', NULL, '{
  "index.mapper.dynamic": false,
  "analysis": {
    "filter": {
      "field_ngram_filter": {
        "type": "edgeNGram",
        "min_gram": 4,
        "max_gram": 16,
        "side": "front"
      }
    },
    "analyzer": {
      "field_default_analyzer": {
        "type": "custom",
        "tokenizer": "classic",
        "filter": [
          "lowercase",
          "asciifolding",
          "stop"
        ]
      },
      "field_shingle_analyzer": {
        "type": "custom",
        "tokenizer": "classic",
        "filter": [
          "lowercase",
          "asciifolding",
          "shingle",
          "stop"
        ]
      },
      "field_snowball_analyzer": {
        "type": "custom",
        "tokenizer": "classic",
        "filter": [
          "lowercase",
          "asciifolding",
          "snowball",
          "stop"
        ]
      },
      "field_ngram_analyzer": {
        "type": "custom",
        "tokenizer": "classic",
        "filter": [
          "stop",
          "lowercase",
          "asciifolding",
          "field_ngram_filter"
        ]
      }
    }
  }
}');


INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.tag.settings.json', NULL, '{
  "index.mapper.dynamic": false,
  "analysis": {
    "filter": {
      "field_ngram_filter": {
        "type": "edgeNGram",
        "min_gram": 4,
        "max_gram": 16,
        "side": "front"
      }
    },
    "analyzer": {
      "field_default_analyzer": {
        "type": "custom",
        "tokenizer": "classic",
        "filter": [
          "lowercase",
          "asciifolding",
          "stop"
        ]
      },
      "field_shingle_analyzer": {
        "type": "custom",
        "tokenizer": "classic",
        "filter": [
          "lowercase",
          "asciifolding",
          "shingle",
          "stop"
        ]
      },
      "field_snowball_analyzer": {
        "type": "custom",
        "tokenizer": "classic",
        "filter": [
          "lowercase",
          "asciifolding",
          "snowball",
          "stop"
        ]
      },
      "field_ngram_analyzer": {
        "type": "custom",
        "tokenizer": "classic",
        "filter": [
          "stop",
          "lowercase",
          "asciifolding",
          "field_ngram_filter"
        ]
      }
    }
  }
}');


INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.bdef.mappings.json', NULL, '{
  "properties": {
    "attributes": {
      "properties": {
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "store": true,
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "store": true,
          "type": "date"
        },
        "id": {
          "type": "long"
        },
        "name": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "store": true,
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "store": true,
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "store": true,
          "type": "date"
        },
        "value": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "type": "text"
        }
      }
    },
    "businessObjectDefinitionTags": {
      "properties": {
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "id": {
          "type": "long"
        },
        "tag": {
          "include_in_parent": true,
          "properties": {
            "childrenTagEntities": {
              "properties": {
                "childrenTagEntities": {
                  "properties": {
                    "createdBy": {
                      "fields": {
                        "keyword": {
                          "ignore_above": 256,
                          "type": "keyword"
                        }
                      },
                      "type": "text"
                    },
                    "createdOn": {
                      "ignore_malformed": true,
                      "include_in_all": false,
                      "type": "date"
                    },
                    "description": {
                      "fields": {
                        "keyword": {
                          "type": "keyword"
                        },
                        "ngrams": {
                          "analyzer": "field_ngram_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "shingles": {
                          "analyzer": "field_shingle_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "stemmed": {
                          "analyzer": "field_snowball_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        }
                      },
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "displayName": {
                      "fields": {
                        "keyword": {
                          "type": "keyword"
                        },
                        "ngrams": {
                          "analyzer": "field_ngram_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "shingles": {
                          "analyzer": "field_shingle_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "stemmed": {
                          "analyzer": "field_snowball_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        }
                      },
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "id": {
                      "type": "long"
                    },
                    "tagCode": {
                      "fields": {
                        "keyword": {
                          "ignore_above": 256,
                          "type": "keyword"
                        },
                        "ngrams": {
                          "analyzer": "field_ngram_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "shingles": {
                          "analyzer": "field_shingle_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "stemmed": {
                          "analyzer": "field_snowball_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        }
                      },
                      "type": "text"
                    },
                    "tagType": {
                      "properties": {
                        "code": {
                          "fields": {
                            "keyword": {
                              "ignore_above": 256,
                              "type": "keyword"
                            },
                            "ngrams": {
                              "analyzer": "field_ngram_analyzer",
                              "term_vector": "with_positions_offsets",
                              "type": "text"
                            },
                            "shingles": {
                              "analyzer": "field_shingle_analyzer",
                              "term_vector": "with_positions_offsets",
                              "type": "text"
                            },
                            "stemmed": {
                              "analyzer": "field_snowball_analyzer",
                              "term_vector": "with_positions_offsets",
                              "type": "text"
                            }
                          },
                          "type": "text"
                        },
                        "createdBy": {
                          "fields": {
                            "keyword": {
                              "ignore_above": 256,
                              "type": "keyword"
                            }
                          },
                          "type": "text"
                        },
                        "createdOn": {
                          "ignore_malformed": true,
                          "include_in_all": false,
                          "type": "date"
                        },
                        "displayName": {
                          "fields": {
                            "keyword": {
                              "type": "keyword"
                            },
                            "ngrams": {
                              "analyzer": "field_ngram_analyzer",
                              "term_vector": "with_positions_offsets",
                              "type": "text"
                            },
                            "shingles": {
                              "analyzer": "field_shingle_analyzer",
                              "term_vector": "with_positions_offsets",
                              "type": "text"
                            },
                            "stemmed": {
                              "analyzer": "field_snowball_analyzer",
                              "term_vector": "with_positions_offsets",
                              "type": "text"
                            }
                          },
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "orderNumber": {
                          "type": "long"
                        },
                        "updatedBy": {
                          "fields": {
                            "keyword": {
                              "ignore_above": 256,
                              "type": "keyword"
                            }
                          },
                          "type": "text"
                        },
                        "updatedOn": {
                          "ignore_malformed": true,
                          "include_in_all": false,
                          "type": "date"
                        }
                      }
                    },
                    "updatedBy": {
                      "fields": {
                        "keyword": {
                          "ignore_above": 256,
                          "type": "keyword"
                        }
                      },
                      "type": "text"
                    },
                    "updatedOn": {
                      "ignore_malformed": true,
                      "include_in_all": false,
                      "type": "date"
                    }
                  }
                },
                "createdBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "createdOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                },
                "description": {
                  "fields": {
                    "keyword": {
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "displayName": {
                  "fields": {
                    "keyword": {
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "id": {
                  "type": "long"
                },
                "tagCode": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "type": "text"
                },
                "tagType": {
                  "properties": {
                    "code": {
                      "fields": {
                        "keyword": {
                          "ignore_above": 256,
                          "type": "keyword"
                        },
                        "ngrams": {
                          "analyzer": "field_ngram_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "shingles": {
                          "analyzer": "field_shingle_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "stemmed": {
                          "analyzer": "field_snowball_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        }
                      },
                      "type": "text"
                    },
                    "createdBy": {
                      "fields": {
                        "keyword": {
                          "ignore_above": 256,
                          "type": "keyword"
                        }
                      },
                      "type": "text"
                    },
                    "createdOn": {
                      "ignore_malformed": true,
                      "include_in_all": false,
                      "type": "date"
                    },
                    "displayName": {
                      "fields": {
                        "keyword": {
                          "type": "keyword"
                        },
                        "ngrams": {
                          "analyzer": "field_ngram_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "shingles": {
                          "analyzer": "field_shingle_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        },
                        "stemmed": {
                          "analyzer": "field_snowball_analyzer",
                          "term_vector": "with_positions_offsets",
                          "type": "text"
                        }
                      },
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "orderNumber": {
                      "type": "long"
                    },
                    "updatedBy": {
                      "fields": {
                        "keyword": {
                          "ignore_above": 256,
                          "type": "keyword"
                        }
                      },
                      "type": "text"
                    },
                    "updatedOn": {
                      "ignore_malformed": true,
                      "include_in_all": false,
                      "type": "date"
                    }
                  }
                },
                "updatedBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "updatedOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                }
              }
            },
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "description": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "displayName": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "id": {
              "type": "long"
            },
            "tagCode": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "type": "text"
            },
            "tagType": {
              "properties": {
                "code": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "type": "text"
                },
                "createdBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "createdOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                },
                "displayName": {
                  "fields": {
                    "keyword": {
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "orderNumber": {
                  "type": "long"
                },
                "updatedBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "updatedOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                }
              }
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          },
          "type": "nested"
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        }
      }
    },
    "businessObjectFormats": {
      "properties": {
        "attributeDefinitions": {
          "properties": {
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "id": {
              "type": "long"
            },
            "name": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "publish": {
              "type": "boolean"
            },
            "required": {
              "type": "boolean"
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          }
        },
        "attributes": {
          "properties": {
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "id": {
              "type": "long"
            },
            "name": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "value": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "type": "text"
            }
          }
        },
        "businessObjectFormatVersion": {
          "type": "long"
        },
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "delimiter": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "description": {
          "fields": {
            "keyword": {
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "escapeCharacter": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "fileType": {
          "properties": {
            "code": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "type": "text"
            },
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "description": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          }
        },
        "id": {
          "type": "long"
        },
        "latestVersion": {
          "type": "boolean"
        },
        "nullValue": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "partitionKey": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "partitionKeyGroup": {
          "properties": {
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "expectedPartitionValues": {
              "properties": {
                "createdBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "createdOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                },
                "id": {
                  "type": "long"
                },
                "partitionValue": {
                  "ignore_malformed": true,
                  "type": "date"
                },
                "updatedBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "updatedOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                }
              }
            },
            "partitionKeyGroupName": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          }
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "usage": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        }
      }
    },
    "columns": {
      "properties": {
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "description": {
          "fields": {
            "keyword": {
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "id": {
          "type": "long"
        },
        "name": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        }
      }
    },
    "createdBy": {
      "fields": {
        "keyword": {
          "ignore_above": 256,
          "type": "keyword"
        }
      },
      "type": "text"
    },
    "createdOn": {
      "ignore_malformed": true,
      "include_in_all": false,
      "type": "date"
    },
    "dataProvider": {
      "properties": {
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "name": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "type": "text"
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        }
      }
    },
    "description": {
      "fields": {
        "keyword": {
          "type": "keyword"
        },
        "ngrams": {
          "analyzer": "field_ngram_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "shingles": {
          "analyzer": "field_shingle_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "stemmed": {
          "analyzer": "field_snowball_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        }
      },
      "term_vector": "with_positions_offsets",
      "type": "text"
    },
    "descriptiveBusinessObjectFormat": {
      "properties": {
        "businessObjectFormatVersion": {
          "type": "long"
        },
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "delimiter": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "description": {
          "fields": {
            "keyword": {
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "escapeCharacter": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "fileType": {
          "properties": {
            "code": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "type": "text"
            },
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "description": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          }
        },
        "id": {
          "type": "long"
        },
        "latestVersion": {
          "type": "boolean"
        },
        "nullValue": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "partitionKey": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "partitionKeyGroup": {
          "properties": {
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "expectedPartitionValues": {
              "properties": {
                "createdBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "createdOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                },
                "id": {
                  "type": "long"
                },
                "partitionValue": {
                  "ignore_malformed": true,
                  "type": "date"
                },
                "updatedBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "updatedOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                }
              }
            },
            "partitionKeyGroupName": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          }
        },
        "schemaColumns": {
          "properties": {
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "description": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "name": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "size": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "type": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            }
          }
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "usage": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "type": "text"
        }
      }
    },
    "displayName": {
      "fields": {
        "keyword": {
          "type": "keyword"
        },
        "ngrams": {
          "analyzer": "field_ngram_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "shingles": {
          "analyzer": "field_shingle_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "stemmed": {
          "analyzer": "field_snowball_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        }
      },
      "term_vector": "with_positions_offsets",
      "type": "text"
    },
    "id": {
      "type": "long"
    },
    "name": {
      "fields": {
        "keyword": {
          "ignore_above": 256,
          "type": "keyword"
        },
        "ngrams": {
          "analyzer": "field_ngram_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "shingles": {
          "analyzer": "field_shingle_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "stemmed": {
          "analyzer": "field_snowball_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        }
      },
      "term_vector": "with_positions_offsets",
      "type": "text"
    },
    "namespace": {
      "properties": {
        "code": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        }
      }
    },
    "sampleDataFiles": {
      "properties": {
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "directoryPath": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "type": "text"
        },
        "fileName": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "type": "text"
        },
        "fileSizeBytes": {
          "type": "long"
        },
        "id": {
          "type": "long"
        },
        "storage": {
          "properties": {
            "attributes": {
              "properties": {
                "createdBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "createdOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                },
                "id": {
                  "type": "long"
                },
                "name": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "updatedBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "updatedOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                },
                "value": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "type": "text"
                }
              }
            },
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "name": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "storagePlatform": {
              "properties": {
                "createdBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "createdOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                },
                "name": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "updatedBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "updatedOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                }
              }
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          }
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        }
      }
    },
    "subjectMatterExperts": {
      "properties": {
        "userId": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        }
      }
    },
    "updatedBy": {
      "fields": {
        "keyword": {
          "ignore_above": 256,
          "type": "keyword"
        }
      },
      "type": "text"
    },
    "updatedOn": {
      "ignore_malformed": true,
      "include_in_all": false,
      "type": "date"
    }
  }
}');


INSERT INTO cnfgn (cnfgn_key_nm, cnfgn_value_ds, cnfgn_value_cl)
VALUES ('elasticsearch.tag.mappings.json', NULL, '{
  "properties": {
    "childrenTagEntities": {
      "properties": {
        "childrenTagEntities": {
          "properties": {
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "description": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "displayName": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "id": {
              "type": "long"
            },
            "tagCode": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "type": "text"
            },
            "tagType": {
              "properties": {
                "code": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "type": "text"
                },
                "createdBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "createdOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                },
                "displayName": {
                  "fields": {
                    "keyword": {
                      "type": "keyword"
                    },
                    "ngrams": {
                      "analyzer": "field_ngram_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "shingles": {
                      "analyzer": "field_shingle_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    },
                    "stemmed": {
                      "analyzer": "field_snowball_analyzer",
                      "term_vector": "with_positions_offsets",
                      "type": "text"
                    }
                  },
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "orderNumber": {
                  "type": "long"
                },
                "updatedBy": {
                  "fields": {
                    "keyword": {
                      "ignore_above": 256,
                      "type": "keyword"
                    }
                  },
                  "type": "text"
                },
                "updatedOn": {
                  "ignore_malformed": true,
                  "include_in_all": false,
                  "type": "date"
                }
              }
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          }
        },
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "description": {
          "fields": {
            "keyword": {
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "displayName": {
          "fields": {
            "keyword": {
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "id": {
          "type": "long"
        },
        "tagCode": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "type": "text"
        },
        "tagType": {
          "properties": {
            "code": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "type": "text"
            },
            "createdBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "createdOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            },
            "displayName": {
              "fields": {
                "keyword": {
                  "type": "keyword"
                },
                "ngrams": {
                  "analyzer": "field_ngram_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "shingles": {
                  "analyzer": "field_shingle_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                },
                "stemmed": {
                  "analyzer": "field_snowball_analyzer",
                  "term_vector": "with_positions_offsets",
                  "type": "text"
                }
              },
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "orderNumber": {
              "type": "long"
            },
            "updatedBy": {
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              },
              "type": "text"
            },
            "updatedOn": {
              "ignore_malformed": true,
              "include_in_all": false,
              "type": "date"
            }
          }
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        }
      }
    },
    "createdBy": {
      "fields": {
        "keyword": {
          "ignore_above": 256,
          "type": "keyword"
        }
      },
      "type": "text"
    },
    "createdOn": {
      "ignore_malformed": true,
      "include_in_all": false,
      "type": "date"
    },
    "description": {
      "fields": {
        "keyword": {
          "type": "keyword"
        },
        "ngrams": {
          "analyzer": "field_ngram_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "shingles": {
          "analyzer": "field_shingle_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "stemmed": {
          "analyzer": "field_snowball_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        }
      },
      "term_vector": "with_positions_offsets",
      "type": "text"
    },
    "displayName": {
      "fields": {
        "keyword": {
          "type": "keyword"
        },
        "ngrams": {
          "analyzer": "field_ngram_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "shingles": {
          "analyzer": "field_shingle_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "stemmed": {
          "analyzer": "field_snowball_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        }
      },
      "term_vector": "with_positions_offsets",
      "type": "text"
    },
    "id": {
      "type": "long"
    },
    "tagCode": {
      "fields": {
        "keyword": {
          "ignore_above": 256,
          "type": "keyword"
        },
        "ngrams": {
          "analyzer": "field_ngram_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "shingles": {
          "analyzer": "field_shingle_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "stemmed": {
          "analyzer": "field_snowball_analyzer",
          "term_vector": "with_positions_offsets",
          "type": "text"
        }
      },
      "type": "text"
    },
    "tagType": {
      "properties": {
        "code": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "type": "text"
        },
        "createdBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "createdOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        },
        "displayName": {
          "fields": {
            "keyword": {
              "type": "keyword"
            },
            "ngrams": {
              "analyzer": "field_ngram_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "shingles": {
              "analyzer": "field_shingle_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            },
            "stemmed": {
              "analyzer": "field_snowball_analyzer",
              "term_vector": "with_positions_offsets",
              "type": "text"
            }
          },
          "term_vector": "with_positions_offsets",
          "type": "text"
        },
        "orderNumber": {
          "type": "long"
        },
        "updatedBy": {
          "fields": {
            "keyword": {
              "ignore_above": 256,
              "type": "keyword"
            }
          },
          "type": "text"
        },
        "updatedOn": {
          "ignore_malformed": true,
          "include_in_all": false,
          "type": "date"
        }
      }
    },
    "updatedBy": {
      "fields": {
        "keyword": {
          "ignore_above": 256,
          "type": "keyword"
        }
      },
      "type": "text"
    },
    "updatedOn": {
      "ignore_malformed": true,
      "include_in_all": false,
      "type": "date"
    }
  }
}');