"""GraphQL queries for Shopify API."""

simple_query = """query {
    __query_name__ {
        __selected_fields__
    }
}"""

simple_query_incremental = """query tapShopify($id: ID!) {
    __query_name__(id: $id) {
        __selected_fields__
    }
}"""

query_incremental = """query tapShopify($first: Int, $after: String, $filter: String) {
    __query_name__(first: $first, after: $after, query: $filter__additional_args__) {
        edges {
            node {
                __selected_fields__
            }
        },
        pageInfo {
            hasNextPage
            endCursor
        }
    }
}"""

bulk_query = '''
mutation {
bulkOperationRunQuery(
    query:"""
        {
            __query_name____filters__ {
                edges {
                    node {
                        __selected_fields__
                    }
                }
            }
        }
    """
)
    {
        bulkOperation {
            id
            status
        }
        userErrors {
            field
            message
        }
    }
}
'''

bulk_query_status = """
query {
    currentBulkOperation {
        id
        status
        errorCode
        createdAt
        completedAt
        objectCount
        fileSize
        url
        partialDataUrl
    }
}
"""

schema_query = """query IntrospectionQuery {
  __schema {
    queryType {
      name
    }
    types {
      ...FullType
    }
  }
}

fragment FullType on __Type {
  kind
  name
  description
  fields(includeDeprecated: true) {
    name
    description
    args {
      ...InputValue
    }
    type {
      ...TypeRef
    }
    isDeprecated
    deprecationReason
  }
  inputFields {
    ...InputValue
  }
  interfaces {
    ...TypeRef
  }
  enumValues(includeDeprecated: true) {
    name
    description
    isDeprecated
    deprecationReason
  }
  possibleTypes {
    ...TypeRef
  }
}

fragment InputValue on __InputValue {
  name
  description
  type {
    ...TypeRef
  }
  defaultValue
}

fragment TypeRef on __Type {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
    }
  }
}"""


queries_query = """{
  __schema {
    queryType {
      fields {
        name
        type {
          name
          ofType {
            name
            fields {
              name
              type {
                ofType {
                  name
                  kind
                  ofType {
                    name
                    ofType {
                      name
                    }
                    kind
                  }
                }
              }
            }
          }
        }
        args {
          name
        }
      }
    }
  }
}"""
