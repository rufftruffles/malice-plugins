package elasticsearch

// ES 8 has no document types: the mapping is flat under "mappings.properties".
// The document shape (file.*, plugins.<category>.<name>, scan_date) is unchanged
// from the v7 "samples" type mapping; engines and Kibana dashboards depend on it.
const mapping = `{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "index.mapping.total_fields.limit": 10000
  },
  "mappings": {
    "properties": {
      "file": {
        "properties": {
          "md5": {
            "type": "keyword"
          },
          "mime": {
            "type": "keyword"
          },
          "name": {
            "type": "keyword"
          },
          "path": {
            "type": "text"
          },
          "sha1": {
            "type": "keyword"
          },
          "sha256": {
            "type": "keyword"
          },
          "sha512": {
            "type": "keyword"
          },
          "size": {
            "type": "keyword"
          }
        }
      },
      "plugins": {
        "properties": {
          "archive": {
            "properties": {}
          },
          "av": {
            "properties": {}
          },
          "document": {
            "properties": {}
          },
          "exe": {
            "properties": {}
          },
          "intel": {
            "properties": {
              "virustotal": {
                "dynamic": false,
                "properties": {}
              }
            }
          },
          "metadata": {
            "properties": {}
          }
        }
      },
      "scan_date": {
        "type": "date"
      }
    }
  }
}`
