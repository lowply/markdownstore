# markdownstore

`markdownstore` is a Go library for applications that keep canonical records
as Markdown files and use SQLite FTS5 as a disposable search index.

Consumers provide a codec, resolved storage paths, a metadata schema, and
weighted full-text search slots:

```go
store, err := markdownstore.Open(markdownstore.Config{
    Directory:     projectDirectory,
    DatabasePath:  databasePath,
    Pattern:       "*.md",
    EntityName:    "project",
    SchemaID:      "project/1",
    Fields: []markdownstore.MetadataField{
        {Name: "period", Required: true},
        {Name: "status", Required: true},
        {Name: "summary", Required: true},
    },
    SearchWeights: []float64{0.5, 1, 2, 1},
    Codec:         codec,
})
```

The codec maps application Markdown to a generic document:

```go
type Document struct {
    ID          string
    Metadata    map[string]string
    Body        string
    SortKey     string
    SearchSlots []string
}
```

The library owns:

- stable reads that retry atomic file replacement;
- atomic fail-if-exists creation;
- serialized library mutations through a per-directory file lock;
- compare-before-replace updates and removals;
- reconciliation of direct additions, edits, renames, and deletions;
- duplicate-ID detection before index mutation;
- normalized metadata indexing and exact metadata filters;
- weighted FTS5 indexing, exact-ID lookup, ranked search, and list ordering;
- clean replacement of a disposable database when its configuration
  fingerprint does not match.

Markdown remains canonical. The SQLite database can be deleted and rebuilt by
calling `Reconcile`.

Metadata keys and values are application-defined scalar strings. Filters may
only use keys declared in `Fields`. The number of `SearchSlots` returned by the
codec must match `SearchWeights`. Consumers must change `SchemaID` when
metadata or validation semantics change.

The library does not read application-specific environment variables.
Applications resolve defaults and overrides before constructing `Config`.

## Development

```bash
go test ./...
go build ./...
```
