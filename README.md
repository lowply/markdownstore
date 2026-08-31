# markdownstore

`markdownstore` is a Go library for applications that keep canonical records
as Markdown files and use SQLite FTS5 as a disposable search index.

Consumers provide a codec and resolved storage paths:

```go
store, err := markdownstore.Open(markdownstore.Config{
    Directory:    projectDirectory,
    DatabasePath: databasePath,
    Pattern:      "*.md",
    Statuses:     []string{"wip", "closed"},
    Codec:        codec,
    EntityName:   "project",
})
```

The library owns:

- stable reads that retry atomic file replacement;
- atomic fail-if-exists creation;
- serialized library mutations through a per-directory file lock;
- compare-before-replace updates and removals;
- reconciliation of direct additions, edits, renames, and deletions;
- duplicate-ID detection before index mutation;
- SQLite schema migration, FTS5 indexing, exact-ID lookup, ranked search, and list ordering.

Markdown remains canonical. The SQLite database can be deleted and rebuilt by
calling `Reconcile`.

The library does not read application-specific environment variables.
Applications resolve defaults and overrides before constructing `Config`.

## Development

```bash
go test ./...
go build ./...
```
