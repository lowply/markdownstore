package markdownstore

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

type testCodec struct{}

type testMetadata struct {
	ID         string `json:"id"`
	Repository string `json:"repository"`
	Name       string `json:"name"`
	Summary    string `json:"summary"`
	Status     string `json:"status"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

func (testCodec) Parse(_ string, frontmatter, body []byte) (Record, error) {
	var metadata testMetadata
	if err := json.Unmarshal(frontmatter, &metadata); err != nil {
		return Record{}, err
	}
	return Record{
		ID: metadata.ID, Repository: metadata.Repository, Name: metadata.Name,
		Summary: metadata.Summary, Body: string(body), SearchText: string(body),
		Status: metadata.Status, CreatedAt: metadata.CreatedAt, UpdatedAt: metadata.UpdatedAt,
	}, nil
}

func (testCodec) Marshal(record Record) ([]byte, error) {
	frontmatter, err := json.Marshal(testMetadata{
		ID: record.ID, Repository: record.Repository, Name: record.Name,
		Summary: record.Summary, Status: record.Status,
		CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	})
	if err != nil {
		return nil, err
	}
	return JoinDocument(frontmatter, []byte(record.Body)), nil
}

func testRecord(id, name, summary, body string) Record {
	return Record{
		ID: id, Repository: "lowply/example", Name: name, Summary: summary,
		Body: body, SearchText: body, Status: "wip",
		CreatedAt: "2026-08-31T00:00:00Z", UpdatedAt: "2026-08-31T00:00:00Z",
	}
}

func newTestStore(t *testing.T) (*Store, Config) {
	t.Helper()
	root := t.TempDir()
	config := Config{
		Directory:    filepath.Join(root, "records"),
		DatabasePath: filepath.Join(root, "records.db"),
		Pattern:      "*.md", Statuses: []string{"wip", "done"}, Codec: testCodec{},
		EntityName: "record",
	}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store, config
}

func TestCreateFailsIfPathExists(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	first := testRecord("first-id", "first", "First", "body")
	if _, err := store.Create(path, first); err != nil {
		t.Fatal(err)
	}
	second := testRecord("second-id", "second", "Second", "replacement")
	if _, err := store.Create(path, second); !errors.Is(err, ErrPathExists) {
		t.Fatalf("error = %v, want ErrPathExists", err)
	}
	entry, err := store.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if entry.ID != first.ID || entry.Body != first.Body {
		t.Fatalf("existing record overwritten: %#v", entry)
	}
}

func TestConcurrentCreatesPublishOnlyOneRecord(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	start := make(chan struct{})
	var wait sync.WaitGroup
	errs := make(chan error, 2)
	for _, record := range []Record{
		testRecord("first-id", "first", "First", "first"),
		testRecord("second-id", "second", "Second", "second"),
	} {
		wait.Add(1)
		go func(record Record) {
			defer wait.Done()
			<-start
			_, err := store.Create(path, record)
			errs <- err
		}(record)
	}
	close(start)
	wait.Wait()
	close(errs)
	successes := 0
	existsErrors := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrPathExists):
			existsErrors++
		default:
			t.Fatalf("unexpected create error: %v", err)
		}
	}
	if successes != 1 || existsErrors != 1 {
		t.Fatalf("successes = %d, exists errors = %d", successes, existsErrors)
	}
}

func TestUpdateSerializesLibraryMutations(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	if _, err := store.Create(path, testRecord("record-id", "record", "Record", "0")); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.Update(path, func(record Record) (Record, error) {
				time.Sleep(20 * time.Millisecond)
				if record.Body == "0" {
					record.Body = "1"
				} else {
					record.Body = "2"
				}
				record.SearchText = record.Body
				return record, nil
			})
			if err != nil {
				t.Errorf("update: %v", err)
			}
		}()
	}
	close(start)
	wait.Wait()
	entry, err := store.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Body != "2" {
		t.Fatalf("body = %q, want serialized updates to produce 2", entry.Body)
	}
}

func TestReconcileTracksEditsRenamesAndDeletes(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	if _, err := store.Create(path, testRecord("record-id", "record", "Initial summary", "Initial body")); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(); err != nil {
		t.Fatal(err)
	}
	assertSearchPath(t, store, "Initial", path)

	updated := testRecord("record-id", "record", "Changed summary", "Changed body")
	if _, err := store.Update(path, func(Record) (Record, error) { return updated, nil }); err != nil {
		t.Fatal(err)
	}
	assertSearchPath(t, store, "Changed", path)

	renamed := filepath.Join(config.Directory, "renamed.md")
	if err := os.Rename(path, renamed); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(); err != nil {
		t.Fatal(err)
	}
	assertSearchPath(t, store, "Changed", renamed)

	if err := os.Remove(renamed); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Search(SearchQuery{Text: "Changed", Limit: 5}); err == nil {
		t.Fatal("deleted record remained searchable")
	}
}

func TestReconcileDirectoryUsesExplicitCanonicalDirectory(t *testing.T) {
	store, _ := newTestStore(t)
	directory := t.TempDir()
	path := filepath.Join(directory, "record.md")
	writeTestDocument(t, path, testRecord("explicit-id", "explicit", "Explicit directory", "body"))
	if err := store.ReconcileDirectory(directory); err != nil {
		t.Fatal(err)
	}
	assertSearchPath(t, store, "Explicit", path)
}

func TestReplaceIndexRecordsAcceptsParsedEntries(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	writeTestDocument(t, path, testRecord("indexed-id", "indexed", "Indexed directly", "body"))
	entry, err := store.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceIndexRecords([]Entry{entry}, nil); err != nil {
		t.Fatal(err)
	}
	assertSearchPath(t, store, "Indexed", path)
}

func TestReconcileRejectsMalformedAndDuplicateFilesWithoutMutation(t *testing.T) {
	store, config := newTestStore(t)
	original := filepath.Join(config.Directory, "original.md")
	if _, err := store.Create(original, testRecord("original", "original", "Original", "body")); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(config.Directory, "first.md")
	second := filepath.Join(config.Directory, "second.md")
	writeTestDocument(t, first, testRecord("duplicate", "first", "First", "body"))
	writeTestDocument(t, second, testRecord("duplicate", "second", "Second", "body"))
	bad := filepath.Join(config.Directory, "bad.md")
	if err := os.WriteFile(bad, []byte("not a document"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(); err == nil || !strings.Contains(err.Error(), bad) {
		t.Fatalf("malformed error = %v", err)
	}
	assertSearchPath(t, store, "Original", original)
	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(); err == nil ||
		!strings.Contains(err.Error(), first) || !strings.Contains(err.Error(), second) {
		t.Fatalf("duplicate error = %v", err)
	}
}

func TestSearchExactIDIncludesDefinedRank(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	if _, err := store.Create(path, testRecord("abc12345", "record", "Summary", "body")); err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(SearchQuery{Text: "abc12345", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].MatchReason != "id" || results[0].Rank != 0 {
		t.Fatalf("results = %#v", results)
	}
	encoded, err := json.Marshal(results[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"rank":0`)) {
		t.Fatalf("exact result omitted rank: %s", encoded)
	}
}

func TestOpenRejectsNewerSchemaBeforePersistentChanges(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "records.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
		INSERT INTO schema_migrations(version, applied_at) VALUES(999, '2026-08-31T00:00:00Z');
		PRAGMA journal_mode = DELETE`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = Open(Config{
		Directory: filepath.Join(root, "records"), DatabasePath: path,
		Pattern: "*.md", Statuses: []string{"wip"}, Codec: testCodec{},
	})
	if err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("error = %v", err)
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("mode changed to %o before rejection", info.Mode().Perm())
	}
	database, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var journal string
	if err := database.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if strings.EqualFold(journal, "wal") {
		t.Fatalf("journal mode changed before rejection: %s", journal)
	}
}

func TestOpenMigratesLegacyProjectDoneStatusToClosed(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "project.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
		INSERT INTO schema_migrations(version, applied_at) VALUES(1, '2026-08-31T00:00:00Z');
		CREATE TABLE projects (
			path TEXT PRIMARY KEY, file_size INTEGER NOT NULL, mod_time_ns INTEGER NOT NULL,
			id TEXT NOT NULL UNIQUE, repository TEXT NOT NULL, name TEXT NOT NULL,
			summary TEXT NOT NULL, body TEXT NOT NULL, status TEXT NOT NULL,
			created_at TEXT NOT NULL, updated_at TEXT NOT NULL
		);
		INSERT INTO projects VALUES(
			'/tmp/legacy.md', 1, 1, 'abc12345', '', 'legacy', 'Legacy',
			'body', 'done', '2026-08-31T00:00:00Z', '2026-08-31T00:00:00Z'
		)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(Config{
		Directory: filepath.Join(root, "projects"), DatabasePath: path,
		Pattern: "*.md", Statuses: []string{"wip", "closed"},
		Codec: testCodec{}, EntityName: "project",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	results, err := store.Search(SearchQuery{Text: "abc12345", Status: "closed", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Status != "closed" {
		t.Fatalf("results = %#v", results)
	}
}

func assertSearchPath(t *testing.T, store *Store, query, path string) {
	t.Helper()
	results, err := store.Search(SearchQuery{Text: query, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Path != path {
		t.Fatalf("results = %#v, want path %s", results, path)
	}
}

func writeTestDocument(t *testing.T, path string, record Record) {
	t.Helper()
	data, err := (testCodec{}).Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveRejectsReplacedRecord(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	entry, err := store.Create(path, testRecord("record-id", "record", "Record", "body"))
	if err != nil {
		t.Fatal(err)
	}
	replacement := testRecord("replacement-id", "replacement", "Replacement", "body")
	writeTestDocument(t, path, replacement)
	if err := store.Remove(path, entry.Fingerprint); !errors.Is(err, ErrChanged) {
		t.Fatalf("error = %v, want ErrChanged", err)
	}
	got, err := store.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != replacement.ID {
		t.Fatalf("replacement removed: %#v", got)
	}
}

func TestValidateConfigRequiresResolvedPathsAndCodec(t *testing.T) {
	_, err := Open(Config{})
	if err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("error = %v", err)
	}
}

func TestReadFileParsesWithoutOpeningDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.md")
	writeTestDocument(t, path, testRecord("read-id", "read", "Read file", "body"))
	entry, err := ReadFile(path, testCodec{}, []string{"wip", "done"}, "record")
	if err != nil {
		t.Fatal(err)
	}
	if entry.ID != "read-id" || entry.Path != path {
		t.Fatalf("entry = %#v", entry)
	}
}

func TestWriteFileAtomicReplacesCanonicalBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.md")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(path, []byte("new")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new" {
		t.Fatalf("data = %q", data)
	}
}

func TestCreateFileAtomicDoesNotReplaceExistingBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.md")
	if err := CreateFileAtomic(path, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := CreateFileAtomic(path, []byte("second")); !errors.Is(err, ErrPathExists) {
		t.Fatalf("error = %v, want ErrPathExists", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "first" {
		t.Fatalf("data = %q", data)
	}
}

func TestCreateRejectsDuplicateIDWithoutPublishingFile(t *testing.T) {
	store, config := newTestStore(t)
	firstPath := filepath.Join(config.Directory, "first.md")
	secondPath := filepath.Join(config.Directory, "second.md")
	if _, err := store.Create(firstPath, testRecord("same-id", "first", "First", "body")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(secondPath, testRecord("same-id", "second", "Second", "body")); !errors.Is(err, ErrIDExists) {
		t.Fatalf("error = %v, want ErrIDExists", err)
	}
	if _, err := os.Stat(secondPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("duplicate canonical file was published: %v", err)
	}
}

func TestUpdateRejectsIDChangeWithoutReplacingFile(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	original, err := store.Create(path, testRecord("original-id", "record", "Record", "body"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Update(path, func(record Record) (Record, error) {
		record.ID = "changed-id"
		record.Summary = "Changed"
		return record, nil
	})
	if !errors.Is(err, ErrIDChanged) {
		t.Fatalf("error = %v, want ErrIDChanged", err)
	}
	current, err := store.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if current.ID != original.ID || current.Summary != original.Summary {
		t.Fatalf("canonical record changed: %#v", current)
	}
}

func ExampleSearchQuery() {
	fmt.Println(SearchQuery{Text: "sqlite index", Status: "wip", Limit: 5}.Text)
	// Output: sqlite index
}
