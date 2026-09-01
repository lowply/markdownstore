package markdownstore

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

type testCodec struct{}

type testFrontmatter struct {
	ID       string            `json:"id"`
	Metadata map[string]string `json:"metadata"`
}

func (testCodec) Parse(_ string, frontmatter, body []byte) (Document, error) {
	var parsed testFrontmatter
	if err := json.Unmarshal(frontmatter, &parsed); err != nil {
		return Document{}, err
	}
	metadata := parsed.Metadata
	return Document{
		ID: parsed.ID, Metadata: metadata, Body: string(body),
		SortKey: metadata["created_at"],
		SearchSlots: []string{
			metadata["repository"], metadata["name"], metadata["summary"], string(body),
		},
	}, nil
}

func (testCodec) Marshal(document Document) ([]byte, error) {
	frontmatter, err := json.Marshal(testFrontmatter{
		ID: document.ID, Metadata: document.Metadata,
	})
	if err != nil {
		return nil, err
	}
	return JoinDocument(frontmatter, []byte(document.Body)), nil
}

func testConfig(root string) Config {
	periodPattern := regexp.MustCompile(`\AFY[0-9]{2}H[12]\z`)
	return Config{
		Directory:    filepath.Join(root, "records"),
		DatabasePath: filepath.Join(root, "records.db"),
		Pattern:      "*.md", EntityName: "record", SchemaID: "test-record/1",
		Fields: []MetadataField{
			{Name: "period", Required: true, Validate: func(value string) error {
				if !periodPattern.MatchString(value) {
					return fmt.Errorf("invalid period %q", value)
				}
				return nil
			}},
			{Name: "repository"},
			{Name: "name", Required: true},
			{Name: "summary", Required: true},
			{Name: "status", Required: true},
			{Name: "created_at", Required: true},
		},
		SearchWeights: []float64{0.5, 1.0, 2.0, 1.0},
		Codec:         testCodec{},
	}
}

func testDocument(id, name, summary, body string) Document {
	metadata := map[string]string{
		"period": "FY27H1", "repository": "lowply/example", "name": name,
		"summary": summary, "status": "wip", "created_at": "2026-08-31T00:00:00Z",
	}
	return Document{
		ID: id, Metadata: metadata, Body: body, SortKey: metadata["created_at"],
		SearchSlots: []string{metadata["repository"], name, summary, body},
	}
}

func newTestStore(t *testing.T) (*Store, Config) {
	t.Helper()
	config := testConfig(t.TempDir())
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

func TestCreateAndGetRoundTripMetadata(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	document := testDocument("abc12345", "metadata", "Generic metadata", "body")
	if _, err := store.Create(path, document); err != nil {
		t.Fatal(err)
	}
	result, err := store.Get("abc12345", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Metadata["period"] != "FY27H1" ||
		result.Metadata["summary"] != "Generic metadata" ||
		result.Path != path {
		t.Fatalf("result = %#v", result)
	}
}

func TestCreateRejectsMissingUnknownAndInvalidMetadata(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Document)
		want   string
	}{
		{name: "Missing", change: func(document *Document) {
			delete(document.Metadata, "period")
		}, want: "required metadata"},
		{name: "Unknown", change: func(document *Document) {
			document.Metadata["unexpected"] = "value"
		}, want: "unknown metadata"},
		{name: "Invalid", change: func(document *Document) {
			document.Metadata["period"] = "FY27"
		}, want: "invalid period"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, config := newTestStore(t)
			document := testDocument("abc12345", "metadata", "Metadata", "body")
			test.change(&document)
			path := filepath.Join(config.Directory, "record.md")
			if _, err := store.Create(path, document); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid document was published: %v", err)
			}
		})
	}
}

func TestCreateRejectsSearchSlotCount(t *testing.T) {
	store, config := newTestStore(t)
	document := testDocument("abc12345", "metadata", "Metadata", "body")
	document.SearchSlots = document.SearchSlots[:3]
	if _, err := store.Create(filepath.Join(config.Directory, "record.md"), document); err == nil ||
		!strings.Contains(err.Error(), "search slots") {
		t.Fatalf("error = %v", err)
	}
}

func TestSearchCombinesExactMetadataFilters(t *testing.T) {
	store, config := newTestStore(t)
	first := testDocument("first-id", "first", "Shared work", "body")
	second := testDocument("second-id", "second", "Shared work", "body")
	second.Metadata["period"] = "FY27H2"
	if _, err := store.Create(filepath.Join(config.Directory, "first.md"), first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(filepath.Join(config.Directory, "second.md"), second); err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(SearchQuery{
		Text: "Shared", Filters: map[string]string{"period": "FY27H1", "status": "wip"}, Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "first-id" {
		t.Fatalf("results = %#v", results)
	}
}

func TestSearchRejectsUnknownFilter(t *testing.T) {
	store, _ := newTestStore(t)
	if _, err := store.Search(SearchQuery{
		Text: "anything", Filters: map[string]string{"unknown": "value"}, Limit: 5,
	}); err == nil || !strings.Contains(err.Error(), "unknown metadata filter") {
		t.Fatalf("error = %v", err)
	}
}

func TestSearchUsesConfiguredSlotWeights(t *testing.T) {
	store, config := newTestStore(t)
	summaryMatch := testDocument("summary-id", "summary-match", "Critical migration", "ordinary body")
	bodyMatch := testDocument("body-id", "body-match", "Ordinary summary", "Critical migration")
	if _, err := store.Create(filepath.Join(config.Directory, "summary.md"), summaryMatch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(filepath.Join(config.Directory, "body.md"), bodyMatch); err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(SearchQuery{Text: "Critical migration", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ID != "summary-id" {
		t.Fatalf("results = %#v", results)
	}
}

func TestSearchExactIDIncludesRankAndFilters(t *testing.T) {
	store, config := newTestStore(t)
	document := testDocument("abc12345", "exact", "Exact", "body")
	if _, err := store.Create(filepath.Join(config.Directory, "record.md"), document); err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(SearchQuery{
		Text: "abc12345", Filters: map[string]string{"period": "FY27H1"}, Limit: 5,
	})
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
		t.Fatalf("rank omitted: %s", encoded)
	}
}

func TestListFiltersMetadataAndUsesSortKey(t *testing.T) {
	store, config := newTestStore(t)
	newer := testDocument("newer-id", "newer", "Newer", "body")
	newer.SortKey = "2026-08-31T02:00:00Z"
	newer.Metadata["created_at"] = newer.SortKey
	older := testDocument("older-id", "older", "Older", "body")
	older.SortKey = "2026-08-31T01:00:00Z"
	older.Metadata["created_at"] = older.SortKey
	other := testDocument("other-id", "other", "Other", "body")
	other.Metadata["period"] = "FY27H2"
	for path, document := range map[string]Document{
		"newer.md": newer, "older.md": older, "other.md": other,
	} {
		if _, err := store.Create(filepath.Join(config.Directory, path), document); err != nil {
			t.Fatal(err)
		}
	}
	results, err := store.List(map[string]string{"period": "FY27H1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ID != "older-id" || results[1].ID != "newer-id" {
		t.Fatalf("results = %#v", results)
	}
}

func TestOpenRebuildsIncompatibleIndexWithoutChangingMarkdown(t *testing.T) {
	root := t.TempDir()
	config := testConfig(root)
	if err := os.MkdirAll(config.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(config.Directory, "record.md")
	writeTestDocument(t, path, testDocument("abc12345", "rebuild", "Rebuild index", "body"))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", config.DatabasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE obsolete(value TEXT); INSERT INTO obsolete VALUES('old')`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	result, err := store.Get("abc12345", nil)
	if err != nil || result.Path != path {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("canonical Markdown changed during index rebuild")
	}
}

func TestOpenRebuildsMatchingFingerprintWithIncompleteSchema(t *testing.T) {
	root := t.TempDir()
	config := testConfig(root)
	database, err := sql.Open("sqlite", config.DatabasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE markdownstore_config (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
		INSERT INTO markdownstore_config(key, value) VALUES('fingerprint', ?)`,
		configFingerprint(config)); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var exists int
	if err := store.db.QueryRow(`SELECT EXISTS(
		SELECT 1 FROM sqlite_master
		WHERE type = 'table' AND name = 'markdownstore_documents'
	)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 1 {
		t.Fatal("incomplete matching-fingerprint schema was not rebuilt")
	}
}

func TestOpenRebuildsIndexWhenSchemaIDChanges(t *testing.T) {
	root := t.TempDir()
	config := testConfig(root)
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.SchemaID = "test-record/2"
	store, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var fingerprint string
	if err := store.db.QueryRow(`SELECT value FROM markdownstore_config WHERE key = 'fingerprint'`).Scan(&fingerprint); err != nil {
		t.Fatal(err)
	}
	if fingerprint != configFingerprint(config) {
		t.Fatalf("fingerprint = %q", fingerprint)
	}
}

func TestReconcileTracksDirectLifecycle(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	writeTestDocument(t, path, testDocument("record-id", "record", "Initial", "body"))
	if err := store.Reconcile(); err != nil {
		t.Fatal(err)
	}
	assertSearchPath(t, store, "Initial", path)

	updated := testDocument("record-id", "record", "Changed", "body")
	writeTestDocument(t, path, updated)
	if err := store.Reconcile(); err != nil {
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
		t.Fatal("deleted document remained searchable")
	}
}

func TestReconcileRejectsDuplicateIDsWithoutMutation(t *testing.T) {
	store, config := newTestStore(t)
	original := filepath.Join(config.Directory, "original.md")
	writeTestDocument(t, original, testDocument("original-id", "original", "Original", "body"))
	if err := store.Reconcile(); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(config.Directory, "first.md")
	second := filepath.Join(config.Directory, "second.md")
	writeTestDocument(t, first, testDocument("duplicate", "first", "First", "body"))
	writeTestDocument(t, second, testDocument("duplicate", "second", "Second", "body"))
	if err := store.Reconcile(); err == nil ||
		!strings.Contains(err.Error(), first) || !strings.Contains(err.Error(), second) {
		t.Fatalf("error = %v", err)
	}
	assertSearchPath(t, store, "Original", original)
}

func TestConcurrentCreatesPublishOnePath(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for _, document := range []Document{
		testDocument("first-id", "first", "First", "body"),
		testDocument("second-id", "second", "Second", "body"),
	} {
		wait.Add(1)
		go func(document Document) {
			defer wait.Done()
			<-start
			_, err := store.Create(path, document)
			errs <- err
		}(document)
	}
	close(start)
	wait.Wait()
	close(errs)
	successes, exists := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrPathExists):
			exists++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || exists != 1 {
		t.Fatalf("successes = %d, exists = %d", successes, exists)
	}
}

func TestUpdateSerializesLibraryMutations(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	if _, err := store.Create(path, testDocument("record-id", "record", "Record", "0")); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.Update(path, func(document Document) (Document, error) {
				time.Sleep(20 * time.Millisecond)
				if document.Body == "0" {
					document.Body = "1"
				} else {
					document.Body = "2"
				}
				document.SearchSlots[3] = document.Body
				return document, nil
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
		t.Fatalf("body = %q", entry.Body)
	}
}

func TestCreateRejectsDuplicateIDWithoutPublishingFile(t *testing.T) {
	store, config := newTestStore(t)
	if _, err := store.Create(
		filepath.Join(config.Directory, "first.md"),
		testDocument("same-id", "first", "First", "body"),
	); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(config.Directory, "second.md")
	if _, err := store.Create(second, testDocument("same-id", "second", "Second", "body")); !errors.Is(err, ErrIDExists) {
		t.Fatalf("error = %v", err)
	}
	if _, err := os.Stat(second); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("duplicate file published: %v", err)
	}
}

func TestUpdateRejectsIDChange(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	if _, err := store.Create(path, testDocument("original-id", "record", "Record", "body")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(path, func(document Document) (Document, error) {
		document.ID = "changed-id"
		return document, nil
	}); !errors.Is(err, ErrIDChanged) {
		t.Fatalf("error = %v", err)
	}
}

func TestRemoveRejectsReplacedDocument(t *testing.T) {
	store, config := newTestStore(t)
	path := filepath.Join(config.Directory, "record.md")
	entry, err := store.Create(path, testDocument("record-id", "record", "Record", "body"))
	if err != nil {
		t.Fatal(err)
	}
	writeTestDocument(t, path, testDocument("replacement", "replacement", "Replacement", "body"))
	if err := store.Remove(path, entry.Fingerprint); !errors.Is(err, ErrChanged) {
		t.Fatalf("error = %v", err)
	}
}

func TestStandaloneFileHelpers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.md")
	document := testDocument("read-id", "read", "Read", "body")
	data, err := (testCodec{}).Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := CreateFileAtomic(path, data); err != nil {
		t.Fatal(err)
	}
	if err := CreateFileAtomic(path, []byte("replacement")); !errors.Is(err, ErrPathExists) {
		t.Fatalf("error = %v", err)
	}
	entry, err := ReadFile(path, testCodec{}, testConfig(t.TempDir()).Fields, []float64{0.5, 1, 2, 1}, "record")
	if err != nil {
		t.Fatal(err)
	}
	if entry.ID != document.ID {
		t.Fatalf("entry = %#v", entry)
	}
	if err := WriteFileAtomic(path, data); err != nil {
		t.Fatal(err)
	}
}

func assertSearchPath(t *testing.T, store *Store, query, path string) {
	t.Helper()
	results, err := store.Search(SearchQuery{Text: query, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Path != path {
		t.Fatalf("results = %#v, want %s", results, path)
	}
}

func writeTestDocument(t *testing.T, path string, document Document) {
	t.Helper()
	data, err := (testCodec{}).Marshal(document)
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
