package markdownstore

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type indexedFile struct {
	ID          string
	Fingerprint Fingerprint
}

func (s *Store) Reconcile() error {
	return s.ReconcileDirectory(s.config.Directory)
}

func (s *Store) ReconcileDirectory(directory string) error {
	absolute, err := filepath.Abs(filepath.Clean(directory))
	if err != nil {
		return fmt.Errorf("resolve canonical directory: %w", err)
	}
	mutexValue, _ := directoryMutexes.LoadOrStore(absolute, &sync.Mutex{})
	return s.withDirectoryLock(absolute, mutexValue.(*sync.Mutex), func() error {
		return s.reconcileDirectory(absolute)
	})
}

func (s *Store) reconcileDirectory(directory string) error {
	paths, err := filepath.Glob(filepath.Join(directory, s.config.Pattern))
	if err != nil {
		return fmt.Errorf("list canonical documents: %w", err)
	}
	sort.Strings(paths)
	indexed, err := s.indexedFiles()
	if err != nil {
		return err
	}
	current := make(map[string]bool, len(paths))
	changed := make([]Entry, 0)
	var parseErrors []error
	for _, path := range paths {
		absolute, err := filepath.Abs(filepath.Clean(path))
		if err != nil {
			parseErrors = append(parseErrors, fmt.Errorf("resolve %s: %w", path, err))
			continue
		}
		current[absolute] = true
		info, err := os.Stat(absolute)
		if err != nil {
			parseErrors = append(parseErrors, fmt.Errorf("stat %s: %w", absolute, err))
			continue
		}
		existing, ok := indexed[absolute]
		fingerprint := Fingerprint{Size: info.Size(), ModTimeNS: info.ModTime().UnixNano()}
		if ok && existing.Fingerprint == fingerprint {
			continue
		}
		entry, err := s.readStable(absolute)
		if err != nil {
			parseErrors = append(parseErrors, err)
			continue
		}
		changed = append(changed, entry)
	}
	if len(parseErrors) > 0 {
		return errors.Join(parseErrors...)
	}

	removed := make([]string, 0)
	idPaths := make(map[string][]string)
	for path, item := range indexed {
		if !current[path] {
			removed = append(removed, path)
			continue
		}
		idPaths[item.ID] = append(idPaths[item.ID], path)
	}
	for _, entry := range changed {
		previousID := entry.ID
		if previous, ok := indexed[entry.Path]; ok {
			previousID = previous.ID
		}
		idPaths[previousID] = removePath(idPaths[previousID], entry.Path)
		idPaths[entry.ID] = append(idPaths[entry.ID], entry.Path)
	}
	sort.Strings(removed)
	var duplicateErrors []error
	for id, duplicatePaths := range idPaths {
		if len(duplicatePaths) < 2 {
			continue
		}
		sort.Strings(duplicatePaths)
		duplicateErrors = append(duplicateErrors,
			fmt.Errorf("duplicate %s ID %q in %s", s.config.EntityName, id, strings.Join(duplicatePaths, " and ")))
	}
	if len(duplicateErrors) > 0 {
		return errors.Join(duplicateErrors...)
	}
	return s.replaceIndexRecords(changed, removed)
}

func removePath(paths []string, target string) []string {
	for index, path := range paths {
		if path == target {
			return append(paths[:index], paths[index+1:]...)
		}
	}
	return paths
}

func (s *Store) indexedFiles() (map[string]indexedFile, error) {
	rows, err := s.db.Query(`SELECT path, id, file_size, mod_time_ns FROM markdownstore_documents`)
	if err != nil {
		return nil, fmt.Errorf("list indexed files: %w", err)
	}
	defer rows.Close()
	result := make(map[string]indexedFile)
	for rows.Next() {
		var path string
		var item indexedFile
		if err := rows.Scan(&path, &item.ID, &item.Fingerprint.Size, &item.Fingerprint.ModTimeNS); err != nil {
			return nil, fmt.Errorf("read indexed file: %w", err)
		}
		result[path] = item
	}
	return result, rows.Err()
}

func (s *Store) replaceIndexRecords(changed []Entry, removed []string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin index update: %w", err)
	}

	defer tx.Rollback()
	for _, path := range append(append([]string{}, removed...), entryPaths(changed)...) {
		if _, err := tx.Exec(`DELETE FROM markdownstore_fts WHERE path = ?`, path); err != nil {
			return fmt.Errorf("remove indexed search document %s: %w", path, err)
		}
		if _, err := tx.Exec(`DELETE FROM markdownstore_documents WHERE path = ?`, path); err != nil {
			return fmt.Errorf("remove indexed document %s: %w", path, err)
		}
	}
	for _, entry := range changed {
		if _, err := tx.Exec(`INSERT INTO markdownstore_documents(
			path, file_size, mod_time_ns, id, body, sort_key
		) VALUES(?, ?, ?, ?, ?, ?)`,
			entry.Path, entry.Fingerprint.Size, entry.Fingerprint.ModTimeNS,
			entry.ID, entry.Body, entry.SortKey); err != nil {
			return fmt.Errorf("index document %s: %w", entry.Path, err)
		}
		keys := make([]string, 0, len(entry.Metadata))
		for key := range entry.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, err := tx.Exec(`INSERT INTO markdownstore_metadata(document_path, key, value)
				VALUES(?, ?, ?)`, entry.Path, key, entry.Metadata[key]); err != nil {
				return fmt.Errorf("index metadata %s for %s: %w", key, entry.Path, err)
			}
		}
		columns := []string{"path"}
		placeholders := []string{"?"}
		args := []any{entry.Path}
		for index, value := range entry.SearchSlots {
			columns = append(columns, "slot_"+strconv.Itoa(index))
			placeholders = append(placeholders, "?")
			args = append(args, value)
		}
		query := `INSERT INTO markdownstore_fts(` + strings.Join(columns, ",") + `)
			VALUES(` + strings.Join(placeholders, ",") + `)`
		if _, err := tx.Exec(query, args...); err != nil {
			return fmt.Errorf("index search document %s: %w", entry.Path, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit index update: %w", err)
	}
	return nil
}

// ReplaceIndexRecords updates the derived index from already parsed canonical documents.
func (s *Store) ReplaceIndexRecords(changed []Entry, removed []string) error {
	return s.replaceIndexRecords(changed, removed)
}

func entryPaths(entries []Entry) []string {
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, entry.Path)
	}
	return paths
}

func (s *Store) getByID(id string, filters map[string]string) (Result, error) {
	if err := s.validateFilters(filters); err != nil {
		return Result{}, err
	}
	filterSQL, args := metadataFilters("d", filters)
	queryArgs := []any{id}
	queryArgs = append(queryArgs, args...)
	var result Result
	err := s.db.QueryRow(`SELECT d.id, d.path FROM markdownstore_documents d
		WHERE d.id = ?`+filterSQL, queryArgs...).Scan(&result.ID, &result.Path)
	if errors.Is(err, sql.ErrNoRows) {
		return Result{}, fmt.Errorf("no %s found with ID %q", s.config.EntityName, id)
	}
	if err != nil {
		return Result{}, fmt.Errorf("get indexed %s: %w", s.config.EntityName, err)
	}
	result.Metadata, err = s.loadMetadata(result.Path)
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *Store) Search(query SearchQuery) ([]Result, error) {
	text := strings.TrimSpace(query.Text)
	if text == "" {
		return nil, fmt.Errorf("search query must not be empty")
	}
	if query.Limit < 1 || query.Limit > 100 {
		return nil, fmt.Errorf("limit must be between 1 and 100")
	}
	if err := s.validateFilters(query.Filters); err != nil {
		return nil, err
	}
	filterSQL, filterArgs := metadataFilters("d", query.Filters)
	exactArgs := append([]any{text}, filterArgs...)
	exactArgs = append(exactArgs, query.Limit)
	rows, err := s.db.Query(`SELECT d.id, d.path FROM markdownstore_documents d
		WHERE d.id = ?`+filterSQL+` LIMIT ?`, exactArgs...)
	if err != nil {
		return nil, fmt.Errorf("search %s by ID: %w", s.config.EntityName, err)
	}
	exact, err := scanBaseResults(rows, false)
	if err != nil {
		return nil, err
	}
	if len(exact) > 0 {
		for index := range exact {
			exact[index].MatchReason = "id"
		}
		return s.attachMetadata(exact)
	}

	fts := buildFTSQuery(text)
	rank := bm25Expression(s.config.SearchWeights)
	searchArgs := append([]any{fts}, filterArgs...)
	searchArgs = append(searchArgs, query.Limit)
	rows, err = s.db.Query(`SELECT d.id, d.path, `+rank+`
		FROM markdownstore_fts
		JOIN markdownstore_documents d ON d.path = markdownstore_fts.path
		WHERE markdownstore_fts MATCH ?`+filterSQL+`
		ORDER BY `+rank+`, d.sort_key DESC LIMIT ?`, searchArgs...)
	if err != nil {
		return nil, fmt.Errorf("search %s records: %w", s.config.EntityName, err)
	}
	results, err := scanBaseResults(rows, true)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no %s match for query %q", s.config.EntityName, text)
	}
	for index := range results {
		results[index].MatchReason = "full_text"
	}
	return s.attachMetadata(results)
}

func (s *Store) List(filters map[string]string) ([]Result, error) {
	if err := s.validateFilters(filters); err != nil {
		return nil, err
	}
	filterSQL, args := metadataFilters("d", filters)
	rows, err := s.db.Query(`SELECT d.id, d.path FROM markdownstore_documents d
		WHERE 1 = 1`+filterSQL+` ORDER BY d.sort_key ASC, d.rowid ASC`, args...)
	if err != nil {
		return nil, fmt.Errorf("list %s records: %w", s.config.EntityName, err)
	}
	results, err := scanBaseResults(rows, false)
	if err != nil {
		return nil, err
	}
	return s.attachMetadata(results)
}

func metadataFilters(alias string, filters map[string]string) (string, []any) {
	keys := make([]string, 0, len(filters))
	for key := range filters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var sqlBuilder strings.Builder
	args := make([]any, 0, len(keys)*2)
	for index, key := range keys {
		metadataAlias := "m" + strconv.Itoa(index)
		sqlBuilder.WriteString(` AND EXISTS (SELECT 1 FROM markdownstore_metadata `)
		sqlBuilder.WriteString(metadataAlias)
		sqlBuilder.WriteString(` WHERE `)
		sqlBuilder.WriteString(metadataAlias)
		sqlBuilder.WriteString(`.document_path = `)
		sqlBuilder.WriteString(alias)
		sqlBuilder.WriteString(`.path AND `)
		sqlBuilder.WriteString(metadataAlias)
		sqlBuilder.WriteString(`.key = ? AND `)
		sqlBuilder.WriteString(metadataAlias)
		sqlBuilder.WriteString(`.value = ?)`)
		args = append(args, key, filters[key])
	}
	return sqlBuilder.String(), args
}

func bm25Expression(weights []float64) string {
	values := []string{"0"}
	for _, weight := range weights {
		values = append(values, strconv.FormatFloat(weight, 'g', -1, 64))
	}
	return `bm25(markdownstore_fts, ` + strings.Join(values, ", ") + `)`
}

func scanBaseResults(rows *sql.Rows, ranked bool) ([]Result, error) {
	defer rows.Close()
	var results []Result
	for rows.Next() {
		var result Result
		if ranked {
			if err := rows.Scan(&result.ID, &result.Path, &result.Rank); err != nil {
				return nil, fmt.Errorf("read search result: %w", err)
			}
		} else if err := rows.Scan(&result.ID, &result.Path); err != nil {
			return nil, fmt.Errorf("read search result: %w", err)
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func (s *Store) attachMetadata(results []Result) ([]Result, error) {
	for index := range results {
		metadata, err := s.loadMetadata(results[index].Path)
		if err != nil {
			return nil, err
		}
		results[index].Metadata = metadata
	}
	return results, nil
}

func (s *Store) loadMetadata(path string) (map[string]string, error) {
	rows, err := s.db.Query(`SELECT key, value FROM markdownstore_metadata
		WHERE document_path = ? ORDER BY key`, path)
	if err != nil {
		return nil, fmt.Errorf("read indexed metadata: %w", err)
	}
	defer rows.Close()
	metadata := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("read indexed metadata: %w", err)
		}
		metadata[key] = value
	}
	return metadata, rows.Err()
}

func buildFTSQuery(query string) string {
	terms := strings.Fields(query)
	quoted := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.ReplaceAll(term, `"`, `""`)
		if term != "" {
			quoted = append(quoted, `"`+term+`"`)
		}
	}
	return strings.Join(quoted, " AND ")
}
