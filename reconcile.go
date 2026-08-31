package markdownstore

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

func (s *Store) ReplaceIndexRecords(changed []Entry, removed []string) error {
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
	rows, err := s.db.Query(`SELECT path, id, file_size, mod_time_ns FROM markdownstore_records`)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list indexed files: %w", err)
	}
	return result, nil
}

func (s *Store) replaceIndexRecords(changed []Entry, removed []string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin index update: %w", err)
	}
	defer tx.Rollback()
	for _, path := range removed {
		if _, err := tx.Exec(`DELETE FROM markdownstore_records WHERE path = ?`, path); err != nil {
			return fmt.Errorf("remove indexed document %s: %w", path, err)
		}
	}
	for _, entry := range changed {
		if _, err := tx.Exec(`DELETE FROM markdownstore_records WHERE path = ?`, entry.Path); err != nil {
			return fmt.Errorf("replace indexed document %s: %w", entry.Path, err)
		}
	}
	for _, entry := range changed {
		_, err := tx.Exec(`INSERT INTO markdownstore_records(
			path, file_size, mod_time_ns, id, repository, name, summary, body,
			search_text, status, created_at, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			entry.Path, entry.Fingerprint.Size, entry.Fingerprint.ModTimeNS,
			entry.ID, entry.Repository, entry.Name, entry.Summary, entry.Body,
			entry.SearchText, entry.Status, entry.CreatedAt, entry.UpdatedAt)
		if err != nil {
			return fmt.Errorf("index document %s: %w", entry.Path, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit index update: %w", err)
	}
	return nil
}

func (s *Store) getByID(id string) (Result, error) {
	var result Result
	err := s.db.QueryRow(`SELECT id, repository, name, summary, status,
		created_at, updated_at, path FROM markdownstore_records WHERE id = ?`, id).Scan(
		&result.ID, &result.Repository, &result.Name, &result.Summary, &result.Status,
		&result.CreatedAt, &result.UpdatedAt, &result.Path)
	if errors.Is(err, sql.ErrNoRows) {
		return Result{}, fmt.Errorf("no %s found with ID %q", s.config.EntityName, id)
	}
	if err != nil {
		return Result{}, fmt.Errorf("get indexed %s: %w", s.config.EntityName, err)
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
	if err := s.validateOptionalStatus(query.Status); err != nil {
		return nil, err
	}
	exactSQL := `SELECT id, repository, name, summary, status, created_at, updated_at, path
		FROM markdownstore_records WHERE id = ?`
	args := []any{text}
	if query.Status != "" {
		exactSQL += ` AND status = ?`
		args = append(args, query.Status)
	}
	exactSQL += ` LIMIT ?`
	args = append(args, query.Limit)
	rows, err := s.db.Query(exactSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("search %s by ID: %w", s.config.EntityName, err)
	}
	exact, err := scanResults(rows, false)
	if err != nil {
		return nil, err
	}
	if len(exact) > 0 {
		for index := range exact {
			exact[index].MatchReason = "id"
			exact[index].Rank = 0
		}
		return exact, nil
	}

	fts := buildFTSQuery(text)
	searchSQL := `SELECT r.id, r.repository, r.name, r.summary, r.status,
			r.created_at, r.updated_at, r.path,
			bm25(markdownstore_fts, 0.5, 1.0, 2.0, 1.0)
		FROM markdownstore_fts
		JOIN markdownstore_records r ON r.rowid = markdownstore_fts.rowid
		WHERE markdownstore_fts MATCH ?`
	args = []any{fts}
	if query.Status != "" {
		searchSQL += ` AND r.status = ?`
		args = append(args, query.Status)
	}
	searchSQL += ` ORDER BY bm25(markdownstore_fts, 0.5, 1.0, 2.0, 1.0),
		r.updated_at DESC LIMIT ?`
	args = append(args, query.Limit)
	rows, err = s.db.Query(searchSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("search %s records: %w", s.config.EntityName, err)
	}
	results, err := scanResults(rows, true)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no %s match for query %q", s.config.EntityName, text)
	}
	for index := range results {
		results[index].MatchReason = "full_text"
	}
	return results, nil
}

func (s *Store) List(status string) ([]Result, error) {
	if err := s.validateOptionalStatus(status); err != nil {
		return nil, err
	}
	query := `SELECT id, repository, name, summary, status, created_at, updated_at, path
		FROM markdownstore_records`
	var args []any
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY created_at ASC, rowid ASC`
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("list %s records: %w", s.config.EntityName, err)
	}
	return scanResults(rows, false)
}

func scanResults(rows *sql.Rows, ranked bool) ([]Result, error) {
	defer rows.Close()
	var results []Result
	for rows.Next() {
		var result Result
		destinations := []any{
			&result.ID, &result.Repository, &result.Name, &result.Summary,
			&result.Status, &result.CreatedAt, &result.UpdatedAt, &result.Path,
		}
		if ranked {
			destinations = append(destinations, &result.Rank)
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("read search result: %w", err)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read search results: %w", err)
	}
	return results, nil
}

func (s *Store) validateOptionalStatus(status string) error {
	if status == "" {
		return nil
	}
	for _, allowed := range s.config.Statuses {
		if status == allowed {
			return nil
		}
	}
	return fmt.Errorf("invalid status %q", status)
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
