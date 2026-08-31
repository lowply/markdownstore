package markdownstore

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 3

func openDatabase(config Config) (*sql.DB, error) {
	existed := true
	if _, err := os.Stat(config.DatabasePath); errors.Is(err, os.ErrNotExist) {
		existed = false
		if err := os.MkdirAll(filepath.Dir(config.DatabasePath), 0o700); err != nil {
			return nil, fmt.Errorf("create index directory: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("inspect index: %w", err)
	}

	db, err := sql.Open("sqlite", config.DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}
	db.SetMaxOpenConns(1)
	current, err := inspectSchemaVersion(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	if current > schemaVersion {
		db.Close()
		return nil, fmt.Errorf("markdownstore database schema version %d is newer than supported version %d", current, schemaVersion)
	}
	if err := configureDatabase(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := migrateDatabase(db, current); err != nil {
		db.Close()
		return nil, err
	}
	if err := os.MkdirAll(config.Directory, 0o700); err != nil {
		db.Close()
		return nil, fmt.Errorf("create canonical directory: %w", err)
	}
	if err := os.Chmod(config.Directory, 0o700); err != nil {
		db.Close()
		return nil, fmt.Errorf("secure canonical directory: %w", err)
	}
	if !existed {
		if err := os.Chmod(filepath.Dir(config.DatabasePath), 0o700); err != nil {
			db.Close()
			return nil, fmt.Errorf("secure index directory: %w", err)
		}
	}
	if err := os.Chmod(config.DatabasePath, 0o600); err != nil {
		db.Close()
		return nil, fmt.Errorf("secure index: %w", err)
	}
	return db, nil
}

func inspectSchemaVersion(db *sql.DB) (int, error) {
	var hasMigrations int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&hasMigrations); err != nil {
		return 0, fmt.Errorf("inspect index schema: %w", err)
	}
	if hasMigrations == 0 {
		return 0, nil
	}
	var current int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return 0, fmt.Errorf("read index schema version: %w", err)
	}
	return current, nil
}

func configureDatabase(db *sql.DB) error {
	if _, err := db.Exec(`PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL;`); err != nil {
		return fmt.Errorf("configure index: %w", err)
	}
	return nil
}

func migrateDatabase(db *sql.DB, current int) error {
	if current == schemaVersion {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin index migration: %w", err)
	}
	defer tx.Rollback()
	if err := createCurrentSchema(tx); err != nil {
		return err
	}
	switch current {
	case 1:
		if err := migrateProjectsV1(tx); err != nil {
			return err
		}
	case 2:
		if err := migrateMemosV2(tx); err != nil {
			return err
		}
	case 0:
	default:
		return fmt.Errorf("unsupported markdownstore database schema version %d", current)
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO schema_migrations(version, applied_at) VALUES(?, ?)`,
		schemaVersion, time.Now().UTC().Format(time.RFC3339)); err != nil {
		return fmt.Errorf("record index schema: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit index migration: %w", err)
	}
	return nil
}

func createCurrentSchema(tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS markdownstore_records (
			path TEXT PRIMARY KEY,
			file_size INTEGER NOT NULL,
			mod_time_ns INTEGER NOT NULL,
			id TEXT NOT NULL UNIQUE,
			repository TEXT NOT NULL,
			name TEXT NOT NULL,
			summary TEXT NOT NULL,
			body TEXT NOT NULL,
			search_text TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS markdownstore_records_status ON markdownstore_records(status)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS markdownstore_fts USING fts5(
			repository,
			name,
			summary,
			search_text,
			content='markdownstore_records',
			content_rowid='rowid',
			tokenize='unicode61'
		)`,
		`CREATE TRIGGER IF NOT EXISTS markdownstore_records_after_insert
			AFTER INSERT ON markdownstore_records BEGIN
			INSERT INTO markdownstore_fts(rowid, repository, name, summary, search_text)
			VALUES (new.rowid, new.repository, new.name, new.summary, new.search_text);
		END`,
		`CREATE TRIGGER IF NOT EXISTS markdownstore_records_after_delete
			AFTER DELETE ON markdownstore_records BEGIN
			INSERT INTO markdownstore_fts(markdownstore_fts, rowid, repository, name, summary, search_text)
			VALUES ('delete', old.rowid, old.repository, old.name, old.summary, old.search_text);
		END`,
		`CREATE TRIGGER IF NOT EXISTS markdownstore_records_after_update
			AFTER UPDATE ON markdownstore_records BEGIN
			INSERT INTO markdownstore_fts(markdownstore_fts, rowid, repository, name, summary, search_text)
			VALUES ('delete', old.rowid, old.repository, old.name, old.summary, old.search_text);
			INSERT INTO markdownstore_fts(rowid, repository, name, summary, search_text)
			VALUES (new.rowid, new.repository, new.name, new.summary, new.search_text);
		END`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("apply index schema version %d: %w", schemaVersion, err)
		}
	}
	return nil
}

func migrateMemosV2(tx *sql.Tx) error {
	exists, err := tableExists(tx, "memos")
	if err != nil || !exists {
		return err
	}
	_, err = tx.Exec(`INSERT OR IGNORE INTO markdownstore_records(
		path, file_size, mod_time_ns, id, repository, name, summary, body,
		search_text, status, created_at, updated_at
	) SELECT path, file_size, mod_time_ns, id, repository, name, summary, body,
		body, status, created_at, updated_at FROM memos`)
	if err != nil {
		return fmt.Errorf("migrate memo index: %w", err)
	}
	return nil
}

func migrateProjectsV1(tx *sql.Tx) error {
	exists, err := tableExists(tx, "projects")
	if err != nil || !exists {
		return err
	}
	columns, err := tableColumns(tx, "projects")
	if err != nil {
		return err
	}
	searchText := "body"
	if columns["goals"] {
		searchText = "goals || char(10) || body"
	}
	query := fmt.Sprintf(`INSERT OR IGNORE INTO markdownstore_records(
		path, file_size, mod_time_ns, id, repository, name, summary, body,
		search_text, status, created_at, updated_at
	) SELECT path, file_size, mod_time_ns, id, repository, name, summary, body,
		%s, CASE status WHEN 'done' THEN 'closed' ELSE status END,
		created_at, updated_at FROM projects`, searchText)
	if _, err := tx.Exec(query); err != nil {
		return fmt.Errorf("migrate project index: %w", err)
	}
	return nil
}

func tableExists(tx *sql.Tx, name string) (bool, error) {
	var exists int
	if err := tx.QueryRow(`SELECT EXISTS(
		SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?
	)`, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect legacy index: %w", err)
	}
	return exists == 1, nil
}

func tableColumns(tx *sql.Tx, name string) (map[string]bool, error) {
	if strings.ContainsAny(name, `"'`) {
		return nil, fmt.Errorf("invalid table name %q", name)
	}
	rows, err := tx.Query(`PRAGMA table_info(` + name + `)`)
	if err != nil {
		return nil, fmt.Errorf("inspect table %s: %w", name, err)
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var column, dataType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &column, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("inspect table %s: %w", name, err)
		}
		columns[column] = true
	}
	return columns, rows.Err()
}
