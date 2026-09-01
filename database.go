package markdownstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

type fingerprintConfig struct {
	SchemaID string             `json:"schema_id"`
	Fields   []fingerprintField `json:"fields"`
	Weights  []float64          `json:"weights"`
}

type fingerprintField struct {
	Name     string `json:"name"`
	Required bool   `json:"required"`
}

func configFingerprint(config Config) string {
	fields := make([]fingerprintField, 0, len(config.Fields))
	for _, field := range config.Fields {
		fields = append(fields, fingerprintField{Name: strings.TrimSpace(field.Name), Required: field.Required})
	}
	data, _ := json.Marshal(fingerprintConfig{
		SchemaID: config.SchemaID, Fields: fields, Weights: config.SearchWeights,
	})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func openDatabase(config Config) (*sql.DB, error) {
	fingerprint := configFingerprint(config)
	compatible, err := databaseCompatible(config.DatabasePath, config, fingerprint)
	if err != nil {
		return nil, err
	}
	if !compatible {
		if err := removeDatabaseFiles(config.DatabasePath); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(config.DatabasePath), 0o700); err != nil {
		return nil, fmt.Errorf("create index directory: %w", err)
	}
	db, err := sql.Open("sqlite", config.DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure index: %w", err)
	}
	if !compatible {
		if err := createSchema(db, config, fingerprint); err != nil {
			db.Close()
			return nil, err
		}
	}
	if err := os.MkdirAll(config.Directory, 0o700); err != nil {
		db.Close()
		return nil, fmt.Errorf("create canonical directory: %w", err)
	}
	if err := os.Chmod(config.Directory, 0o700); err != nil {
		db.Close()
		return nil, fmt.Errorf("secure canonical directory: %w", err)
	}
	if err := os.Chmod(config.DatabasePath, 0o600); err != nil {
		db.Close()
		return nil, fmt.Errorf("secure index: %w", err)
	}
	return db, nil
}

func databaseCompatible(path string, config Config, fingerprint string) (bool, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("inspect index: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return false, nil
	}
	defer db.Close()
	var hasConfig int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'markdownstore_config'`).Scan(&hasConfig); err != nil {
		return false, nil
	}
	if hasConfig == 0 {
		return false, nil
	}
	var stored string
	if err := db.QueryRow(`SELECT value FROM markdownstore_config WHERE key = 'fingerprint'`).Scan(&stored); err != nil {
		return false, nil
	}
	if stored != fingerprint {
		return false, nil
	}
	if _, err := db.Exec(`SELECT path, file_size, mod_time_ns, id, body, sort_key
		FROM markdownstore_documents LIMIT 0`); err != nil {
		return false, nil
	}
	if _, err := db.Exec(`SELECT document_path, key, value
		FROM markdownstore_metadata LIMIT 0`); err != nil {
		return false, nil
	}
	ftsColumns := []string{"path"}
	for index := range config.SearchWeights {
		ftsColumns = append(ftsColumns, "slot_"+strconv.Itoa(index))
	}
	if _, err := db.Exec(`SELECT ` + strings.Join(ftsColumns, ",") + `
		FROM markdownstore_fts LIMIT 0`); err != nil {
		return false, nil
	}
	return true, nil
}

func removeDatabaseFiles(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("replace incompatible index %s: %w", candidate, err)
		}
	}
	return nil
}

func createSchema(db *sql.DB, config Config, fingerprint string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin index schema: %w", err)
	}
	defer tx.Rollback()
	statements := []string{
		`CREATE TABLE markdownstore_config (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE markdownstore_documents (
			path TEXT PRIMARY KEY,
			file_size INTEGER NOT NULL,
			mod_time_ns INTEGER NOT NULL,
			id TEXT NOT NULL UNIQUE,
			body TEXT NOT NULL,
			sort_key TEXT NOT NULL
		)`,
		`CREATE TABLE markdownstore_metadata (
			document_path TEXT NOT NULL REFERENCES markdownstore_documents(path) ON DELETE CASCADE,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY(document_path, key)
		)`,
		`CREATE INDEX markdownstore_metadata_lookup
			ON markdownstore_metadata(key, value, document_path)`,
		ftsSchema(len(config.SearchWeights)),
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("create index schema: %w", err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO markdownstore_config(key, value) VALUES('fingerprint', ?)`, fingerprint); err != nil {
		return fmt.Errorf("record index fingerprint: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit index schema: %w", err)
	}
	return nil
}

func ftsSchema(slotCount int) string {
	columns := []string{"path UNINDEXED"}
	for index := range slotCount {
		columns = append(columns, "slot_"+strconv.Itoa(index))
	}
	columns = append(columns, `tokenize='unicode61'`)
	return `CREATE VIRTUAL TABLE markdownstore_fts USING fts5(` + strings.Join(columns, ", ") + `)`
}
