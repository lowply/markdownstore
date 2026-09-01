package markdownstore

import (
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
)

var (
	ErrPathExists = errors.New("canonical path already exists")
	ErrChanged    = errors.New("canonical document changed")
	ErrIDExists   = errors.New("canonical ID already exists")
	ErrIDChanged  = errors.New("canonical ID cannot be changed")
)

type Document struct {
	ID          string
	Metadata    map[string]string
	Body        string
	SortKey     string
	SearchSlots []string
}

type MetadataField struct {
	Name     string
	Required bool
	Validate func(string) error
}

type Fingerprint struct {
	Size      int64
	ModTimeNS int64
}

type Entry struct {
	Document
	Path        string
	Fingerprint Fingerprint
}

type Result struct {
	ID          string            `json:"id"`
	Metadata    map[string]string `json:"metadata"`
	Path        string            `json:"path"`
	MatchReason string            `json:"match_reason,omitempty"`
	Rank        float64           `json:"rank"`
}

type SearchQuery struct {
	Text    string
	Filters map[string]string
	Limit   int
}

type Codec interface {
	Parse(path string, frontmatter, body []byte) (Document, error)
	Marshal(document Document) ([]byte, error)
}

type Config struct {
	Directory     string
	DatabasePath  string
	Pattern       string
	EntityName    string
	SchemaID      string
	Fields        []MetadataField
	SearchWeights []float64
	Codec         Codec
}

type Store struct {
	config Config
	fields map[string]MetadataField
	db     *sql.DB
	mu     *sync.Mutex
}

var directoryMutexes sync.Map

func Open(config Config) (*Store, error) {
	normalized, fields, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	db, err := openDatabase(normalized)
	if err != nil {
		return nil, err
	}
	mutexValue, _ := directoryMutexes.LoadOrStore(normalized.Directory, &sync.Mutex{})
	store := &Store{
		config: normalized, fields: fields, db: db, mu: mutexValue.(*sync.Mutex),
	}
	if err := store.Reconcile(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func normalizeConfig(config Config) (Config, map[string]MetadataField, error) {
	if strings.TrimSpace(config.Directory) == "" {
		return Config{}, nil, fmt.Errorf("markdownstore directory must not be empty")
	}
	if strings.TrimSpace(config.DatabasePath) == "" {
		return Config{}, nil, fmt.Errorf("markdownstore database path must not be empty")
	}
	if config.Codec == nil {
		return Config{}, nil, fmt.Errorf("markdownstore codec must not be nil")
	}
	if strings.TrimSpace(config.SchemaID) == "" {
		return Config{}, nil, fmt.Errorf("markdownstore schema ID must not be empty")
	}
	if len(config.SearchWeights) == 0 {
		return Config{}, nil, fmt.Errorf("markdownstore search weights must not be empty")
	}
	directory, err := filepath.Abs(filepath.Clean(config.Directory))
	if err != nil {
		return Config{}, nil, fmt.Errorf("resolve markdownstore directory: %w", err)
	}
	databasePath, err := filepath.Abs(filepath.Clean(config.DatabasePath))
	if err != nil {
		return Config{}, nil, fmt.Errorf("resolve markdownstore database path: %w", err)
	}
	config.Directory = directory
	config.DatabasePath = databasePath
	config.SchemaID = strings.TrimSpace(config.SchemaID)
	if config.Pattern == "" {
		config.Pattern = "*.md"
	}
	if config.EntityName == "" {
		config.EntityName = "document"
	}
	fields, err := buildFieldMap(config.Fields)
	if err != nil {
		return Config{}, nil, err
	}
	return config, fields, nil
}

func buildFieldMap(definitions []MetadataField) (map[string]MetadataField, error) {
	fields := make(map[string]MetadataField, len(definitions))
	for _, definition := range definitions {
		definition.Name = strings.TrimSpace(definition.Name)
		if definition.Name == "" {
			return nil, fmt.Errorf("metadata field name must not be empty")
		}
		if _, exists := fields[definition.Name]; exists {
			return nil, fmt.Errorf("duplicate metadata field %q", definition.Name)
		}
		fields[definition.Name] = definition
	}
	return fields, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Read(path string) (Entry, error) {
	return s.readStable(path)
}

func (s *Store) Get(id string, filters map[string]string) (Result, error) {
	return s.getByID(id, filters)
}

func (s *Store) HasID(id string) (bool, error) {
	var exists int
	if err := s.db.QueryRow(`SELECT EXISTS(
		SELECT 1 FROM markdownstore_documents WHERE id = ?
	)`, id).Scan(&exists); err != nil {
		return false, fmt.Errorf("check %s ID: %w", s.config.EntityName, err)
	}
	return exists == 1, nil
}

func (s *Store) validateDocument(document Document) error {
	if strings.TrimSpace(document.ID) == "" {
		return fmt.Errorf("%s ID must not be empty", s.config.EntityName)
	}
	if strings.TrimSpace(document.SortKey) == "" {
		return fmt.Errorf("%s sort key must not be empty", s.config.EntityName)
	}
	if len(document.SearchSlots) != len(s.config.SearchWeights) {
		return fmt.Errorf("%s search slots must contain %d values", s.config.EntityName, len(s.config.SearchWeights))
	}
	for key := range document.Metadata {
		if _, exists := s.fields[key]; !exists {
			return fmt.Errorf("unknown metadata %q", key)
		}
	}
	for name, field := range s.fields {
		value, exists := document.Metadata[name]
		if field.Required && !exists {
			return fmt.Errorf("required metadata %q is missing", name)
		}
		if exists && field.Validate != nil {
			if err := field.Validate(value); err != nil {
				return fmt.Errorf("metadata %q: %w", name, err)
			}
		}
	}
	return nil
}

func (s *Store) validateFilters(filters map[string]string) error {
	for key := range filters {
		if _, exists := s.fields[key]; !exists {
			return fmt.Errorf("unknown metadata filter %q", key)
		}
	}
	return nil
}

func equalDocument(first, second Document) bool {
	return first.ID == second.ID &&
		maps.Equal(first.Metadata, second.Metadata) &&
		first.Body == second.Body &&
		first.SortKey == second.SortKey &&
		slices.Equal(first.SearchSlots, second.SearchSlots)
}

func cloneDocument(document Document) Document {
	document.Metadata = maps.Clone(document.Metadata)
	document.SearchSlots = slices.Clone(document.SearchSlots)
	return document
}

func (s *Store) withMutationLock(operation func() error) error {
	return s.withDirectoryLock(s.config.Directory, s.mu, operation)
}

func (s *Store) withDirectoryLock(directory string, mutex *sync.Mutex, operation func() error) error {
	mutex.Lock()
	defer mutex.Unlock()
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create canonical directory: %w", err)
	}
	lockPath := filepath.Join(directory, ".markdownstore.lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open mutation lock: %w", err)
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock canonical directory: %w", err)
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	return operation()
}
