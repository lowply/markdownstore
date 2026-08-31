package markdownstore

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

type Record struct {
	ID         string
	Repository string
	Name       string
	Summary    string
	Body       string
	SearchText string
	Status     string
	CreatedAt  string
	UpdatedAt  string
}

type Fingerprint struct {
	Size      int64
	ModTimeNS int64
}

type Entry struct {
	Record
	Path        string
	Fingerprint Fingerprint
}

type Result struct {
	ID          string  `json:"id"`
	Repository  string  `json:"repository"`
	Name        string  `json:"name"`
	Summary     string  `json:"summary"`
	Status      string  `json:"status"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
	Path        string  `json:"path"`
	MatchReason string  `json:"match_reason,omitempty"`
	Rank        float64 `json:"rank"`
}

type SearchQuery struct {
	Text   string
	Status string
	Limit  int
}

type Codec interface {
	Parse(path string, frontmatter, body []byte) (Record, error)
	Marshal(record Record) ([]byte, error)
}

type Config struct {
	Directory    string
	DatabasePath string
	Pattern      string
	Statuses     []string
	Codec        Codec
	EntityName   string
}

type Store struct {
	config Config
	db     *sql.DB
	mu     *sync.Mutex
}

var directoryMutexes sync.Map

func Open(config Config) (*Store, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	db, err := openDatabase(normalized)
	if err != nil {
		return nil, err
	}
	mutexValue, _ := directoryMutexes.LoadOrStore(normalized.Directory, &sync.Mutex{})
	return &Store{config: normalized, db: db, mu: mutexValue.(*sync.Mutex)}, nil
}

func normalizeConfig(config Config) (Config, error) {
	if strings.TrimSpace(config.Directory) == "" {
		return Config{}, fmt.Errorf("markdownstore directory must not be empty")
	}
	if strings.TrimSpace(config.DatabasePath) == "" {
		return Config{}, fmt.Errorf("markdownstore database path must not be empty")
	}
	if config.Codec == nil {
		return Config{}, fmt.Errorf("markdownstore codec must not be nil")
	}
	directory, err := filepath.Abs(filepath.Clean(config.Directory))
	if err != nil {
		return Config{}, fmt.Errorf("resolve markdownstore directory: %w", err)
	}
	databasePath, err := filepath.Abs(filepath.Clean(config.DatabasePath))
	if err != nil {
		return Config{}, fmt.Errorf("resolve markdownstore database path: %w", err)
	}
	config.Directory = directory
	config.DatabasePath = databasePath
	if config.Pattern == "" {
		config.Pattern = "*.md"
	}
	if config.EntityName == "" {
		config.EntityName = "document"
	}
	if len(config.Statuses) == 0 {
		return Config{}, fmt.Errorf("markdownstore statuses must not be empty")
	}
	seen := make(map[string]bool, len(config.Statuses))
	for _, status := range config.Statuses {
		status = strings.TrimSpace(status)
		if status == "" || seen[status] {
			return Config{}, fmt.Errorf("invalid markdownstore status %q", status)
		}
		seen[status] = true
	}
	return config, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Read(path string) (Entry, error) {
	return s.readStable(path)
}

func (s *Store) Get(id string) (Result, error) {
	return s.getByID(id)
}

func (s *Store) HasID(id string) (bool, error) {
	var exists int
	if err := s.db.QueryRow(`SELECT EXISTS(
		SELECT 1 FROM markdownstore_records WHERE id = ?
	)`, id).Scan(&exists); err != nil {
		return false, fmt.Errorf("check %s ID: %w", s.config.EntityName, err)
	}
	return exists == 1, nil
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

func (s *Store) validateRecord(record Record) error {
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("%s ID must not be empty", s.config.EntityName)
	}
	if strings.TrimSpace(record.Name) == "" {
		return fmt.Errorf("%s name must not be empty", s.config.EntityName)
	}
	if strings.TrimSpace(record.Summary) == "" {
		return fmt.Errorf("%s summary must not be empty", s.config.EntityName)
	}
	if strings.TrimSpace(record.CreatedAt) == "" || strings.TrimSpace(record.UpdatedAt) == "" {
		return fmt.Errorf("%s timestamps must not be empty", s.config.EntityName)
	}
	for _, status := range s.config.Statuses {
		if record.Status == status {
			return nil
		}
	}
	return fmt.Errorf("invalid %s status %q", s.config.EntityName, record.Status)
}

func equalRecord(first, second Record) bool {
	return first == second
}
