package markdownstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func ReadFile(
	path string,
	codec Codec,
	fields []MetadataField,
	searchWeights []float64,
	entityName string,
) (Entry, error) {
	if codec == nil {
		return Entry{}, fmt.Errorf("markdownstore codec must not be nil")
	}
	fieldMap, err := buildFieldMap(fields)
	if err != nil {
		return Entry{}, err
	}
	store := &Store{
		config: Config{Codec: codec, Fields: fields, SearchWeights: searchWeights, EntityName: entityName},
		fields: fieldMap,
	}
	if store.config.EntityName == "" {
		store.config.EntityName = "document"
	}
	return store.readStable(path)
}

func WriteFileAtomic(path string, data []byte) error {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("resolve canonical path: %w", err)
	}
	temporary, err := createTemporary(filepath.Dir(absolute), data)
	if err != nil {
		return err
	}
	defer os.Remove(temporary)
	if err := os.Rename(temporary, absolute); err != nil {
		return fmt.Errorf("save canonical document: %w", err)
	}
	return nil
}

func CreateFileAtomic(path string, data []byte) error {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("resolve canonical path: %w", err)
	}
	temporary, err := createTemporary(filepath.Dir(absolute), data)
	if err != nil {
		return err
	}
	defer os.Remove(temporary)
	if err := os.Link(temporary, absolute); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%w: %s", ErrPathExists, absolute)
		}
		return fmt.Errorf("publish canonical document: %w", err)
	}
	return nil
}

func (s *Store) Create(path string, document Document) (Entry, error) {
	var result Entry
	err := s.withMutationLock(func() error {
		absolute, err := filepath.Abs(filepath.Clean(path))
		if err != nil {
			return fmt.Errorf("resolve canonical path: %w", err)
		}
		if err := s.validateDocument(document); err != nil {
			return err
		}
		exists, err := s.HasID(document.ID)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: %s", ErrIDExists, document.ID)
		}
		data, err := s.config.Codec.Marshal(document)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", s.config.EntityName, err)
		}
		if err := CreateFileAtomic(absolute, data); err != nil {
			return err
		}
		result, err = s.readStable(absolute)
		if err != nil {
			os.Remove(absolute)
			return err
		}
		if err := s.replaceIndexRecords([]Entry{result}, nil); err != nil {
			if removeErr := os.Remove(absolute); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return errors.Join(err, fmt.Errorf("roll back canonical document: %w", removeErr))
			}
			return err
		}
		return nil
	})
	return result, err
}

func (s *Store) Update(path string, mutate func(Document) (Document, error)) (Entry, error) {
	var result Entry
	err := s.withMutationLock(func() error {
		current, err := s.readStable(path)
		if err != nil {
			return err
		}
		originalData, err := os.ReadFile(current.Path)
		if err != nil {
			return fmt.Errorf("read original %s: %w", s.config.EntityName, err)
		}
		updated, err := mutate(cloneDocument(current.Document))
		if err != nil {
			return err
		}
		if updated.ID != current.ID {
			return fmt.Errorf("%w: %s", ErrIDChanged, current.Path)
		}
		if err := s.validateDocument(updated); err != nil {
			return err
		}
		data, err := s.config.Codec.Marshal(updated)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", s.config.EntityName, err)
		}
		temporary, err := createTemporary(filepath.Dir(current.Path), data)
		if err != nil {
			return err
		}
		defer os.Remove(temporary)
		latest, err := s.readStable(current.Path)
		if err != nil {
			return err
		}
		if latest.Fingerprint != current.Fingerprint || !equalDocument(latest.Document, current.Document) {
			return fmt.Errorf("%w while updating %s: %s", ErrChanged, s.config.EntityName, current.Path)
		}
		if err := os.Rename(temporary, current.Path); err != nil {
			return fmt.Errorf("save %s: %w", s.config.EntityName, err)
		}
		result, err = s.readStable(current.Path)
		if err != nil {
			return err
		}
		if err := s.replaceIndexRecords([]Entry{result}, nil); err != nil {
			if rollbackErr := WriteFileAtomic(current.Path, originalData); rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("roll back canonical document: %w", rollbackErr))
			}
			return err
		}
		return nil
	})
	return result, err
}

func (s *Store) Remove(path string, expected Fingerprint) error {
	return s.withMutationLock(func() error {
		current, err := s.readStable(path)
		if err != nil {
			return err
		}
		if current.Fingerprint != expected {
			return fmt.Errorf("%w before removing %s: %s", ErrChanged, s.config.EntityName, current.Path)
		}
		latest, err := s.readStable(current.Path)
		if err != nil {
			return err
		}
		if latest.Fingerprint != current.Fingerprint || !equalDocument(latest.Document, current.Document) {
			return fmt.Errorf("%w before removing %s: %s", ErrChanged, s.config.EntityName, current.Path)
		}
		if err := os.Remove(current.Path); err != nil {
			return fmt.Errorf("remove %s: %w", s.config.EntityName, err)
		}
		return s.replaceIndexRecords(nil, []string{current.Path})
	})
}

func createTemporary(directory string, data []byte) (string, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create canonical directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".markdownstore-write-*")
	if err != nil {
		return "", fmt.Errorf("create temporary document: %w", err)
	}
	path := temporary.Name()
	cleanup := func(cause error) (string, error) {
		temporary.Close()
		os.Remove(path)
		return "", cause
	}
	if err := temporary.Chmod(0o600); err != nil {
		return cleanup(fmt.Errorf("secure temporary document: %w", err))
	}
	if _, err := temporary.Write(data); err != nil {
		return cleanup(fmt.Errorf("write temporary document: %w", err))
	}
	if err := temporary.Sync(); err != nil {
		return cleanup(fmt.Errorf("sync temporary document: %w", err))
	}
	if err := temporary.Close(); err != nil {
		os.Remove(path)
		return "", fmt.Errorf("close temporary document: %w", err)
	}
	return path, nil
}

func (s *Store) readStable(path string) (Entry, error) {
	var lastErr error
	for range 3 {
		entry, err := s.readOnce(path)
		if !errors.Is(err, ErrChanged) {
			return entry, err
		}
		lastErr = err
	}
	return Entry{}, lastErr
}

func (s *Store) readOnce(path string) (Entry, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return Entry{}, fmt.Errorf("resolve %s: %w", path, err)
	}
	file, err := os.Open(absolute)
	if err != nil {
		return Entry{}, fmt.Errorf("open %s: %w", absolute, err)
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return Entry{}, fmt.Errorf("stat open document %s: %w", absolute, err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return Entry{}, fmt.Errorf("read %s: %w", absolute, err)
	}
	frontmatter, body, err := SplitDocument(data)
	if err != nil {
		return Entry{}, fmt.Errorf("parse %s: %w", absolute, err)
	}
	document, err := s.config.Codec.Parse(absolute, frontmatter, body)
	if err != nil {
		return Entry{}, fmt.Errorf("parse %s: %w", absolute, err)
	}
	if err := s.validateDocument(document); err != nil {
		return Entry{}, fmt.Errorf("parse %s: %w", absolute, err)
	}
	after, err := file.Stat()
	if err != nil {
		return Entry{}, fmt.Errorf("stat read document %s: %w", absolute, err)
	}
	pathInfo, err := os.Stat(absolute)
	if err != nil {
		return Entry{}, fmt.Errorf("stat %s: %w", absolute, err)
	}
	if before.Size() != after.Size() || before.ModTime() != after.ModTime() || !os.SameFile(after, pathInfo) {
		return Entry{}, fmt.Errorf("%w while reading %s", ErrChanged, absolute)
	}
	return Entry{
		Document: document, Path: absolute,
		Fingerprint: Fingerprint{Size: after.Size(), ModTimeNS: after.ModTime().UnixNano()},
	}, nil
}
