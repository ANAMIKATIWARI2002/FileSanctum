package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"path/filepath"
	"regexp"
	"net"
	"sync"
)

const defaultRootFolderName = "ggnetwork"

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize := 5
	sliceLen := len(hashStr) / blocksize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blocksize, (i*blocksize)+blocksize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	PathName string
	Filename string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName, "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Filename)
}

type StoreOpts struct {
	// Root is the folder name of the root, containing all the folders/files of the system.
	Root              string
	PathTransformFunc PathTransformFunc
	ListenAddr string
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts
	ListenAddr string
    Root       string
	storageDir string  // Add this field
    networkDir string  // Add this field
    peers      map[string]net.Conn
    mu         sync.Mutex
	PathTransformFunc func(string) PathKey 
	
}


func NewStore(opts StoreOpts) *Store {
    sanitize := func(path string) string {
        return strings.ReplaceAll(path, ":", "_")
    }

    sanitizedRoot := sanitize(opts.Root)
    
    return &Store{
        ListenAddr: opts.ListenAddr,
        Root:       sanitizedRoot,
        storageDir: filepath.Join(sanitizedRoot + "_storage"),
        networkDir: filepath.Join(sanitizedRoot + "_network"),
        peers:      make(map[string]net.Conn),
        PathTransformFunc: DefaultPathTransformFunc, // Add default transform
    }
}

func (s *Store) Has(id string, key string) bool {
	pathKey := s.PathTransformFunc(key)

	sanitize := func(path string) string {
        return strings.ReplaceAll(path, ":", "_")
    }

	pathNameWithRoot := filepath.Join(
        s.storageDir,
        sanitize(id),
        sanitize(pathKey.PathName),
    )
	// fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	fullPathWithRoot := filepath.Join(
        pathNameWithRoot,
        sanitize(pathKey.Filename),
    )

	_, err := os.Stat(fullPathWithRoot)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}


func (s *Store) Delete(id string, key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.Filename)
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FirstPathName())

	return os.RemoveAll(firstPathNameWithRoot)
}

func (s *Store) Write(id string, key string, r io.Reader) (int64, error) {
	return s.writeStream(id, key, r)
}

func (s *Store) WriteDecrypt(encKey []byte, id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	n, err := copyDecrypt(encKey, r, f)
	return int64(n), err
}

func sanitizePathComponent(input string) string {
    // Remove all non-alphanumeric characters except underscores and hyphens
    reg := regexp.MustCompile(`[^a-zA-Z0-9_-]+`)
    safe := reg.ReplaceAllString(input, "")
    
    // Ensure Windows reserved names are handled
    if strings.HasPrefix(strings.ToLower(safe), "con") || 
       strings.HasPrefix(strings.ToLower(safe), "aux") {
        safe = "file_" + safe
    }
    
    return safe
}

func (s *Store) openFileForWriting(id string, key string) (*os.File, error) {
    if s == nil {
        return nil, fmt.Errorf("store is nil")
    }
    
    if s.PathTransformFunc == nil {
        return nil, fmt.Errorf("PathTransformFunc is not initialized")
    }

    pathKey := s.PathTransformFunc(key)
    
    sanitize := func(path string) string {
        return strings.ReplaceAll(path, ":", "_")
    }

    pathNameWithRoot := filepath.Join(
        s.storageDir,
        sanitize(id),
        sanitize(pathKey.PathName),
    )

    if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
        return nil, err
    }

    fullPathWithRoot := filepath.Join(
        pathNameWithRoot,
        sanitize(pathKey.Filename),
    )

    return os.Create(fullPathWithRoot)
}

func (s *Store) writeStream(id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}
	return io.Copy(f, r)
}



func (s *Store) Read(id string, key string) (int64, io.Reader, error) {
	return s.readStream(id, key)
}

func (s *Store) readStream(id string, key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)

	sanitize := func(path string) string {
        return strings.ReplaceAll(path, ":", "_")
    }

	pathNameWithRoot := filepath.Join(
        s.storageDir,
        sanitize(id),
        sanitize(pathKey.PathName),
    )

	fullPathWithRoot := filepath.Join(
        pathNameWithRoot,
        sanitize(pathKey.Filename),
    )
	// fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	file, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), file, nil
}
