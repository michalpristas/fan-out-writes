package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"time"

	multierror "github.com/hashicorp/go-multierror"
)

func main() {
	ctx := context.Background()
	blobName := "blob"
	blobReader := bytes.NewReader([]byte("test content"))
	storages := getStorages()

	fmt.Println(uploadBlob(ctx, blobName, blobReader, storages))
}

func uploadBlob(ctx context.Context, blobName string, blobReader io.Reader, storages []*Storage) error {
	var results = make(chan error, len(storages))
	defer close(results)

	readers, closer := getReaders(blobReader, len(storages))

	// compose reader for each backend
	for i, store := range storages {
		go func(s *Storage, rdr io.Reader) {
			select {
			case <-ctx.Done(): // something failed
				break
			case results <- s.Save(ctx, blobName, rdr):
			}
		}(store, readers[i])
	}

	var errs error
	for i := 0; i < len(storages); i++ {
		if r := <-results; r != nil {
			errs = multierror.Append(errs, r)
		}
		if i == 0 {
			closer.Close()
		}
	}
	return errs
}

func getStorages() []*Storage {
	var storages []*Storage

	storages = append(storages, NewStorage("aa"))
	storages = append(storages, NewStorage("bb"))
	storages = append(storages, NewStorage("cc"))

	return storages
}

func getReaders(source io.Reader, count int) ([]io.Reader, io.Closer) {
	readers := make([]io.Reader, 0, count)
	pipeWriters := make([]io.Writer, 0, count)
	pipeClosers := make([]io.Closer, 0, count)

	for i := 0; i < count-1; i++ {
		pr, pw := io.Pipe()
		readers = append(readers, pr)
		pipeWriters = append(pipeWriters, pw)
		pipeClosers = append(pipeClosers, pw)
	}

	multiWriter := io.MultiWriter(pipeWriters...)
	teeReader := io.TeeReader(source, multiWriter)

	// append teereader so it populates data to the rest of the readers
	readers = append([]io.Reader{teeReader}, readers...)

	return readers, NewMultiCloser(pipeClosers)
}

type Storage struct {
	rootDir string
}

func NewStorage(rootDir string) *Storage {
	return &Storage{
		rootDir: rootDir,
	}
}

func (s *Storage) Save(c context.Context, blob string, blobReader io.Reader) error {
	seed := rand.NewSource(time.Now().UnixNano())
	rg := rand.New(seed)
	r := rg.Int() % 1000

	content, _ := ioutil.ReadAll(blobReader)
	fmt.Printf("storage '%s' wrote a file '%s'\n", s.rootDir, content)

	select {
	case <-time.After(time.Duration(r) * time.Microsecond):
		fmt.Println(r)
	case <-c.Done():
		return nil
	}

	if r%2 == 0 {
		return errors.New("oh")
	}

	return nil
}

type MultiCloser struct {
	closers []io.Closer
}

func NewMultiCloser(closers []io.Closer) *MultiCloser {
	return &MultiCloser{
		closers: closers,
	}
}

func (m *MultiCloser) Close() error {
	var err error
	for _, c := range m.closers {
		if e := c.Close(); e != nil {
			err = multierror.Append(err, e)
		}
	}
	return err
}
