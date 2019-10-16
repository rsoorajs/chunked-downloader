package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

const chunkSize = 256

// Number of chunks to download at a time.
// Constrained by the number of open network connections the OS can support.
const nWorkers = 16

func main() {
	fmt.Println("vim-go")
}

type ChunkClient struct {
	http.Client
	ChunkSize  int
	VerifyETag bool
	NWorkers   int
}

func (c *ChunkClient) GetFile(url string) error {
	res, err := c.Head(url)
	if err != nil {
		return err
	}
	length := res.ContentLength
	if length < 0 {
		return errors.New("Content-Length header is required")
	}
	etag := res.Header.Get("ETag")

	f, err := os.Create("output")
	if err != nil {
		return err
	}

	err = c.getAllChunks(f, url, length)
	if err != nil {
		return err
	}

	if etag != "" {
		hash := md5.New()
		if _, err := io.Copy(hash, f); err != nil {
			return err
		}
		if etag != hex.EncodeToString(hash.Sum(nil)) {
			return errors.New("ETag verification failed")
		}
	}

	return nil
}

func (c *ChunkClient) getAllChunks(outfile *os.File, url string, length int64) error {
	tokens := make(chan struct{}, c.NWorkers)
	errs := make(chan error, c.NWorkers)
	nChunks := int(length / int64(c.ChunkSize))

	defer func() {
		// Allow all workers to complete before exiting.
		for i := 0; i < c.NWorkers; i++ {
			tokens <- struct{}{}
		}
	}()

	for i := 0; i <= nChunks; i++ {
		tokens <- struct{}{}
		offset := i * c.ChunkSize
		go func() {
			res, err := c.getChunk(url, offset)
			if err != nil {
				errs <- err
			}
			if res.StatusCode != http.StatusPartialContent {
				errs <- fmt.Errorf("chunk at offset %d failed with %d", offset, http.StatusPartialContent)
			}
			_, err = io.Copy(&chunkWriter{outfile, int64(offset)}, res.Body)
			if err != nil {
				errs <- err
			}
			<-tokens
		}()
		// Check if there have been any errors before proceeding to the next
		// iteration.
		select {
		case err := <-errs:
			return err
		default:
		}
	}
	return nil
}

func (c *ChunkClient) getChunk(url string, offset int) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return &http.Response{}, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+c.ChunkSize-1))
	return c.Do(req)
}

type chunkWriter struct {
	io.WriterAt
	offset int64
}

func (cw *chunkWriter) Write(b []byte) (int, error) {
	n, err := cw.WriteAt(b, cw.offset)
	cw.offset += int64(n)
	return n, err
}
