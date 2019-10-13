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

	tokens := make(chan struct{}, c.NWorkers)
	f, err := os.Create("output")
	if err != nil {
		return err
	}

	nChunks := int(length / int64(c.ChunkSize))
	for i := 0; i <= nChunks; i++ {
		tokens <- struct{}{}
		offset := i * c.ChunkSize
		go func() {
			res, err := c.getChunk(url, offset, offset+c.ChunkSize-1)
			if err != nil {
				panic(err)
			}
			if res.StatusCode != http.StatusPartialContent {
				panic(fmt.Errorf("chunk at offset %d failed with %d", offset, http.StatusPartialContent))
			}
			_, err = io.Copy(&chunkWriter{f, int64(offset)}, res.Body)
			if err != nil {
				panic(err)
			}
			<-tokens
		}()
	}

	// Ensure that all workers have finished by collecting all tokens.
	for i := 0; i < c.NWorkers; i++ {
		tokens <- struct{}{}
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

func (c *ChunkClient) getChunk(url string, offset int, limit int) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return &http.Response{}, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, limit))
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
