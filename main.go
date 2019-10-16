package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

const defaultChunkSize = 512 * 1000

// Number of chunks to download at a time.
// Constrained by the number of open network connections the OS can support.
const defaultNWorkers = 16

func main() {
	url := flag.String("url", "", "URL of file to download")
	outname := flag.String("o", "", "Output filename")
	verify := flag.Bool("verify", false, "Whether to verify an MD5 ETag for the file")
	flag.Parse()
	if *url == "" || *outname == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	client := ChunkClient{
		ChunkSize:  defaultChunkSize,
		NWorkers:   defaultNWorkers,
		VerifyETag: *verify,
	}
	outfile, err := os.Create(*outname)
	if err != nil {
		log.Fatal(err)
	}
	err = client.GetFile(*url, outfile)
	if err != nil {
		log.Fatal(err)
	}
}

// ChunkClient makes requests to download a large file over multiple HTTP/HTTPS
// requests.
type ChunkClient struct {
	http.Client
	ChunkSize  int
	VerifyETag bool
	NWorkers   int
}

// GetFile makes chunked request to download the file hosted at `url` to an
// output file.
func (c *ChunkClient) GetFile(url string, out *os.File) error {
	res, err := c.Head(url)
	if err != nil {
		return err
	}
	length := res.ContentLength
	if length < 0 {
		return errors.New("Content-Length header is required")
	}
	etag := res.Header.Get("ETag")

	err = c.getAllChunks(out, url, length)
	if err != nil {
		return err
	}

	if etag != "" {
		hash := md5.New()
		if _, err := io.Copy(hash, out); err != nil {
			return err
		}
		if etag != hex.EncodeToString(hash.Sum(nil)) {
			return errors.New("ETag verification failed")
		}
	}

	return nil
}

// getAllChunks implements a worker pool to do the work of downloading a file
// of a known length in chunks.
func (c *ChunkClient) getAllChunks(out *os.File, url string, length int64) error {
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
			defer func() {
				<-tokens
			}()
			res, err := c.getChunk(url, offset)
			if err != nil {
				errs <- err
				return
			}
			if res.StatusCode != http.StatusPartialContent {
				errs <- fmt.Errorf("chunk at offset %d failed with %d", offset, http.StatusPartialContent)
				return
			}
			_, err = io.Copy(&chunkWriter{out, int64(offset)}, res.Body)
			if err != nil {
				errs <- err
				return
			}
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

// getChunk requests a single chunk of a large file and returns the response.
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
