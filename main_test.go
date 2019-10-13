package main

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

var ts *httptest.Server

const testFile = "/frankenstein.txt"

func TestMain(m *testing.M) {
	ts = httptest.NewServer(
		http.HandlerFunc(etagFileServer),
	)
	defer ts.Close()
	os.Exit(m.Run())
}

func etagFileServer(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Range") == "" {
		file, err := os.Open("fixtures" + r.URL.Path) // Testing only!
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		hash := md5.New()
		if _, err := io.Copy(hash, file); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("ETag", hex.EncodeToString(hash.Sum(nil)))
	}
	http.FileServer(http.Dir("fixtures")).ServeHTTP(w, r)
}

func TestGetChunk(t *testing.T) {
	c := ChunkClient{Client: http.Client{}}
	res, err := c.getChunk(ts.URL+testFile, 316882, 316929)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusPartialContent {
		t.Fatalf("Expected status %d, got %d", http.StatusPartialContent, res.StatusCode)
	}
	body, err := ioutil.ReadAll(res.Body)

	if string(body) != "Beware, for I am fearless and therefore powerful" {
		t.Fatalf("Unexpected body:\n%s", body)
	}
}

func TestChunkedGet(t *testing.T) {
	// For testing, use a smaller file and a smaller chunk size.
	c := ChunkClient{
		Client:    http.Client{},
		NWorkers:  nWorkers,
		ChunkSize: 256,
	}
	err := c.GetFile(ts.URL + testFile)
	if err != nil {
		t.Fatal(err)
	}
}

func TestChunkedGetVerifyETag(t *testing.T) {
	c := ChunkClient{
		Client:     http.Client{},
		NWorkers:   nWorkers,
		ChunkSize:  256,
		VerifyETag: true,
	}
	err := c.GetFile(ts.URL + testFile)
	if err != nil {
		t.Fatal(err)
	}
}

func TestChunkedGetTLS(t *testing.T) {
	tlsServer := httptest.NewTLSServer(
		http.HandlerFunc(etagFileServer),
	)
	defer ts.Close()
	c := ChunkClient{
		Client: http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
		NWorkers:   nWorkers,
		ChunkSize:  256,
		VerifyETag: true,
	}
	err := c.GetFile(tlsServer.URL + testFile)
	if err != nil {
		t.Fatal(err)
	}
}
