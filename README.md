# Chunked File Downloader

A client for downloading a large file over multiple web requests.

## Build
```
go build
```

## Run

Flags:
```
  -o string
    	Output filename
  -url string
    	URL of file to download
  -verify
    	Whether to verify an MD5 ETag for the file
```

Example usage:
```
./chunked-downloader -url https://getlantern.org/bigfile.mp3 -o bigfile.mp3 -verify true
```
