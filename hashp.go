package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	filesRead uint64
	bytesRead uint64
}

var stats = Stats{}

type ToHash struct {
	path string
	size int64
}

func printErr(api string, err error, message string) {
	fmt.Printf("E: %s, %s, %s\n", api, err.Error(), message)
}

func printStats(stats *Stats) {
	fmt.Printf("files: %d\t%d MB\n",
		atomic.LoadUint64(&stats.filesRead),
		atomic.LoadUint64(&stats.bytesRead)/1024/1024)
}

func enumerate(directoryname string, files chan<- ToHash) {

	defer close(files)

	walkErr := filepath.Walk(directoryname, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
		}

		if info.IsDir() {
			// intentionally left blank
		} else {
			files <- ToHash{path, info.Size()}
		}

		return nil
	})

	if walkErr != nil {
		printErr("filepath.Walk", walkErr, directoryname)
	}
}

type HashResult struct {
	filename string
	filesize int64
	hash     []byte
}

func hashWriter(filename string, rootDir string, hashes <-chan HashResult, wg *sync.WaitGroup) {
	defer wg.Done()

	lenRootDir := len(rootDir)

	fp, err := os.Create(filename)
	if err != nil {
		panic(fmt.Sprintf("could not create result file for hashes. err: %s", err))
	} else {
		defer fp.Close()

		for hash := range hashes {
			relativeFilename := hash.filename[lenRootDir+1:]
			fp.WriteString(fmt.Sprintf("%s %12d %s\n", hex.EncodeToString(hash.hash), hash.filesize, relativeFilename))
		}
	}
}

func hasher(files <-chan ToHash, filedata <-chan []byte, hashes chan<- HashResult) {

	h := sha256.New()

	for file := range files {
		for data := range filedata {
			if len(data) == 0 {
				hashes <- HashResult{file.path, file.size, h.Sum(nil)}
				h.Reset()
				break
			} else {
				h.Write(data)
			}
		}
	}
}

func readFileSendToHasher(file ToHash, hasherFiles chan<- ToHash, hasherData chan<- []byte, bufs [][]byte) {
	fp, err := os.Open(file.path)
	if err != nil {
		printErr("open file", err, file.path)
	} else {
		defer fp.Close()

		bufIdx := 0
		sentFilenameToHasher := false
		for {
			bufIdx = 1 - bufIdx
			buf := bufs[bufIdx]
			numberRead, err := fp.Read(buf[:])
			if err != nil && !errors.Is(err, io.EOF) {
				printErr("read file", err, "")
				break
			} else {
				if !sentFilenameToHasher {
					// sent filename to hasher this late because of trouble with locked files
					// giving an error
					hasherFiles <- file
					sentFilenameToHasher = true
				}

				atomic.AddUint64(&stats.bytesRead, uint64(numberRead))

				hasherData <- buf[:numberRead]

				if numberRead == 0 {
					break
				}
			}
		}
		atomic.AddUint64(&stats.filesRead, 1)
	}
}

func readFiles(files <-chan ToHash, hashes chan<- HashResult, bufsize int, wg *sync.WaitGroup) {

	defer wg.Done()

	hasherFiles := make(chan ToHash)
	hasherData := make(chan []byte) // a channel with ONLY 1 item possible!!! due to "double buffering" with read

	go hasher(hasherFiles, hasherData, hashes)

	bufs := make([][]byte, 2)
	bufs[0] = make([]byte, bufsize)
	bufs[1] = make([]byte, bufsize)

	for file := range files {
		readFileSendToHasher(file, hasherFiles, hasherData, bufs)
	}
	close(hasherData)
	close(hasherFiles)
}

func main() {

	var workers int
	var bufsize int
	defaultWorker := runtime.NumCPU()
	flag.IntVar(&workers, "w", defaultWorker, "number of workers (Number CPUs)")
	flag.IntVar(&bufsize, "b", 4096, "buffersize read")
	flag.Parse()

	if len(flag.Args()) != 1 {
		flag.Usage()
		os.Exit(4)
	}

	dir2hash, err := filepath.Abs(flag.Arg(0))
	if err != nil {
		printErr("filepath.Abs", err, flag.Arg(0))
	}

	const MAX_ENUMERATE = 100

	var wgWriter sync.WaitGroup

	// channel to the writer of hashes
	hashes := make(chan HashResult, 128)
	wgWriter.Add(1)
	go hashWriter("./hashes.txt", dir2hash, hashes, &wgWriter)

	// channel from enumerate to read files
	files := make(chan ToHash, MAX_ENUMERATE)

	var wgReader sync.WaitGroup
	for i := 0; i < workers; i++ {
		wgReader.Add(1)
		go readFiles(files, hashes, bufsize, &wgReader)
	}

	go enumerate(dir2hash, files)

	finished := make(chan struct{})
	go func() {
		// wait for all readers to finish
		wgReader.Wait()
		// signal END to HashWriter
		close(hashes)

		wgWriter.Wait()
		close(finished)
	}()

loop:
	for {
		select {
		case <-finished:
			printStats(&stats)
			break loop
		case <-time.After(2 * time.Second):
			printStats(&stats)
		}
	}
}
