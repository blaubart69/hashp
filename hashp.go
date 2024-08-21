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
	"path"
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

type ToHash struct {
	path string
	size int64
}

type HashData struct {
	len int
	buf []byte
}

type HashResult struct {
	filename string
	filesize int64
	hash     []byte
}

func printErr(api string, err error) {
	fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}

func printStats(stats *Stats, statsLast *Stats, pauseSecs uint) {

	fileRead := atomic.LoadUint64(&stats.filesRead)
	bytesRead := atomic.LoadUint64(&stats.bytesRead)

	bytesReadDiff := bytesRead - statsLast.bytesRead

	fmt.Printf("files: %d\t%d MB\tread: %4d MB/s\n",
		fileRead,
		bytesRead/1024/1024,
		bytesReadDiff/uint64(pauseSecs)/1024/1024)

	statsLast.bytesRead = bytesRead
	statsLast.filesRead = fileRead
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
		printErr("filepath.Walk", walkErr)
	}
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

func hasher(files <-chan ToHash, filedata <-chan HashData, hashes chan<- HashResult, hasherRunning *int32) {

	defer func() {
		if atomic.AddInt32(hasherRunning, -1) == 0 {
			// I'm the last hasher.
			// close the single channel of HashResults
			close(hashes)
		}
	}()

	h := sha256.New()

	for file := range files {
		h.Reset()
		for data := range filedata {
			if data.len == -1 {
				// error readinf the file
				// don't send a result for this file
				break
			} else if data.len == 0 {
				hashes <- HashResult{file.path, file.size, h.Sum(nil)}
				break
			} else {
				h.Write(data.buf)
			}
		}
	}
}

func sendFileToHasher(file ToHash, hasherFiles chan<- ToHash, hasherData chan<- HashData, bufs [][]byte, stats *Stats) {
	fp, err := os.Open(file.path)
	if err != nil {
		printErr("OPEN", err)
	} else {
		defer fp.Close()

		hasherFiles <- file

		bufIdx := 0
		for {
			bufIdx = 1 - bufIdx
			buf := bufs[bufIdx]
			numberRead, err := fp.Read(buf[:])
			if err != nil && !errors.Is(err, io.EOF) {
				hasherData <- HashData{-1, nil} // signal read error to hasher
				printErr("READ", err)
				break
			} else {
				atomic.AddUint64(&stats.bytesRead, uint64(numberRead))
				hasherData <- HashData{numberRead, buf[:numberRead]}

				if numberRead == 0 {
					break
				}
			}
		}
		atomic.AddUint64(&stats.filesRead, 1)
	}
}

func readFilesSendToHasher(files <-chan ToHash, hashes chan<- HashResult, bufsize int, stats *Stats, hasherRunning *int32) {

	hasherFiles := make(chan ToHash)
	hasherData := make(chan HashData) // a channel with ONLY 1 item possible!!! due to "double buffering" with read

	atomic.AddInt32(hasherRunning, 1)
	go hasher(hasherFiles, hasherData, hashes, hasherRunning)

	bufs := make([][]byte, 2)
	bufs[0] = make([]byte, bufsize)
	bufs[1] = make([]byte, bufsize)

	for file := range files {
		sendFileToHasher(file, hasherFiles, hasherData, bufs, stats)
	}
	close(hasherData)
	close(hasherFiles)
}

func getRootDir(pathToHash string, pathToHashStat fs.FileInfo) string {
	if pathToHashStat.IsDir() {
		return pathToHash
	} else {
		rootDir, err := filepath.Abs(path.Dir(pathToHash))
		if err != nil {
			panic(err)
		}
		return rootDir
	}
}

func main() {

	var workers int
	var bufsize int
	defaultWorker := runtime.NumCPU()
	flag.IntVar(&workers, "w", defaultWorker, "number of workers (Number CPUs)")
	flag.IntVar(&bufsize, "b", 4096, "buffersize read")
	flag.Parse()

	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Usage of %s: [OPTS] {directory|file}\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(4)
	}

	pathToHash, err := filepath.Abs(flag.Arg(0))
	if err != nil {
		printErr("filepath.Abs", err)
		os.Exit(8)
	}

	pathToHashStat, err := os.Stat(pathToHash)
	if err != nil {
		printErr("stat", err)
		os.Exit(8)
	}

	var stats = Stats{}
	var statsLast = Stats{}

	// channel to the writer of hashes
	hashes := make(chan HashResult, 128)

	rootDir := getRootDir(pathToHash, pathToHashStat)
	var wgWriter sync.WaitGroup
	wgWriter.Add(1)
	go hashWriter("./hashes.txt", rootDir, hashes, &wgWriter)

	// channel from enumerate to read files
	var MAX_ENUMERATE = defaultWorker * 8
	files := make(chan ToHash, MAX_ENUMERATE)

	var hasherRunning int32 = 0
	for i := 0; i < workers; i++ {
		go readFilesSendToHasher(files, hashes, bufsize, &stats, &hasherRunning)
	}

	if pathToHashStat.IsDir() {
		go enumerate(pathToHash, files)
	} else {
		files <- ToHash{path: pathToHash, size: pathToHashStat.Size()}
		close(files)
	}

	finished := make(chan struct{})
	go func() {
		wgWriter.Wait()
		close(finished)
	}()

	var statsPauseSecs uint = 2
loop:

	for {
		select {
		case <-finished:
			printStats(&stats, &statsLast, statsPauseSecs)
			break loop
		case <-time.After(time.Duration(statsPauseSecs) * time.Second):
			printStats(&stats, &statsLast, statsPauseSecs)
		}
	}
}
