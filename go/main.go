package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	filesRead uint64
	bytesRead uint64
	errors    uint64
}

type ToHash struct {
	path string
	size int64
}

type MuxWriter struct {
	mux    *sync.Mutex
	writer *bufio.Writer
	fp     *os.File
}

func NewMuxWriter(filename string, bufsize int) *MuxWriter {

	fp, err := os.Create(filename)
	if err != nil {
		log.Fatalln(err)
	}

	return &MuxWriter{
		fp:     fp,
		mux:    &sync.Mutex{},
		writer: bufio.NewWriterSize(fp, 64*1024),
	}
}

func (mw *MuxWriter) WriteString(s string) (int, error) {
	mw.mux.Lock()
	defer mw.mux.Unlock()
	return mw.writer.WriteString(s)
}

func (mw *MuxWriter) Close() {
	mw.writer.Flush()
	mw.fp.Close()
}

func ByteCountIEC(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
func printStats(stats *Stats, pauseSecs uint) {

	statsLast := Stats{}

	for {
		time.Sleep(time.Duration(pauseSecs) * time.Second)
		filesRead := atomic.LoadUint64(&stats.filesRead)
		bytesRead := atomic.LoadUint64(&stats.bytesRead)

		bytesReadDiff := bytesRead - statsLast.bytesRead
		filesReadDiff := filesRead - statsLast.filesRead

		fmt.Printf("files: %12d %10s | files/s: %6d %4d MB/s | err: %d\n",
			filesRead,
			ByteCountIEC(bytesRead),
			filesReadDiff/uint64(pauseSecs),
			bytesReadDiff/uint64(pauseSecs)/1024/1024,
			atomic.LoadUint64(&stats.errors))

		statsLast.bytesRead = bytesRead
		statsLast.filesRead = filesRead
	}
}

func enumerate(directoryname string, files chan<- ToHash, errFunc func(error)) {

	defer close(files)

	walkErr := filepath.Walk(directoryname, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			errFunc(err)
		}

		if info.Mode().IsRegular() {
			files <- ToHash{path, info.Size()}
		}

		return nil
	})

	if walkErr != nil {
		panic(fmt.Sprintf("filepath.Walk, %s", walkErr))
	}
}

func hashFiles(files <-chan ToHash, lenRootDir int, stats *Stats, hashWriter *MuxWriter, errFunc func(error), wg *sync.WaitGroup) {
	defer wg.Done()
	hash := sha256.New()

	for file := range files {
		fp, err := openreadonly(file.path)
		if err != nil {
			errFunc(err)
		} else {
			hash.Reset()
			written, err := io.Copy(hash, fp)
			fp.Close()
			if err != nil {
				errFunc(err)
			} else {
				atomic.AddUint64(&stats.filesRead, 1)
				atomic.AddUint64(&stats.bytesRead, uint64(written))
				relativeFilename := file.path[lenRootDir:]
				hashWriter.WriteString(fmt.Sprintf("%s\t%12d\t%s\r\n", hex.EncodeToString(hash.Sum(nil)), file.size, relativeFilename))
			}
		}
	}
}

func createErrorFunc(writer *MuxWriter, errCounter *uint64) func(error) {
	return func(err error) {
		atomic.AddUint64(errCounter, 1)
		writer.WriteString(fmt.Sprintf("%s\n", err.Error()))
	}
}

func main() {

	var workers int
	var hashspeedTest bool
	defaultWorker := runtime.NumCPU()
	flag.IntVar(&workers, "w", defaultWorker, "number of workers (number CPUs)")
	flag.BoolVar(&hashspeedTest, "t", false, "test the speed of hashing")
	flag.Parse()

	if hashspeedTest {
		testHashSpeed(workers)
		os.Exit(0)
	}

	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Usage of %s: [OPTS] {directory|file}\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(4)
	}

	cleanPath, err := filepath.Abs(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	pathToHashStat, err := os.Stat(cleanPath)
	if err != nil {
		log.Fatal(err)
	}

	files := make(chan ToHash, 1024)
	errWriter := NewMuxWriter("errors.txt", 64*1024)
	hashWriter := NewMuxWriter("hashes.txt", 128*1024)
	defer errWriter.Close()
	defer hashWriter.Close()

	var stats = Stats{}

	errFunc := createErrorFunc(errWriter, &stats.errors)

	var lenRootDir int
	if pathToHashStat.IsDir() {
		lenRootDir = len(cleanPath)
		if strings.HasSuffix(cleanPath, "\\") {
			lenRootDir -= 1
		}
		fmt.Printf("directory to hash: %s\n", cleanPath)
		go enumerate(cleanPath, files, errFunc)
	} else {
		lenRootDir = len(filepath.Dir(cleanPath))
		fmt.Printf("FILE: dir: %s, len: %d\n", cleanPath, lenRootDir)
		files <- ToHash{path: cleanPath, size: pathToHashStat.Size()}
		close(files)
	}
	lenRootDir += len(string(filepath.Separator))
	var wgHasher sync.WaitGroup
	for i := 0; i < workers; i++ {
		wgHasher.Add(1)
		go hashFiles(files, lenRootDir, &stats, hashWriter, errFunc, &wgHasher)
	}

	go printStats(&stats, 2)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		errWriter.Close()
		hashWriter.Close()
		os.Exit(99)
	}()

	start := time.Now()
	wgHasher.Wait()
	errWriter.writer.Flush()
	hashWriter.writer.Flush()

	fmt.Printf(
		"files      %d"+
			"\ndata       %v"+
			"\nerrors      %d"+
			"\nduration   %s\n",
		atomic.LoadUint64(&stats.filesRead),
		ByteCountIEC(atomic.LoadUint64(&stats.bytesRead)),
		atomic.LoadUint64(&stats.errors),
		time.Since(start))

}

func dummyHash(bytesHashed *uint64) {
	hash := sha256.New()
	data := make([]byte, 4096)

	for i := range data {
		data[i] = 42
	}

	for {
		written, err := hash.Write(data)
		if err != nil {
			panic(err)
		}
		atomic.AddUint64(bytesHashed, uint64(written))
	}
}

func testHashSpeed(workers int) {

	var bytesHashed uint64 = 0

	for i := 0; i < workers; i++ {
		go dummyHash(&bytesHashed)
	}
	fmt.Printf("started %d hash workers\n", workers)

	var last uint64 = 0
	for {
		time.Sleep(1 * time.Second)
		curr := atomic.LoadUint64(&bytesHashed)
		diff := curr - last
		fmt.Printf("bytes/s\t%12s\t%12d\n", ByteCountIEC(diff), diff)
		last = curr
	}
}
