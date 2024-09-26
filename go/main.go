package main

import (
	"bufio"
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
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/cpuid/v2"
	"github.com/minio/sha256-simd"
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
	writer io.StringWriter
}

func NewMuxWriter(writer io.StringWriter) *MuxWriter {

	return &MuxWriter{
		mux:    &sync.Mutex{},
		writer: writer,
	}
}

func (mw *MuxWriter) WriteString(s string) (int, error) {
	mw.mux.Lock()
	defer mw.mux.Unlock()
	return mw.writer.WriteString(s)
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
			MikeByteSize(bytesRead),
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

type Progress struct {
	bytesRead *uint64
}

func (p *Progress) Write(b []byte) (int, error) {
	atomic.AddUint64(p.bytesRead, uint64(len(b)))
	return len(b), nil
}

func hashFiles(files <-chan ToHash, lenRootDir int, stats *Stats, hashWriter io.StringWriter, errFunc func(error), wg *sync.WaitGroup) {
	defer wg.Done()
	hash := sha256.New()

	progress := Progress{bytesRead: &stats.bytesRead}

	for file := range files {
		fp, err := openreadonly(file.path)
		if err != nil {
			errFunc(err)
		} else {
			hash.Reset()
			teeReader := io.TeeReader(fp, &progress)
			_, err := io.Copy(hash, teeReader)
			fp.Close()
			if err != nil {
				errFunc(err)
			} else {
				atomic.AddUint64(&stats.filesRead, 1)
				//atomic.AddUint64(&stats.bytesRead, uint64(written)) // now happens in the TeeReader
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
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to this file")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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

	fpErr, err := os.Create("errors.txt")
	if err != nil {
		panic(err)
	}
	defer fpErr.Close()
	errWriter := NewMuxWriter(fpErr)

	fpHashes, err := os.Create("hashes.txt")
	if err != nil {
		panic(err)
	}
	hashWriter := bufio.NewWriterSize(fpHashes, 128*1024)
	defer func() {
		hashWriter.Flush()
		fpHashes.Close()
	}()
	hashMuxWriter := NewMuxWriter(hashWriter)

	var stats = Stats{}

	errFunc := createErrorFunc(errWriter, &stats.errors)

	files := make(chan ToHash, 1024)

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
		go hashFiles(files, lenRootDir, &stats, hashMuxWriter, errFunc, &wgHasher)
	}

	go printStats(&stats, 2)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fpErr.Close()
		hashWriter.Flush()
		fpHashes.Close()
		os.Exit(99)
	}()

	start := time.Now()
	wgHasher.Wait()

	fmt.Printf(
		"\nfiles      %d"+
			"\ndata       %v"+
			"\nerrors      %d"+
			"\nduration   %s\n",
		atomic.LoadUint64(&stats.filesRead),
		MikeByteSize(atomic.LoadUint64(&stats.bytesRead)),
		atomic.LoadUint64(&stats.errors),
		time.Since(start))

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
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
	// Print basic CPU information:
	fmt.Println("Name:", cpuid.CPU.BrandName)
	fmt.Println("Family", cpuid.CPU.Family, "Model:", cpuid.CPU.Model, "Vendor ID:", cpuid.CPU.VendorID)
	fmt.Printf("PhysicalCores:        %12d\n", cpuid.CPU.PhysicalCores)
	fmt.Printf("ThreadsPerCore:       %12d\n", cpuid.CPU.ThreadsPerCore)
	fmt.Printf("LogicalCores:         %12d\n", cpuid.CPU.LogicalCores)
	fmt.Printf("Cacheline bytes:      %12d\n", cpuid.CPU.CacheLine)
	fmt.Printf("L1 Data Cache:        %12d bytes %12s\n", cpuid.CPU.Cache.L1D, MikeByteSize(uint64(cpuid.CPU.Cache.L1D)))
	fmt.Printf("L1 Instruction Cache: %12d bytes %12s\n", cpuid.CPU.Cache.L1I, MikeByteSize(uint64(cpuid.CPU.Cache.L1I)))
	fmt.Printf("L2 Cache:             %12d bytes %12s\n", cpuid.CPU.Cache.L2, MikeByteSize(uint64(cpuid.CPU.Cache.L2)))
	fmt.Printf("L3 Cache:             %12d bytes %12s\n", cpuid.CPU.Cache.L3, MikeByteSize(uint64(cpuid.CPU.Cache.L3)))
	fmt.Println("Features:", strings.Join(cpuid.CPU.FeatureSet(), ","))

	fmt.Printf("started %d hash workers\n", workers)

	var last uint64 = 0
	for {
		time.Sleep(1 * time.Second)
		curr := atomic.LoadUint64(&bytesHashed)
		diff := curr - last
		fmt.Printf("bytes/s\t%12s\t%12d\n", MikeByteSize(diff), diff)
		last = curr
	}
}
