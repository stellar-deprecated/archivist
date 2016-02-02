// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"io"
	"io/ioutil"
	"path"
	"encoding/json"
	"regexp"
	"strconv"
	"net/url"
	"errors"
	"log"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"strings"
)

const hexPrefixPat = "/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{2}/"
const rootHASPath = ".well-known/stellar-history.json"
const concurrency = 32

type CommandOptions struct {
	Range Range
	DryRun bool
	Force bool
}

type ConnectOptions struct {
	S3Region string
}

type ArchiveBackend interface {
	Exists(path string) bool
	GetFile(path string) (io.ReadCloser, error)
	PutFile(path string, in io.ReadCloser) error
	ListFiles(path string) (chan string, chan error)
	CanListFiles() bool
}

func Categories() []string {
	return []string{ "history", "ledger", "transactions", "results", "scp"}
}

func categoryExt(n string) string {
	if n == "history" {
		return "json"
	} else {
		return "xdr.gz"
	}
}

func categoryRequired(n string) bool {
	return n != "scp"
}

type Archive struct {
	mutex sync.Mutex
	checkpointFiles map[string](map[uint32]bool)
	allBuckets map[Hash]bool
	referencedBuckets map[Hash]bool
	missingBuckets int
	backend ArchiveBackend
}

func (a *Archive) GetPathHAS(path string) (HistoryArchiveState, error) {
	var has HistoryArchiveState
	rdr, err := a.backend.GetFile(path)
	if err != nil {
		return has, err
	}
	dec := json.NewDecoder(rdr)
	err = dec.Decode(&has)
	return has, err
}

func (a *Archive) PutPathHAS(path string, has HistoryArchiveState, opts *CommandOptions) error {
	if a.backend.Exists(path) && !opts.Force {
		log.Printf("skipping existing " + path)
		return nil
	}
	buf, err := json.MarshalIndent(has, "", "    ")
	if err != nil {
		return err
	}
	return a.backend.PutFile(path,
		ioutil.NopCloser(bytes.NewReader(buf)))
}

func CategoryCheckpointPath(cat string, chk uint32) string {
	ext := categoryExt(cat)
	pre := CheckpointPrefix(chk).Path()
	return path.Join(cat, pre, fmt.Sprintf("%s-%8.8x.%s", cat, chk, ext))
}

func BucketPath(bucket Hash) string {
	pre := HashPrefix(bucket)
	return path.Join("bucket", pre.Path(), fmt.Sprintf("bucket-%s.xdr.gz", bucket))
}

func (a *Archive) GetRootHAS() (HistoryArchiveState, error) {
	return a.GetPathHAS(rootHASPath)
}

func (a *Archive) GetCheckpointHAS(chk uint32) (HistoryArchiveState, error) {
	return a.GetPathHAS(CategoryCheckpointPath("history", chk))
}

func (a *Archive) PutCheckpointHAS(chk uint32, has HistoryArchiveState, opts *CommandOptions) error {
	return a.PutPathHAS(CategoryCheckpointPath("history", chk), has, opts)
}

func (a *Archive) PutRootHAS(has HistoryArchiveState, opts *CommandOptions) error {
	return a.PutPathHAS(rootHASPath, has, opts)
}

func (a *Archive) ListBucket(dp DirPrefix) (chan string, chan error) {
	return a.backend.ListFiles(path.Join("bucket", dp.Path()))
}

func (a *Archive) ListAllBuckets() (chan string, chan error) {
	return a.backend.ListFiles("bucket")
}

// Make a goroutine that unconditionally pulls an error channel into
// (unbounded) local memory, and feeds it to a downstream consumer. This is
// slightly hacky, but the alternatives are to either send {val,err} pairs
// along each "primary" channel, or else risk a primary channel producer
// stalling because nobody's draining its error channel yet. I (vaguely,
// currently) prefer this approach, though time will tell if it has a
// good taste later.
//
// Code here modeled on github.com/eapache/channels/infinite_channel.go
func makeErrorPump(in chan error) chan error {
	buf := make([]error, 0)
	var next error
	ret := make(chan error)
	go func() {
		var out chan error
		for in != nil || out != nil {
			select {
			case err, ok := <-in:
				if ok {
					buf = append(buf, err)
				} else {
					in = nil
				}
			case out <- next:
				buf = buf[1:]
			}
			if len(buf) > 0 {
				out = ret
				next = buf[0]
			} else {
				out = nil
				next = nil
			}
		}
		close(ret)
	}()
	return ret
}

func noteError(e error) uint32 {
	if e != nil {
		log.Printf("Error: " + e.Error())
		return 1
	}
	return 0
}

func drainErrors(errs chan error) uint32 {
	var count uint32
	for e := range errs {
		count += noteError(e)
	}
	return count
}

func (a *Archive) ListAllBucketHashes() (chan Hash, chan error) {
	sch, errs := a.backend.ListFiles("bucket")
	ch := make(chan Hash)
	rx := regexp.MustCompile("bucket" + hexPrefixPat + "bucket-([0-9a-f]{64})\\.xdr\\.gz$")
	errs = makeErrorPump(errs)
	go func() {
		for s := range sch {
			m := rx.FindStringSubmatch(s)
			if m != nil {
				ch <- MustDecodeHash(m[1])
			}
		}
		close(ch)
	}()
	return ch, errs
}

func (a *Archive) ListCategoryCheckpoints(cat string, pth string) (chan uint32, chan error) {
	ext := categoryExt(cat)
	rx := regexp.MustCompile(cat + hexPrefixPat + cat +
		"-([0-9a-f]{8})\\." + regexp.QuoteMeta(ext) + "$")
	sch, errs := a.backend.ListFiles(path.Join(cat, pth))
	ch := make(chan uint32)
	errs = makeErrorPump(errs)

	go func() {
		for s := range sch {
			m := rx.FindStringSubmatch(s)
			if m != nil {
				i, e := strconv.ParseUint(m[1], 16, 32)
				if e == nil {
					ch <- uint32(i)
				} else {
					errs <- errors.New("decoding checkpoint number in filename " + s)
				}
			}
		}
		close(ch)
	}()
	return ch, errs
}

func Connect(u string, opts *ConnectOptions) (*Archive, error) {
	arch := Archive{
		checkpointFiles:make(map[string](map[uint32]bool)),
		allBuckets:make(map[Hash]bool),
		referencedBuckets:make(map[Hash]bool),
	}
	if opts == nil {
		opts = new(ConnectOptions)
	}
	for _, cat := range Categories() {
		arch.checkpointFiles[cat] = make(map[uint32]bool)
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return &arch, err
	}
	pth := parsed.Path
	if parsed.Scheme == "s3" {
		// Inside s3, all paths start _without_ the leading /
		if len(pth) > 0 && pth[0] == '/' {
			pth = pth[1:]
		}
		arch.backend = MakeS3Backend(parsed.Host, pth, opts)
	} else if parsed.Scheme == "file" {
		pth = path.Join(parsed.Host, pth)
		arch.backend = MakeFsBackend(pth, opts)
	} else if parsed.Scheme == "http" {
		arch.backend = MakeHttpBackend(parsed, opts)
	} else if parsed.Scheme == "mock" {
		arch.backend = MakeMockBackend(opts)
	} else {
		err = errors.New("unknown URL scheme: '" + parsed.Scheme + "'")
	}
	return &arch, err
}

func MustConnect(u string, opts *ConnectOptions) *Archive {
	arch, err := Connect(u, opts)
	if err != nil {
		log.Fatal(err)
	}
	return arch
}

func copyPath(src *Archive, dst *Archive, pth string, opts *CommandOptions) error {
	if opts.DryRun {
		log.Printf("dryrun skipping " + pth)
		return nil
	}
	if dst.backend.Exists(pth) && !opts.Force {
		log.Printf("skipping existing " + pth)
		return nil
	}
	rdr, err := src.backend.GetFile(pth)
	if err != nil {
		return err
	}
	return dst.backend.PutFile(pth, rdr)
}

func makeTicker(onTick func(uint)) chan bool {
	tick := make(chan bool)
	go func() {
		var k uint = 0
		for range tick {
			k++
			if k & 0xfff == 0 {
				onTick(k)
			}
		}
	}()
	return tick
}

func Mirror(src *Archive, dst *Archive, opts *CommandOptions) error {
	rootHAS, e := src.GetRootHAS()
	if e != nil {
		return e
	}

	opts.Range = opts.Range.Clamp(rootHAS.Range())

	log.Printf("copying range %s\n", opts.Range)

	// Make a bucket-fetch map that shows which buckets are
	// already-being-fetched
	bucketFetch := make(map[Hash]bool)
	var bucketFetchMutex sync.Mutex

	var errs uint32
	tick := makeTicker(func(ticks uint) {
		bucketFetchMutex.Lock()
		sz := opts.Range.Size()
		log.Printf("Copied %d/%d checkpoints (%f%%), %d buckets",
			ticks, sz,
			100.0 * float64(ticks)/float64(sz),
			len(bucketFetch))
		bucketFetchMutex.Unlock()
	})


	var wg sync.WaitGroup
	checkpoints := opts.Range.Checkpoints()
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				ix, ok := <- checkpoints
				if !ok {
					break
				}
				has, e := src.GetCheckpointHAS(ix)
				if e != nil {
					atomic.AddUint32(&errs, noteError(e))
					continue
				}
				for _, bucket := range has.Buckets() {
					alreadyFetching := false
					bucketFetchMutex.Lock()
					_, alreadyFetching = bucketFetch[bucket]
					if !alreadyFetching {
						bucketFetch[bucket] = true
					}
					bucketFetchMutex.Unlock()
					if !alreadyFetching {
						pth := BucketPath(bucket)
						e = copyPath(src, dst, pth, opts)
						atomic.AddUint32(&errs, noteError(e))
					}
				}
				e = dst.PutCheckpointHAS(ix, has, opts)
				atomic.AddUint32(&errs, noteError(e))

				for _, cat := range Categories() {
					pth := CategoryCheckpointPath(cat, ix)
					e = copyPath(src, dst, pth, opts)
					atomic.AddUint32(&errs, noteError(e))
				}
				tick <- true
			}
			wg.Done()
		}()
	}

	wg.Wait()
	log.Printf("Copied %d checkpoints, %d buckets",
		opts.Range.Size(), len(bucketFetch))
	close(tick)
	e = dst.PutRootHAS(rootHAS, opts)
	errs += noteError(e)
	if errs != 0 {
		return fmt.Errorf("%d errors while mirroring", errs)
	}
	return nil
}

func Repair(src *Archive, dst *Archive, opts *CommandOptions) error {
	state, e := dst.GetRootHAS()
	if e != nil {
		return e
	}
	opts.Range = opts.Range.Clamp(state.Range())

	log.Printf("Starting scan for repair")
	var errs uint32
	errs += noteError(dst.ScanCheckpoints(opts))

	log.Printf("Examining checkpoint files for gaps")
	missingCheckpointFiles := dst.CheckCheckpointFilesMissing(opts)

	repairedHistory := false
	for cat, missing := range missingCheckpointFiles {
		for _, chk := range missing {
			pth := CategoryCheckpointPath(cat, chk)
			log.Printf("Repairing %s", pth)
			errs += noteError(copyPath(src, dst, pth, opts))
			if cat == "history" {
				repairedHistory = true
			}
		}
	}

	if repairedHistory {
		log.Printf("Re-running checkpoing-file scan, for bucket repair")
		dst.ClearCachedInfo()
		errs += noteError(dst.ScanCheckpoints(opts))
	}

	errs += noteError(dst.ScanBuckets())

	log.Printf("Examining buckets referenced by checkpoints")
	missingBuckets := dst.CheckBucketsMissing()

	for bkt, _ := range missingBuckets {
		pth := BucketPath(bkt)
		log.Printf("Repairing %s", pth)
		errs += noteError(copyPath(src, dst, pth, opts))
	}

	if errs != 0 {
		return fmt.Errorf("%d errors while repairing", errs)
	}
	return nil
}

func (arch* Archive) ClearCachedInfo() {
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	for _, cat := range Categories() {
		arch.checkpointFiles[cat] = make(map[uint32]bool)
	}
	arch.allBuckets = make(map[Hash]bool)
	arch.referencedBuckets = make(map[Hash]bool)
}

func (arch* Archive) ReportCheckpointStats() {
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	s := make([]string, 0)
	for _, cat := range Categories() {
		tab := arch.checkpointFiles[cat]
		s = append(s, fmt.Sprintf("%d %s", len(tab), cat))
	}
	log.Printf("Archive: %s", strings.Join(s, ", "))
}

func (arch* Archive) ReportBucketStats() {
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	log.Printf("Archive: %d buckets total, %d referenced",
		len(arch.allBuckets), len(arch.referencedBuckets))
}

func (arch *Archive) NoteCheckpointFile(cat string, chk uint32, present bool) {
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	arch.checkpointFiles[cat][chk] = present
}

func (arch *Archive) NoteExistingBucket(bucket Hash) {
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	arch.allBuckets[bucket] = true
}

func (arch *Archive) NoteReferencedBucket(bucket Hash) {
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	arch.referencedBuckets[bucket] = true
}

type scanCheckpointReq struct {
	category string
	pathprefix string
}

func (arch *Archive) ScanCheckpoints(opts *CommandOptions) error {
	state, e := arch.GetRootHAS()
	if e != nil {
		return e
	}
	opts.Range = opts.Range.Clamp(state.Range())

	log.Printf("Scanning checkpoint files in range: %s", opts.Range)

	var errs uint32
	tick := makeTicker(func(_ uint){
		arch.ReportCheckpointStats()
	})

	var wg sync.WaitGroup
	wg.Add(concurrency)

	req := make(chan scanCheckpointReq)

	cats := Categories()
	go func() {
		for _, cat := range cats {
			for _, pth := range RangePaths(opts.Range) {
				req <- scanCheckpointReq{category:cat, pathprefix:pth}
			}
		}
		close(req)
	}()

	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				r, ok := <-req
				if !ok {
					break
				}
				ch, es := arch.ListCategoryCheckpoints(r.category, r.pathprefix)
				for n := range ch {
					tick <- true
					arch.NoteCheckpointFile(r.category, n, true)
				}
				atomic.AddUint32(&errs, drainErrors(es))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	close(tick)
	log.Printf("Checkpoint files scanned with %d errors", errs)
	arch.ReportCheckpointStats()
	if errs != 0 {
		return fmt.Errorf("%d errors scanning checkpoints", errs)
	}
	return nil
}

func (arch *Archive) Scan(opts *CommandOptions) error {
	e1 := arch.ScanCheckpoints(opts)
	e2 := arch.ScanBuckets()
	if e1 != nil {
		return e1
	}
	if e2 != nil {
		return e2
	}
	return nil
}

func (arch *Archive) CheckCheckpointFilesMissing(opts *CommandOptions) map[string][]uint32 {
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	missing := make(map[string][]uint32)
	for _, cat := range Categories() {
		missing[cat] = make([]uint32, 0)
		for ix := range opts.Range.Checkpoints() {
			_, ok := arch.checkpointFiles[cat][ix]
			if !ok {
				missing[cat] = append(missing[cat], ix)
			}
		}
	}
	return missing
}


func (arch* Archive) CheckBucketsMissing() map[Hash]bool {
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	missing := make(map[Hash]bool)
	for k, _ := range arch.referencedBuckets {
		_, ok := arch.allBuckets[k]
		if !ok {
			missing[k] = true
		}
	}
	return missing
}

func (arch *Archive) ScanAllBuckets() error {
	log.Printf("Scanning all buckets, and those referenced by range")

	tick := makeTicker(func(_ uint){
		arch.ReportBucketStats()
	})

	allBuckets, ech := arch.ListAllBucketHashes()

	for b := range allBuckets {
		arch.NoteExistingBucket(b)
		tick <- true
	}

	errs := drainErrors(ech)
	if errs != 0 {
		return fmt.Errorf("%d errors while scanning all buckets", errs)
	}
	return nil
}

func (arch *Archive) ScanBuckets() error {

	var errs uint32

	// First scan _all_ buckets
	errs += noteError(arch.ScanAllBuckets())

	// Grab the set of checkpoints we have HASs for, to read references.
	arch.mutex.Lock()
	hists := arch.checkpointFiles["history"]
	seqs := make([]uint32, 0, len(hists))
	for k, present := range hists {
		if present {
			seqs = append(seqs, k)
		}
	}
	arch.mutex.Unlock()

	var wg sync.WaitGroup
	wg.Add(concurrency)

	tick := makeTicker(func(_ uint){
		arch.ReportBucketStats()
	})

	// Make a bunch of goroutines that pull each HAS and enumerate
	// its buckets into a channel. These are the _referenced_ buckets.
	req := make(chan uint32)
	go func() {
		for _, seq := range seqs {
			req <- seq
		}
		close(req)
	}()
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				ix, ok := <- req
				if !ok {
					break
				}
				has, e := arch.GetCheckpointHAS(ix)
				atomic.AddUint32(&errs, noteError(e))
				for _, bucket := range has.Buckets() {
					arch.NoteReferencedBucket(bucket)
				}
				tick <- true
			}
			wg.Done()
		}()
	}

	wg.Wait()
	arch.ReportBucketStats()
	close(tick)
	if errs != 0 {
		return fmt.Errorf("%d errors while scanning buckets", errs)
	}
	return nil
}

func (arch *Archive) ReportMissing(opts *CommandOptions) error {

	state, e := arch.GetRootHAS()
	if e != nil {
		return e
	}
	opts.Range = opts.Range.Clamp(state.Range())

	log.Printf("Examining checkpoint files for gaps")
	missingCheckpointFiles := arch.CheckCheckpointFilesMissing(opts)
	log.Printf("Examining buckets referenced by checkpoints")
	missingBuckets := arch.CheckBucketsMissing()

	missingCheckpoints := false
	for cat, missing := range missingCheckpointFiles {
		s := make([]string, 0)
		if !categoryRequired(cat) {
			continue
		}
		for _, m := range missing {
			s = append(s, fmt.Sprintf("0x%8.8x", m))
		}
		if len(missing) != 0 {
			missingCheckpoints = true
			log.Printf("Missing %s: %s", cat, strings.Join(s, ", "))
		}
	}

	if !missingCheckpoints {
		log.Printf("No checkpoint files missing in range %s", opts.Range)
	}

	for bucket, _ := range missingBuckets {
		log.Printf("Missing bucket: %s", bucket)
	}

	if len(missingBuckets) == 0 {
		log.Printf("No missing buckets referenced in range %s", opts.Range)
	}

	return nil
}
