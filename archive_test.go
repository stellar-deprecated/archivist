// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"fmt"
	"testing"
	"crypto/rand"
	"crypto/sha256"
	"bytes"
	"io/ioutil"
	"os"
	"math/big"
)

func GetTestS3Archive() *Archive {
	mx := big.NewInt(0xffffffff)
	r, e := rand.Int(rand.Reader, mx)
	if e != nil {
		panic(e)
	}
	return MustConnect(fmt.Sprintf("s3://history-stg.stellar.org/dev/archivist/test-%s", r),
		&ConnectOptions{S3Region: "eu-west-1",})
}

func GetTestMockArchive() *Archive {
	return MustConnect("mock://test", nil)
}

func GetTestFileArchive() *Archive {
	d, e := ioutil.TempDir("/tmp", "archivist")
	if e != nil {
		panic(e)
	}
	return MustConnect("file://" + d, nil)
}

func GetTestArchive() *Archive {
	ty := os.Getenv("ARCHIVIST_TEST_TYPE")
	if ty == "file" {
		return GetTestFileArchive()
	} else if ty == "s3" {
		return GetTestS3Archive()
	} else {
		return GetTestMockArchive()
	}
}

func (arch *Archive) AddRandomBucket() (Hash, error) {
	var h Hash
	buf := make([]byte, 1024)
	_, e := rand.Read(buf)
	if e != nil {
		return h, e
	}
	h = sha256.Sum256(buf)
	pth := BucketPath(h)
	e = arch.backend.PutFile(pth, ioutil.NopCloser(bytes.NewReader(buf)))
	return h, e
}

func (arch *Archive) AddRandomCheckpointFile(cat string, chk uint32) error {
	buf := make([]byte, 1024)
	_, e := rand.Read(buf)
	if e != nil {
		return e
	}
	pth := CategoryCheckpointPath(cat, chk)
	return arch.backend.PutFile(pth, ioutil.NopCloser(bytes.NewReader(buf)))
}

func (arch *Archive) AddRandomCheckpoint(chk uint32) error {
	opts := &CommandOptions{Force:true}
	for _, cat := range Categories() {
		if cat == "history" {
			var has HistoryArchiveState
			has.CurrentLedger = chk
			for i := 0; i < NumLevels; i++ {
				curr, e := arch.AddRandomBucket()
				if e != nil {
					return e
				}
				snap, e := arch.AddRandomBucket()
				if e != nil {
					return e
				}
				next, e := arch.AddRandomBucket()
				if e != nil {
					return e
				}
				has.CurrentBuckets[i].Curr = curr.String()
				has.CurrentBuckets[i].Snap = snap.String()
				has.CurrentBuckets[i].Next.Output = next.String()
			}
			arch.PutCheckpointHAS(chk, has, opts)
			arch.PutRootHAS(has, opts)
		} else {
			arch.AddRandomCheckpointFile(cat, chk)
		}
	}
	return nil
}

func (arch *Archive) PopulateRandomRange(rng Range) error {
	for chk := range rng.Checkpoints() {
		if e := arch.AddRandomCheckpoint(chk); e != nil {
			return e
		}
	}
	return nil
}

func testRange() Range {
	return Range{Low:63, High:0x3bf}
}

func GetRandomPopulatedArchive() *Archive {
	a := GetTestArchive()
	a.PopulateRandomRange(testRange())
	return a
}

func TestScan(t *testing.T) {
	opts := &CommandOptions{Range:testRange()}
	GetRandomPopulatedArchive().Scan(opts)
}

func TestMirror(t *testing.T) {
	opts := &CommandOptions{Range:testRange()}
	src := GetRandomPopulatedArchive()
	dst := GetTestArchive()
	Mirror(src, dst, opts)
}

