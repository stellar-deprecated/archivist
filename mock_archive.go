// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"bytes"
	"io"
	"io/ioutil"
	"errors"
	"sync"
	"log"
)

type MockArchiveBackend struct {
	mutex sync.Mutex
	files map[string][]byte
	dryrun bool
}

func (b *MockArchiveBackend) GetFile(pth string) (io.ReadCloser, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	var buf []byte
	buf, ok := b.files[pth]
	if !ok {
		return nil, errors.New("no such file: " + pth)
	}
	return ioutil.NopCloser(bytes.NewReader(buf)), nil
}

func (b *MockArchiveBackend) PutFile(pth string, in io.ReadCloser) error {
	if b.dryrun {
		log.Printf("dryrun: put file %s", pth)
		in.Close()
		return nil
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	buf, e := ioutil.ReadAll(in)
	if e != nil {
		return e
	}
	b.files[pth] = buf
	return nil
}

func (b *MockArchiveBackend) ListFiles(pth string) (chan string, chan error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	ch := make(chan string)
	errs := make(chan error)
	files := make([]string, 0, len(b.files))
	for k, _ := range b.files {
		files = append(files, k)
	}
	go func() {
		for _, f := range files {
			ch <- f
		}
		close(ch)
		close(errs)
	}()
	return ch, errs
}

func MakeMockBackend(opts *ConnectOptions) ArchiveBackend {
	b := new(MockArchiveBackend)
	b.files = make(map[string][]byte)
	b.dryrun = opts.DryRun
	return b
}
