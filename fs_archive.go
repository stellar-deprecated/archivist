// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"io"
	"os"
	"path"
	"path/filepath"
)

type FsArchiveBackend struct {
	prefix string
}

func (b *FsArchiveBackend) GetFile(pth string) (io.ReadCloser, error) {
	return os.Open(path.Join(b.prefix, pth))
}

func exists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil { return true, nil }
    if os.IsNotExist(err) { return false, nil }
    return true, err
}

func (b *FsArchiveBackend) PutFile(pth string, in io.ReadCloser) error {
	pth = path.Join(b.prefix, pth)
	dir := path.Dir(pth)
	ex, e := exists(dir)
	if e != nil {
		return e
	}
	if !ex {
		if e := os.MkdirAll(dir, 0755); e != nil {
			return e
		}
	}
	out, e := os.Create(pth)
	if e != nil {
		return e
	}
	_, e = io.Copy(out, in)
	in.Close()
	out.Close()
	return e
}

func (b *FsArchiveBackend) ListFiles(pth string) (chan string, chan error) {
	ch := make(chan string)
	errs := make(chan error)
	go func() {
		filepath.Walk(path.Join(b.prefix, pth),
			func(p string, info os.FileInfo, err error) error {
				if err != nil {
					errs <- err
					return nil
				}
				if info != nil && ! info.IsDir() {
					ch <- p
				}
				return nil
			})
		close(ch)
		close(errs)
	}()
	return ch, errs
}

func MakeFsBackend(pth string) ArchiveBackend {
	return &FsArchiveBackend{
		prefix: pth,
	}
}
