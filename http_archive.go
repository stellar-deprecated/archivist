// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"io"
	"path"
	"net/http"
	"net/url"
	"errors"
)

type HttpArchiveBackend struct {
	client http.Client
	base url.URL
}

func (b *HttpArchiveBackend) GetFile(pth string) (io.ReadCloser, error) {
	var derived url.URL = b.base
	derived.Path = path.Join(derived.Path, pth)
	resp, err := b.client.Get(derived.String())
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (b *HttpArchiveBackend) Exists(pth string) bool {
	var derived url.URL = b.base
	derived.Path = path.Join(derived.Path, pth)
	resp, err := b.client.Get(derived.String())
	return err != nil && resp.StatusCode >= 200 && resp.StatusCode < 400
}

func (b *HttpArchiveBackend) PutFile(pth string, in io.ReadCloser) error {
	in.Close()
	return errors.New("PutFile not available over HTTP")
}

func (b *HttpArchiveBackend) ListFiles(pth string) (chan string, chan error) {
	ch := make(chan string)
	er := make(chan error)
	close(ch)
	er <- errors.New("ListFiles not available over HTTP")
	close(er)
	return ch, er
}

func (b *HttpArchiveBackend) CanListFiles() bool {
	return false
}

func MakeHttpBackend(base *url.URL, opts *ConnectOptions) ArchiveBackend {
	return &HttpArchiveBackend{
		base: *base,
	}
}
