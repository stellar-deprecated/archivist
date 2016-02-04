// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"errors"
	"compress/gzip"
	"crypto/sha256"
	"github.com/stellar/go-stellar-base/xdr"
)

type XdrStream struct {
	buf bytes.Buffer
	rdr io.ReadCloser
	rdr2 io.ReadCloser
}

func NewXdrStream(in io.ReadCloser) *XdrStream {
	return &XdrStream{rdr: in}
}

func NewXdrGzStream(in io.ReadCloser) (*XdrStream, error) {
	rdr, err := gzip.NewReader(in)
	if err != nil {
		in.Close()
		return nil, err
	}
	return &XdrStream{rdr: rdr, rdr2: in}, nil
}

func (a *Archive) GetXdrStream(pth string) (*XdrStream, error) {
	if !strings.HasSuffix(pth, ".xdr.gz") {
		return nil, errors.New("File has non-.xdr.gz suffix: " + pth)
	}
	rdr, err := a.backend.GetFile(pth)
	if err != nil {
		return nil, err
	}
	return NewXdrGzStream(rdr)
}

func HashXdr(x interface{}) (Hash, error) {
	var msg bytes.Buffer
	_, err := xdr.Marshal(&msg, x)
	if err != nil {
		var zero Hash
		return zero, err
	}
	return Hash(sha256.Sum256(msg.Bytes())), nil
}

func (x *XdrStream) Close() {
	if x.rdr != nil {
		x.rdr.Close()
	}
	if x.rdr2 != nil {
		x.rdr2.Close()
	}
}

func (x *XdrStream) ReadOne(in interface{}) error {
	var nbytes uint32
	err := binary.Read(x.rdr, binary.BigEndian, &nbytes)
	if err != nil {
		if err == io.EOF {
			x.rdr.Close()
			return io.EOF
		} else {
			x.rdr.Close()
			return err
		}
	}
	nbytes &= 0x7fffffff
	x.buf.Reset()
	if nbytes == 0 {
		x.rdr.Close()
		return io.EOF
	}
	read, err := x.buf.ReadFrom(io.LimitReader(x.rdr, int64(nbytes)))
	if read != int64(nbytes) {
		x.rdr.Close()
		return errors.New("Read wrong number of bytes from XDR")
	}
	if err != nil {
		x.rdr.Close()
		return err
	}

	readi, err := xdr.Unmarshal(&x.buf, in)
	if err != nil {
			x.rdr.Close()
	}
	if int64(readi) != int64(nbytes) {
		return errors.New("Unmarshalled wrong number of bytes from XDR")
	}
	return err
}
