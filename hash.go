// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

type Hash [sha256.Size]byte

func DecodeHash(s string) (Hash, error) {
	var h Hash
	hs, err := hex.DecodeString(s)
	if err != nil {
		return h, err
	}
	if len(hs) != sha256.Size {
		return h, errors.New(fmt.Sprintf("unexpected hash size: %d", len(hs)))
	}
	n := copy(h[:], hs)
	if n != sha256.Size {
		return h, errors.New(fmt.Sprintf("copy() returned unexpected count: %d", n))
	}
	return h, nil
}

func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

func MustDecodeHash(s string) Hash {
	h, e := DecodeHash(s)
	if e != nil {
		panic(e)
	}
	return h
}

func (h Hash) IsZero() bool {
	for _, n := range h {
		if n != 0 {
			return false
		}
	}
	return true
}
