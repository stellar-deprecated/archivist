// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"fmt"
	"bytes"
	"encoding/binary"
	"crypto/sha256"
	"io"
	"compress/gzip"
	"github.com/stellar/go-stellar-base/xdr"
)

func (arch *Archive) VerifyCategoryCheckpoint(cat string, chk uint32) error {
	if cat == "history" {
		return nil
	}
	pth := CategoryCheckpointPath(cat, chk)
	rdr, err := arch.backend.GetFile(pth)
	if err != nil {
		return err
	}

	rdr, err = gzip.NewReader(rdr)
	if err != nil {
		return err
	}

	var tmp interface{}
	var lhe xdr.LedgerHeaderHistoryEntry
	scn := func() error { return nil }

	n := 0

	switch cat {
	case "ledger":
		tmp = &lhe
		scn = func() error {
			var msg bytes.Buffer
			_, err = xdr.Marshal(&msg, &lhe.Header)
			if err != nil {
				return err
			}
			h := sha256.Sum256(msg.Bytes())
			if h != lhe.Hash {
				return fmt.Errorf("Ledger %d expected hash %s, got %s",
					lhe.Header.LedgerSeq, Hash(lhe.Hash), Hash(h))
			}
			arch.mutex.Lock()
			defer arch.mutex.Unlock()
			seq := uint32(lhe.Header.LedgerSeq)
			arch.foundLedgerHashes[seq] = h
			arch.expectedLedgerHashes[seq - 1] = Hash(lhe.Header.PreviousLedgerHash)
			return nil
		}
	case "transactions":
		tmp = &xdr.TransactionHistoryEntry{}
	case "results":
		tmp = &xdr.TransactionHistoryResultEntry{}
	default:
		return nil
	}

	for {
		var nbytes uint32
		err := binary.Read(rdr, binary.BigEndian, &nbytes)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		nbytes &= 0x7fffffff
		buf := make([]byte, nbytes)
		count, err := rdr.Read(buf)
		if err != nil {
			return err
		}
		if uint32(count) != nbytes {
			return fmt.Errorf("Wanted %d bytes, got %d", nbytes, count)
		}

		count, err = xdr.Unmarshal(bytes.NewReader(buf), &tmp)
		if err != nil {
			if err == io.EOF || count == 0 {
				break
			} else {
				return err
			}
		}
		if err = scn(); err != nil {
			return err
		}
		n++
	}
	rdr.Close()
	return nil
}

