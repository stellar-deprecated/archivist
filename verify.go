// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"fmt"
	"io"
	"github.com/stellar/go-stellar-base/xdr"
)

func (arch *Archive) VerifyLedgerHeaderHistoryEntry(entry *xdr.LedgerHeaderHistoryEntry) error {
	h, err := HashXdr(&entry.Header)
	if err != nil {
		return err
	}
	if h != Hash(entry.Hash) {
		return fmt.Errorf("Ledger %d expected hash %s, got %s",
			entry.Header.LedgerSeq, Hash(entry.Hash), Hash(h))
	}
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	seq := uint32(entry.Header.LedgerSeq)
	arch.foundLedgerHashes[seq] = h
	arch.expectedLedgerHashes[seq - 1] = Hash(entry.Header.PreviousLedgerHash)
	return nil
}

func (arch *Archive) VerifyTransactionHistoryEntry(entry *xdr.TransactionHistoryEntry) error {
	return nil
}

func (arch *Archive) VerifyTransactionHistoryResultEntry(entry *xdr.TransactionHistoryResultEntry) error {
	return nil
}


func (arch *Archive) VerifyCategoryCheckpoint(cat string, chk uint32) error {

	if cat == "history" {
		return nil
	}

	rdr, err := arch.GetXdrStream(CategoryCheckpointPath(cat, chk))

	var tmp interface{}
	step := func() error { return nil }

	var lhe xdr.LedgerHeaderHistoryEntry
	var the xdr.TransactionHistoryEntry
	var thre xdr.TransactionHistoryResultEntry

	switch cat {
	case "ledger":
		tmp = &lhe
		step = func() error {
			return arch.VerifyLedgerHeaderHistoryEntry(&lhe)
		}
	case "transactions":
		tmp = &the
		step = func() error {
			return arch.VerifyTransactionHistoryEntry(&the)
		}
	case "results":
		tmp = &thre
		step = func() error {
			return arch.VerifyTransactionHistoryResultEntry(&thre)
		}
	default:
		return nil
	}

	for {
		if err = rdr.ReadOne(&tmp); err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}
		if err = step(); err != nil {
			return err
		}
	}
	rdr.Close()
	return nil
}

