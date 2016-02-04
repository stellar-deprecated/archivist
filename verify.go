// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"fmt"
	"io"
	"log"
	"bytes"
	"sort"
	"crypto/sha256"
	"github.com/stellar/go-stellar-base/xdr"
)

// Transaction sets are sorted in two different orders: one for hashing and
// one for applying. Hash order is just the lexicographic order of the
// hashes of the txs themselves. Apply order is built on top, by xoring
// each tx hash with the set-hash (to defeat anyone trying to force a given
// apply sequence), and sub-ordering by account sequence number.
//
// TxSets are stored in the XDR file in apply-order, but we want to sort
// them here back in simple hash order so we can confirm the hash value
// agreed-on by SCP.
//
// Moreover, txsets (when sorted) are _not_ hashed by simply hashing the
// XDR; they have a slightly-more-manual hashing process.

type ByHash struct {
	txe []xdr.TransactionEnvelope
	hsh []Hash
}

func (h *ByHash) Len() int { return len(h.hsh) }
func (h *ByHash) Swap(i, j int) {
	h.txe[i], h.txe[j] = h.txe[j], h.txe[i]
	h.hsh[i], h.hsh[j] = h.hsh[j], h.hsh[i]
}
func (h *ByHash) Less(i, j int) bool {
	return bytes.Compare(h.hsh[i][:], h.hsh[j][:]) < 0
}

func SortTxsForHash(txset *xdr.TransactionSet) error {
	bh := &ByHash{
		txe:txset.Txs,
		hsh:make([]Hash, len(txset.Txs)),
	}
	for i, tx := range txset.Txs {
		h, err := HashXdr(&tx)
		if err != nil {
			return err
		}
		bh.hsh[i] = h
	}
	sort.Sort(bh)
	return nil
}

func HashTxSet(txset *xdr.TransactionSet) (Hash, error) {
	err := SortTxsForHash(txset)
	var h Hash
	if err != nil {
		return h, err
	}
	hsh := sha256.New()
	hsh.Write(txset.PreviousLedgerHash[:])

	for _, env := range txset.Txs {
		_, err := xdr.Marshal(hsh, &env)
		if err != nil {
			return h, err
		}
	}
	sum := hsh.Sum([]byte{})
	copy(h[:], sum[:])
	return h, nil
}

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
	arch.actualLedgerHashes[seq] = h
	arch.expectLedgerHashes[seq - 1] = Hash(entry.Header.PreviousLedgerHash)
	arch.expectTxSetHashes[seq] = Hash(entry.Header.ScpValue.TxSetHash)
	return nil
}

func (arch *Archive) VerifyTransactionHistoryEntry(entry *xdr.TransactionHistoryEntry) error {
	h, err := HashTxSet(&entry.TxSet)
	if err != nil {
		return err
	}
	arch.mutex.Lock()
	defer arch.mutex.Unlock()
	arch.actualTxSetHashes[uint32(entry.LedgerSeq)] = h
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
	if err != nil {
		return err
	}
	defer rdr.Close()

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
	return nil
}

func compareHashMaps(expect map[uint32]Hash, actual map[uint32]Hash, ty string) error {
	n := 0
	for eledger, ehash := range expect {
		ahash, ok := actual[eledger]
		if ok && ahash != ehash {
			n++
			log.Printf("Mismatched hash on %s %8.8x: expected %s, got %s",
				ty, eledger, ehash, ahash)
		}
	}
	log.Printf("Verified %d %ss have expected hashes", len(expect) - n, ty)
	if n != 0 {
		return fmt.Errorf("Found %d mismatched %ss", n, ty)
	}
	return nil
}

func (arch *Archive) ReportInvalid(opts *CommandOptions) error {
	if !opts.Verify {
		return nil
	}

	arch.mutex.Lock()
	defer arch.mutex.Unlock()

	n := noteError(compareHashMaps(arch.expectLedgerHashes,
		arch.actualLedgerHashes, "ledger header"))

	n += noteError(compareHashMaps(arch.expectTxSetHashes,
		arch.actualTxSetHashes, "transaction set"))

	if n != 0 {
		return fmt.Errorf("Found %d validity errors", n)
	}
	return nil
}
