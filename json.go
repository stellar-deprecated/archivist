// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package archivist

import (
	"os"
	"io"
	"fmt"
	"path"
	"strings"
	"encoding/json"
	"github.com/stellar/go-stellar-base/xdr"
)

func DumpXdrAsJson(args []string) error {
	var lhe xdr.LedgerHeaderHistoryEntry
	var the xdr.TransactionHistoryEntry
	var thre xdr.TransactionHistoryResultEntry
	var bke xdr.BucketEntry
	var tmp interface{}

	for _, arg := range args {
		rdr, err := os.Open(arg)
		if err != nil {
			return err
		}
		base := path.Base(arg)
		if strings.HasPrefix(base, "bucket") {
			tmp = &bke
		} else if strings.HasPrefix(base, "ledger") {
			tmp = &lhe
		} else if strings.HasPrefix(base, "transactions") {
			tmp = &the
		} else if strings.HasPrefix(base, "results") {
			tmp = &thre
		} else {
			return fmt.Errorf("Error: unrecognized XDR file type %s", base)
		}
		xr := NewXdrStream(rdr)
		n := 0
		for {
			if err = xr.ReadOne(&tmp); err != nil {
				if err == io.EOF {
					break
				} else {
					return fmt.Errorf("Error on XDR record %d of %s",
						n, arg)
				}
			}
			n++
			buf, err := json.MarshalIndent(tmp, "", "    ")
			if err != nil {
				return err
			}
			os.Stdout.Write(buf)
		}
		xr.Close()
	}
	return nil
}
