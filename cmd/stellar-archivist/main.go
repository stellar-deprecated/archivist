// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"os"
	"github.com/codegangsta/cli"
	"fmt"
	"log"
	"github.com/stellar/archivist"
)

func status(a string, opts *Options) {
	arch := archivist.MustConnect(a, &opts.ConnectOpts)
	state, e := arch.GetRootHAS()
	if e != nil {
		log.Fatal(e)
	}
	buckets := state.Buckets()
	summ, nz := state.LevelSummary()
	fmt.Printf("\n")
	fmt.Printf("       Archive: %s\n", a)
	fmt.Printf("        Server: %s\n", state.Server)
	fmt.Printf(" CurrentLedger: %d (0x%8.8x)\n", state.CurrentLedger, state.CurrentLedger)
	fmt.Printf("CurrentBuckets: %s (%d nonzero levels)\n", summ, nz)
	fmt.Printf(" Newest bucket: %s\n", buckets[0])
	fmt.Printf("\n")
}

func scan(a string, opts *Options) {
	arch := archivist.MustConnect(a, &opts.ConnectOpts)
	rng := opts.Range(arch)
	e := arch.Scan(rng)
	if e != nil {
		log.Fatal(e)
	}
	e = arch.ReportMissing(rng)
	if e != nil {
		log.Fatal(e)
	}
}

type Options struct {
	Low int
	High int
	Last int
	Force bool
	ConnectOpts archivist.ConnectOptions
}

func (opts *Options) Range(arch *archivist.Archive) archivist.Range {
	if arch != nil && opts.Last != -1 {
		state, e := arch.GetRootHAS()
		if e == nil {
			low := state.CurrentLedger - uint32(opts.Last)
			return archivist.MakeRange(low, state.CurrentLedger)
		}
	}
	return archivist.MakeRange(uint32(opts.Low),
		uint32(opts.High))

}

func mirror(src string, dst string, opts *Options) {
	srcArch := archivist.MustConnect(src, &opts.ConnectOpts)
	dstArch := archivist.MustConnect(dst, &opts.ConnectOpts)
	rng := opts.Range(srcArch)
	log.Printf("mirroring %v -> %v\n", src, dst)
	e := archivist.Mirror(srcArch, dstArch, rng)
	if e != nil {
		log.Fatal(e)
	}
}

func repair(src string, dst string, opts *Options) {
	srcArch := archivist.MustConnect(src, &opts.ConnectOpts)
	dstArch := archivist.MustConnect(dst, &opts.ConnectOpts)
	rng := opts.Range(srcArch)
	log.Printf("repairing %v -> %v\n", src, dst)
	e := archivist.Repair(srcArch, dstArch, rng)
	if e != nil {
		log.Fatal(e)
	}
}

func main() {

	var opts Options

	app := cli.NewApp()
	app.Name = "Archivist"
	app.Usage = "inspect stellar history archives"
	app.Flags = []cli.Flag{
		&cli.IntFlag{
			Name: "low",
			Usage: "first ledger to act on",
			Value: 0,
			Destination: &opts.Low,
		},
		&cli.IntFlag{
			Name: "high",
			Usage: "last ledger to act on",
			Value: 0xffffffff,
			Destination: &opts.High,
		},
		&cli.IntFlag{
			Name: "last",
			Usage: "number of recent ledgers to act on",
			Value: -1,
			Destination: &opts.Last,
		},
		&cli.StringFlag{
			Name: "s3region",
			Usage: "S3 region to connect to",
			Value: "us-east-1",
			Destination: &opts.ConnectOpts.S3Region,
		},
		&cli.BoolFlag{
			Name: "dryrun, n",
			Usage: "describe file-writes, but do not perform any",
			Destination: &opts.ConnectOpts.DryRun,
		},
	}
	app.Commands = []cli.Command{
		{
			Name: "status",
			Action: func(c *cli.Context) {
				status(c.Args().First(), &opts)
			},
		},
		{
			Name: "scan",
			Action: func(c *cli.Context) {
				scan(c.Args().First(), &opts)
			},
		},
		{
			Name: "mirror",
			Action: func(c *cli.Context) {
				src := c.Args()[0]
				dst := c.Args()[1]
				if len(c.Args()) != 2 || src == "" || dst == "" {
					log.Fatal("require exactly 2 arguments")
				}
				mirror(src, dst, &opts)
			},
		},
		{
			Name: "repair",
			Action: func(c *cli.Context) {
				src := c.Args()[0]
				dst := c.Args()[1]
				if len(c.Args()) != 2 || src == "" || dst == "" {
					log.Fatal("require exactly 2 arguments")
				}
				repair(src, dst, &opts)
			},
		},
	}
	app.Run(os.Args)
}

