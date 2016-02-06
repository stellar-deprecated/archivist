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

type Options struct {
	Low int
	High int
	Last int
	CommandOpts archivist.CommandOptions
	ConnectOpts archivist.ConnectOptions
}

func (opts *Options) SetRange(arch *archivist.Archive) {
	if arch != nil && opts.Last != -1 {
		state, e := arch.GetRootHAS()
		if e == nil {
			low := state.CurrentLedger - uint32(opts.Last)
			opts.CommandOpts.Range =
				archivist.MakeRange(low, state.CurrentLedger)
			return
		}
	}
	opts.CommandOpts.Range =
		archivist.MakeRange(uint32(opts.Low),
		uint32(opts.High))

}

func scan(a string, opts *Options) {
	arch := archivist.MustConnect(a, &opts.ConnectOpts)
	opts.SetRange(arch)
	e1 := arch.Scan(&opts.CommandOpts)
	e2 := arch.ReportMissing(&opts.CommandOpts)
	e3 := arch.ReportInvalid(&opts.CommandOpts)
	if e1 != nil {
		log.Fatal(e1)
	}
	if e2 != nil {
		log.Fatal(e2)
	}
	if e3 != nil {
		log.Fatal(e3)
	}
}

func mirror(src string, dst string, opts *Options) {
	srcArch := archivist.MustConnect(src, &opts.ConnectOpts)
	dstArch := archivist.MustConnect(dst, &opts.ConnectOpts)
	opts.SetRange(srcArch)
	log.Printf("mirroring %v -> %v\n", src, dst)
	e := archivist.Mirror(srcArch, dstArch, &opts.CommandOpts)
	if e != nil {
		log.Fatal(e)
	}
}

func repair(src string, dst string, opts *Options) {
	srcArch := archivist.MustConnect(src, &opts.ConnectOpts)
	dstArch := archivist.MustConnect(dst, &opts.ConnectOpts)
	opts.SetRange(srcArch)
	log.Printf("repairing %v -> %v\n", src, dst)
	e := archivist.Repair(srcArch, dstArch, &opts.CommandOpts)
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
		&cli.IntFlag{
			Name: "concurrency, c",
			Usage: "number of files to operate on concurrently",
			Value: 32,
			Destination: &opts.CommandOpts.Concurrency,
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
			Destination: &opts.CommandOpts.DryRun,
		},
		&cli.BoolFlag{
			Name: "force, f",
			Usage: "overwrite existing files",
			Destination: &opts.CommandOpts.Force,
		},
		&cli.BoolFlag{
			Name: "verify",
			Usage: "verify file contents",
			Destination: &opts.CommandOpts.Verify,
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
		{
			Name: "dumpxdr",
			Action: func(c *cli.Context) {
				err := archivist.DumpXdrAsJson(c.Args())
				if err != nil {
					log.Fatal(err)
				}
			},
		},
	}
	app.Run(os.Args)
}

