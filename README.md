# stellar-archivist

This is a small tool, written in Go, for working with `stellar-core` history archives directly.
It is a standalone tool that does not require `stellar-core`, or any other programs.

It is much smaller and simpler than `stellar-core`, and is intended only for maintenance tasks such as:

  - reporting the current state of an archive
  - mirroring archives, or portions of archives
  - scanning all or recent portions of archives for missing files
  - repairing archives by copying missing files from other archives
  - performing integrity checks on files

It is a work in progress, and does not yet do all these things. Nor does it handle errors very well
yet; enhancements are very welcome!


## Specifying history archives

Unlike `stellar-core`, `stellar-archivist` does not run subprocesses to access history archives;
instead it operates directly on history archives given by URLs. Currently it understands URLs
of the following schemes:

  - `s3://bucketname/prefix`
  - `file://path`

Supporting an additional URL scheme requires writing a new archive backend implementation; see
for example [the S3 backend](s3_archive.go).

The disadvantage of this approach is that it requires special-purpose code to support each type of
archive; the advantage is that more operations are supported, and the tool can scan and operate on
archives much more quickly. This is necessary to handle bulk operations on archives with many
thousands of files efficiently.
