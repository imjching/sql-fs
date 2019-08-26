# sql-fs

POSIX-style filesystem on top of a SQL database using [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace). At the moment, this filesystem only supports CockroachDB. This is a proof of concept and has limitations. See *Future Work* below.

## Details

This project uses [bazil.org/fuse](https://github.com/bazil/fuse), which is a Go library for writing FUSE userspace filesystems. Bazil implements the kernel-userspace communication protocol.

`sql-fs` will communicate with the kernel through Bazil to register the `mount/` mountpoint as a filesystem. The kernel will forward all filesystem operations for that filesystem back to the `sql-fs` process through the communication channel established.

## Dependencies

1. [CockroachDB](https://github.com/cockroachdb/cockroach)
2. FUSE kernel driver and libraries:
   - Mac: [OSXFUSE](https://osxfuse.github.io/)
   - Linux: `fuse` package
3. Go.

## Usage

Assuming that CockroachDB is listening on port `26257`, here are the steps to run sql-fs:

```
# Clone the sql-fs git repository
git clone git@github.com:imjching/sql-fs.git
cd sql-fs

# Run schema.sql
cockroach sql --insecure < schema.sql

# Build the binary and start the filesystem
make run

# Your mountpoint will be ./mount
```

## Future Work
1. Support for multiple databases (MySQL, PostgreSQL, etc.) with abstraction.
2. Concurrent file access. The current implementation for writing and reading is a little fragile.
3. Tests.

## References

- To FUSE or Not to FUSE: Performance of
User-Space File Systems: https://www.usenix.org/system/files/conference/fast17/fast17-vangoor.pdf
