package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	_ "github.com/lib/pq"
)

// Reference: https://github.com/bazil/fuse/blob/master/examples/hellofs/hello.go
func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	connUrl := "postgres://roacher@localhost:26257/sqlfs?sslmode=disable&connect_timeout=5"
	db, err := sql.Open("postgres", connUrl)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("sql-fs"),     // FreeBSD ignores this.
		fuse.Subtype("sql-fs"),    // OS X and FreeBSD ignore this.
		fuse.LocalVolume(),        // OS X only.
		fuse.VolumeName("sql-fs"), // OS X only.
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	go func() {
		for range sigCh {
			log.Println("Unmounting...")
			if err := fuse.Unmount(mountpoint); err != nil {
				log.Println(err)
			} else {
				log.Println("Unmounting completed.")
				return
			}
		}
	}()

	fmt.Printf("FUSE Protocol: %s\n", c.Protocol())

	err = fs.Serve(c, fileSystem{db: db})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
