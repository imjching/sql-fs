.PHONY: all
all: bin/sqlfs

.PHONY: bin/sqlfs
bin/sqlfs:
	go build -v -o $@ ./sqlfs

.PHONY: run
run: bin/sqlfs
	./bin/sqlfs mount
