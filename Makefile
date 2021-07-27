.DEFAULT_GOAL := all

.PHONY: all
all: build test

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test -v ./...

.PHONY: bench
bench:
	go test -bench=. -benchmem

.PHONY: clean
clean: clean-data ## clean up
	go clean
	rm -f coverage.out
	rm -f *.bench
