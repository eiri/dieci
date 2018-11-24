.DEFAULT_GOAL := all

.PHONY: help
help: ## this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: all
all: deps test ## get deps and run tests

.PHONY: deps
deps: ## install deps
	go get -t ./...

testdata/fox-dog.idx.golden:
	cd testdata; go run make_fixtures.go

.PHONY: test
test: testdata/fox-dog.idx.golden clean-data ## run tests
	go test -v ./...

testdata/words.data:
	cd testdata; go run make_fixtures.go

.PHONY: bench
bench: testdata/words.data ## run a micro benchmark
	go test -bench=. -benchmem

.PHONY: benchcmp
benchcmp: testdata/words.data ## run before and after benchmark
	git stash
	go test -bench=. -benchmem > before.bench
	git stash pop
	go test -bench=. -benchmem > after.bench
	benchcmp before.bench after.bench

.PHONY: clean
clean: clean-data ## clean up
	go clean
	rm -f coverage.out
	rm -f *.bench
	rm -f testdata/*.data
	rm -f testdata/*.idx
	rm -f testdata/*.golden

.PHONY: clean-data
clean-data: ## remove index and datalog files from the test runs
	rm -f *.idx
	rm -f *.data

.PHONY: format
format: ## format code
	go fmt -x *.go
