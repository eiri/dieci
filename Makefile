.DEFAULT_GOAL := all

.PHONY: help
help: ## this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: all
all: deps test ## get deps and run tests

.PHONY: deps
deps: ## install deps
	go get -t ./...

.PHONY: test
test: clean-data ## run tests
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
	rm -f testdata/words.data

.PHONY: clean-data
clean-data: ## remove storage files from the test runs
	rm -f *.data

.PHONY: format
format: ## format code
	go fmt -x *.go
