package main

import (
	"bufio"
	"github.com/eiri/beansdb"
	"io"
	"os"
	"strings"
)

func touch(name string) {
	f, _ := os.OpenFile(name+".data", os.O_CREATE|os.O_EXCL, 0600)
	defer f.Close()
}

func foxdog() {
	touch("fox-dog")
	s, err := beansdb.Open("fox-dog")
	if err != nil {
		panic(err)
	}
	defer s.Close()
	words := "The quick brown fox jumps over the lazy dog"
	for _, word := range strings.Fields(words) {
		_, err = s.Write([]byte(word))
		if err != nil {
			panic(err)
		}
	}
	os.Rename("fox-dog.data", "fox-dog.data.golden")
	os.Rename("fox-dog.idx", "fox-dog.idx.golden")
}

func words() {
	touch("words")
	s, err := beansdb.Open("words")
	if err != nil {
		panic(err)
	}
	defer s.Close()
	f, err := os.Open("/usr/share/dict/words")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		word, _, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		_, err = s.Write(word)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	if _, err := os.Stat("fox-dog.idx.golden"); os.IsNotExist(err) {
		foxdog()
	}
	if _, err := os.Stat("words.data"); os.IsNotExist(err) {
		words()
	}
}
