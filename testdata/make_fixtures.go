package main

import (
	"bufio"
	"github.com/eiri/beansdb"
	"io"
	"os"
)

func main() {
	// storage
	s, err := beansdb.New()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	// input
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
	os.Rename(s.Name(), "words.data")
}
