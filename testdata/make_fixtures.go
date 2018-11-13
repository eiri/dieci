package main

import (
	"bufio"
	"fmt"
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
	s.Close()
	os.Rename(fmt.Sprintf("%s.data", s.Name()), "words.data")
	os.Rename(fmt.Sprintf("%s.idx", s.Name()), "words.idx")
}
