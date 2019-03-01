package main

import (
	"bufio"
	"io"
	"os"

	"github.com/eiri/dieci"
)

func buildDS(name string) {
	w, err := os.Open("/usr/share/dict/words")
	if err != nil {
		panic(err)
	}
	defer w.Close()
	ds, err := dieci.Open(name)
	if err != nil {
		panic(err)
	}
	defer ds.Close()
	words := bufio.NewReader(w)
	for {
		word, _, err := words.ReadLine()
		if err == io.EOF {
			break
		}
		_, err = ds.Write(word)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	if _, err := os.Stat("words.data"); os.IsNotExist(err) {
		f, _ := os.Create("words.data")
		f.Close()
		buildDS("words")
	}
}
