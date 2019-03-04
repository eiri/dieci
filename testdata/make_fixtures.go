package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"time"

	"github.com/eiri/dieci"
)

func buildDS(name string) {
	w, err := os.Open("/usr/share/dict/words")
	if err != nil {
		log.Panic(err)
	}
	defer w.Close()
	ds, err := dieci.Open(name)
	if err != nil {
		log.Panic(err)
	}
	defer ds.Close()
	log.Printf("Starting...")
	start := time.Now()
	total := 0
	batch_start := time.Now()
	batch_count := 0
	words := bufio.NewReader(w)
	for {
		word, _, err := words.ReadLine()
		if err == io.EOF {
			break
		}
		_, err = ds.Write(word)
		if err != nil {
			log.Panic(err)
		}
		batch_count++
		total++
		if batch_count == 50000 {
			batch_elapsed := time.Since(batch_start)
			log.Printf("Wrote %d words in %s", batch_count, batch_elapsed)
			batch_count = 0
			batch_start = time.Now()
		}
	}
	elapsed := time.Since(start)
	log.Printf("Done in %s, wrote %d words", elapsed, total)
}

func main() {
	if _, err := os.Stat("words.data"); os.IsNotExist(err) {
		f, _ := os.Create("words.data")
		f.Close()
		buildDS("words")
	}
}
