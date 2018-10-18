// Command line utility for BeansDB
package main

import (
	_ "github.com/eiri/beansdb"
	"log"
)

func main() {
	log.SetFlags(log.Lshortfile)
	log.Print("ohai")
}
