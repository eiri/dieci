// Package beansdb implements basic API for BeansDB data store
package beansdb

import (
	"fmt"
)

// New creates a new empty storage
func New() {
	fmt.Println("New")
}

// Open opens provided storage
func Open() {
	fmt.Println("Open")
}

// Read a data for a given score
func Read() {
	fmt.Println("Read")
}

// Write given data and return it's score
func Write() {
	fmt.Println("Write")
}

// Delete provided storage
func Delete() {
	fmt.Println("Delete")
}
