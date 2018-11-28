package beansdb

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDataLogOpenClose to ensure we can open a datalog file
func TestDataLogOpenClose(t *testing.T) {
	// setup
	name := filepath.Join("testdata", "fox-dog")
	err := copyGoldenFile(name + ".data")
	if err != nil {
		t.Fatal(err)
	}
	// tests
	d, err := openDataLog(name)
	if err != nil {
		t.Fatal(err)
	}
	if d.cur != 71 {
		t.Fatalf("Expecting cursor be at 71, got %d", d.cur)
	}
	err = d.close()
	if err != nil {
		t.Fatal(err)
	}
	if d.cur != 0 {
		t.Fatalf("Expecting cursor be at 0, got %d", d.cur)
	}
}

// TestDataLogGet to ensure we can read from a datalog
func TestDataLogGet(t *testing.T) {
	name := filepath.Join("testdata", "fox-dog")
	d, err := openDataLog(name)
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	blocks := []addr{
		addr{4, 3},
		addr{11, 5},
		addr{20, 5},
		addr{29, 3},
		addr{36, 5},
		addr{45, 4},
		addr{53, 3},
		addr{60, 4},
		addr{68, 3},
	}
	words := "The quick brown fox jumps over the lazy dog"
	for i, w := range strings.Fields(words) {
		p, l := blocks[i][0], blocks[i][1]
		b, err := d.get(p, l)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != w {
			t.Fatalf("Expecting %s, got %q", w, string(b))
		}
	}
}

// TestDataLogPut to ensure we can write into a datalog
func TestDataLogPut(t *testing.T) {
	// setup
	name := filepath.Join("testdata", "fox-dog")
	os.Remove(name + ".data")
	f, err := os.Create(name + ".data")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	// test
	d, err := openDataLog(name)
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	if d.cur != 0 {
		t.Fatalf("Expecting cursor to be at 0, got %b", d.cur)
	}
	words := "The quick brown fox jumps over the lazy dog"
	for _, w := range strings.Fields(words) {
		_, _, err := d.put([]byte(w))
		if err != nil {
			t.Fatal(err)
		}
	}
	stored, err := ioutil.ReadFile(name + ".data")
	if err != nil {
		t.Fatal(err)
	}
	expected, err := ioutil.ReadFile(name + ".data.golden")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, expected) {
		t.Fatal("Expected datalog to be identical to golden")
	}
}

// TestDataLogDelete to ensure we can delete a datalog
func TestDataLogDelete(t *testing.T) {
	name := filepath.Join("testdata", "fox-dog")
	d, err := openDataLog(name)
	if err != nil {
		t.Fatal(err)
	}
	err = d.delete()
	if err != nil {
		t.Fatal(err)
	}
}

func copyGoldenFile(name string) error {
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		os.Remove(name)
	}
	src, err := os.Open(name + ".golden")
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(name)
	if err != nil {
		return err
	}
	defer func() {
		dst.Sync()
		dst.Close()
	}()
	_, err = io.Copy(dst, src)
	return err
}
