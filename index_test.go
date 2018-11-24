package beansdb

import (
    "bytes"
    "io/ioutil"
    "os"
    "path/filepath"
    "strings"
    "testing"
)

// TestIndexLoad to ensure we can load an existing index
func TestIndexLoad(t *testing.T) {
    name := filepath.Join("testdata", "fox-dog.idx.golden")
    c, err := loadIndex(name)
    if err != nil {
        t.Fatal(err)
    }
    if len(c) != 9 {
        t.Fatalf("Expecting 9 keys in index, got %d", len(c))
    }
}

// TestIndexRebuild to ensure we can rebuild an index from a datalog
func TestIndexRebuild(t *testing.T) {
    name := filepath.Join("testdata", "fox-dog")
    os.Remove(name + ".idx")
    f, err := os.OpenFile(name+".idx", os.O_CREATE|os.O_WRONLY, 0600)
    if err != nil {
        t.Fatal(err)
    }
    c := make(cache)
    i := &index{c, f}
    defer i.close()
    err = rebuildIndex(name, i)
    if err != nil {
        t.Fatal(err)
    }
    if len(c) != 9 {
        t.Fatalf("Expecting 9 keys in index, got %d", len(c))
    }
    rebuilt, err := ioutil.ReadFile(name + ".idx")
    if err != nil {
        t.Fatal(err)
    }
    expected, err := ioutil.ReadFile(name + ".idx.golden")
    if err != nil {
        t.Fatal(err)
    }
    if !bytes.Equal(rebuilt, expected) {
        t.Fatal("Expected rebuild index to be identical to golden")
    }
}

// TestIndexOpenClose to ensure we can open an index
func TestIndexOpenClose(t *testing.T) {
    name := filepath.Join("testdata", "fox-dog")
    i, err := openIndex(name)
    if err != nil {
        t.Fatal(err)
    }
    if len(i.cache) != 9 {
        t.Fatalf("Expecting 9 keys in index, got %d", len(i.cache))
    }
    err = i.close()
    if err != nil {
        t.Fatal(err)
    }
    if len(i.cache) != 0 {
        t.Fatal("Expecting index cache to reset")
    }
}

// TestIndexGet to ensure we can read from an index
func TestIndexGet(t *testing.T) {
    name := filepath.Join("testdata", "fox-dog")
    i, err := openIndex(name)
    if err != nil {
        t.Fatal(err)
    }
    defer i.close()
    cur := 0
    words1 := "The quick brown fox jumps over the lazy dog"
    for _, w := range strings.Fields(words1) {
        b := []byte(w)
        score := makeScore(b)
        pos, blen, ok := i.get(score)
        if !ok {
            t.Fatalf("Expecting %s => %s to be in the index", w, score)
        }
        if len(b) != blen {
            t.Fatalf("Expecting lenth of %s be %d, got %d", w, len(b), blen)
        }
        if pos <= cur {
            t.Fatalf("Expecting position of %s in datalog to be further", w)
        }
        cur = pos
    }
    words2 := "When zombies arrive quickly fax judge Pat"
    for _, w := range strings.Fields(words2) {
        b := []byte(w)
        score := makeScore(b)
        pos, blen, ok := i.get(score)
        if ok {
            t.Fatalf("Expecting %s not to be in the index", w)
        }
        if pos != 0 {
            t.Fatalf("Expecting %s position to be 0", w)
        }
        if blen != 0 {
            t.Fatalf("Expecting %s length to be 0", w)
        }
    }
}

// TestIndexPut to ensure we can write in an index
func TestIndexPut(t *testing.T) {
    // missing index
    name := filepath.Join("testdata", "missing")
    i, err := openIndex(name)
    if err == nil {
        t.Fatal("Expecting an error on a missing index")
    }
    defer i.delete()
    words := "The quick brown fox jumps over the lazy dog"
    for pos, w := range strings.Fields(words) {
        b := []byte(w)
        score := makeScore(b)
        err := i.put(score, pos, len(b))
        if err != nil {
            t.Fatal(err)
        }
        err = i.put(score, 0, 0)
        if err != nil {
            t.Fatal(err)
        }
    }
    // read back
    for pos, w := range strings.Fields(words) {
        b := []byte(w)
        score := makeScore(b)
        bpos, blen, ok := i.get(score)
        if !ok {
            t.Fatalf("Expecting %s => %s to be in the index", w, score)
        }
        if bpos != pos {
            t.Fatalf("Expecting %s position to be %d, got %d", w, pos, bpos)
        }
        if blen != len(w) {
            t.Fatalf("Expecting %s length to be %d, got %d", w, len(w), blen)
        }
    }
}

// TestIndexDelete to ensure we can delete an index
func TestIndexDelete(t *testing.T) {
    // missing index
    name := filepath.Join("testdata", "fox-dog")
    i, err := openIndex(name)
    if err != nil {
        t.Fatal(err)
    }
    err = i.delete()
    if err != nil {
        t.Fatal(err)
    }
}
