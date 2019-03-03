package main

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"log"
	"math/big"
	"os"
)

type hashingFunc func() []byte

var md5Hmac = hmac.New(md5.New, []byte("secret"))
var sha1Hmac = hmac.New(sha1.New, []byte("secret"))

func makeSize() uint32 {
	size, _ := rand.Int(rand.Reader, big.NewInt(big.MaxPrec))
	return uint32(size.Uint64())
}

func MakeMd5Score() []byte {
	data := make([]byte, 65536)
	rand.Read(data)
	md5Hmac.Reset()
	md5Hmac.Write(data)
	score := md5Hmac.Sum(nil)
	return score[:]
}

func MakeSha1Score() []byte {
	data := make([]byte, 65536)
	rand.Read(data)
	sha1Hmac.Reset()
	sha1Hmac.Write(data)
	score := sha1Hmac.Sum(nil)
	return score[:]
}

func makeIdx(name string, num int, hf hashingFunc) int64 {
	f, err := os.Create(name)
	if err != nil {
		log.Panic(err)
	}
	defer f.Close()
	for i := 1; i <= num; i++ {
		size := makeSize()
		score := hf()
		//log.Printf("score %x, size %d", score, size)
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, size)
		buf = append(buf, score...)
		_, err := f.Write(buf)
		if err != nil {
			log.Panic(err)
		}
	}
	stat, _ := f.Stat()
	return stat.Size()
}

func main() {
	buf := make([]byte, 16)
	rand.Read(buf)
	baseName := hex.EncodeToString(buf)
	keyNum := 10000
	name := baseName + "-md5.idx"
	size := makeIdx(name, keyNum, MakeMd5Score)
	log.Printf("Hashing  md5, %d keys, filesize: %d", keyNum, size)

	name = baseName + "-sha1.idx"
	size = makeIdx(name, keyNum, MakeSha1Score)
	log.Printf("Hashing sha1, %d keys, filesize: %d", keyNum, size)
}
