package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexSizesHashingFuncs(t *testing.T) {
	assert := require.New(t)

	t.Run("md5", func(t *testing.T) {
		score1 := MakeMd5Score()
		assert.Len(score1, 16)
		score2 := MakeMd5Score()
		assert.NotEqual(score1, score2)
	})

	t.Run("sha1", func(t *testing.T) {
		score1 := MakeSha1Score()
		assert.Len(score1, 20)
		score2 := MakeSha1Score()
		assert.NotEqual(score1, score2)
	})
}

func BenchmarkMd5(b *testing.B) {
	for n := 0; n < b.N; n++ {
		MakeMd5Score()
	}
}

func BenchmarkSha1(b *testing.B) {
	for n := 0; n < b.N; n++ {
		MakeSha1Score()
	}
}
