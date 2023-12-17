package file_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetAndGetInt(t *testing.T) {
	page := NewPageBySize(256)
	val := uint64(1234)
	offset := uint64(23) //指定写入偏移
	page.SetInt(offset, val)
	valGot := page.GetInt(offset)
	require.Equal(t, val, valGot)
}

func TestSetAndGetByteArray(t *testing.T) {
	page := NewPageBySize(256)
	bs := []byte{1, 2, 3, 4, 5, 6}
	offset := uint64(111)
	page.SetBytes(offset, bs)
	bsGot := page.GetBytes(offset)
	require.Equal(t, bs, bsGot)
}

func TestSetAndGetString(t *testing.T) {
	page := NewPageBySize(256)
	s := "hello, 世界"
	offset := uint64(177)
	page.SetString(offset, s)
	sGot := page.GetString(offset)
	require.Equal(t, s, sGot)
}

func TestMaxLengthForString(t *testing.T) {
	s := "hello, 世界"
	sLen := uint64(len([]byte(s)))
	page := NewPageBySize(256)
	sLenGot := page.MaxLengthForString(s)
	require.Equal(t, sLen+8, sLenGot)
}

func TestGetContents(t *testing.T) {
	bs := []byte{1, 2, 3, 4, 5, 6}
	page := NewPageByBytes(bs)
	bsGot := page.contents()
	require.Equal(t, bs, bsGot)
}
