package file_manager

import (
	"encoding/binary"
)

type Page struct {
	buffer []byte
}

func NewPageBySize(blockSize uint64) *Page {
	bytes := make([]byte, blockSize)
	return &Page{
		buffer: bytes,
	}
}

func NewPageByBytes(bytes []byte) *Page {
	return &Page{
		buffer: bytes,
	}
}

// ------------------- int 类型的 get 和 put -------------------

func (p *Page) GetInt(offset uint64) uint64 {
	num := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	return num
}

func uint64ToByteArray(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func (p *Page) SetInt(offset uint64, val uint64) {
	b := uint64ToByteArray(val)
	copy(p.buffer[offset:], b)
}

// ------------------- byte 数组类型的 get 和 put -------------------

func (p *Page) GetBytes(offset uint64) []byte {
	// 用8个字节表示后续数组长度，后续数组长度不超过2^64
	l := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	newBuffer := make([]byte, l)
	copy(newBuffer, p.buffer[offset+8:])
	return newBuffer
}

func (p *Page) SetBytes(offset uint64, b []byte) {
	// 首先将数组长度写入buffer，然后将数组写入buffer
	l := uint64(len(b))
	lenBuffer := uint64ToByteArray(l)
	copy(p.buffer[offset:], lenBuffer)
	copy(p.buffer[offset+8:], b)
}

// ------------------- string 类型的 get 和 put -------------------

func (p *Page) GetString(offset uint64) string {
	strBytes := p.GetBytes(offset)
	return string(strBytes)
}

func (p *Page) SetString(offset uint64, s string) {
	strBytes := []byte(s)
	p.SetBytes(offset, strBytes)
}

func (p *Page) MaxLengthForString(s string) uint64 {
	bs := []byte(s) //返回字符串相对于字节数组的长度
	uint64Size := 8 //存储字符串时预先存储其长度，也就是uint64,它占了8个字节
	return uint64(uint64Size + len(bs))
}

func (p *Page) contents() []byte {
	return p.buffer
}
