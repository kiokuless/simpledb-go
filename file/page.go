package file

import (
	"encoding/binary"
)

type Page struct {
	// メモリ中のバッファ、最大の長さは4096か？
	buffer []byte
}

const (
	Int32Size = 4
)

func NewPageFromBytes(buf []byte) *Page {
	return &Page{buf}
}

func NewPageFromBlockSize(blockSize int32) *Page {
	return NewPageFromBytes(make([]byte, blockSize))
}

func (p *Page) GetInt(offset int32) int32 {
	// オフセットの位置から4バイトを読んで int32 を解釈する
	return int32(binary.LittleEndian.Uint32(p.buffer[offset : offset+Int32Size]))
}

func (p *Page) SetInt(offset int32, value int32) {
	// オフセットの位置から4バイト(int32)分書き込む
	binary.LittleEndian.PutUint32(p.buffer[offset:], uint32(value))
}

// Blob は次のような形式で保存されている
// |length| data .... |
// length: 4バイト
// data: lengthバイト
func (p *Page) GetBytes(offset int32) []byte {
	length := p.GetInt(offset)
	newOffset := offset + Int32Size
	b := make([]byte, length)
	copy(b, p.buffer[newOffset:newOffset+length])
	return b
}

func (p *Page) SetBytes(offset int32, data []byte) {
	p.SetInt(offset, int32(len(data)))
	copy(p.buffer[offset+Int32Size:], data)
}

func (p *Page) GetString(offset int32) string {
	b := p.GetBytes(offset)
	return string(b)
}

func (p *Page) SetString(offset int32, str string) {
	p.SetBytes(offset, []byte(str))
}
