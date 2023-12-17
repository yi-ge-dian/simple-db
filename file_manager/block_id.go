package file_manager

import (
	"crypto/sha256"
	"fmt"
)

type BlockId struct {
	fileName string //区块所在文件
	blkNum   uint64 //区块的标号
}

func NewBlockId(fileName string, blkNum uint64) *BlockId {
	return &BlockId{
		fileName: fileName,
		blkNum:   blkNum,
	}
}

func (b *BlockId) FileName() string {
	return b.fileName
}

func (b *BlockId) Number() uint64 {
	return b.blkNum
}

func (b *BlockId) Equals(other *BlockId) bool {
	return b.fileName == other.fileName && b.blkNum == other.blkNum
}

func asSha256(o interface{}) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", o)))

	return fmt.Sprintf("%x", h.Sum(nil))
}
func (b *BlockId) HashCode() string {
	return asSha256(*b)
}
