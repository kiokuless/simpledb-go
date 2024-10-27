package file

type BlockID struct {
	filename string
	blockNum int32
}

func NewBlockID(filename string, blockNum int32) *BlockID {
	return &BlockID{filename, blockNum}
}

func (b *BlockID) Filename() string {
	return b.filename
}

func (b *BlockID) Number() int32 {
	return b.blockNum
}

func (b *BlockID) Equals(other any) bool {
	o, ok := other.(*BlockID)
	if !ok {
		return false
	}
	return b.filename == o.filename && b.blockNum == o.blockNum
}

func MaxLength(strlen int32) int32 {
	// ascii に限ってはこれでもいけるはず？
	return 4 + strlen
}
