package server

import "github.com/kiokuless/simpledb-go/file"

type SimpleDB struct {
	fileManager *file.Manager
}

func NewSimpleDB(dbDir string, blockSize int32) (*SimpleDB, error) {
	return &SimpleDB{
		fileManager: file.NewManager(dbDir, blockSize),
	}, nil
}
func (db *SimpleDB) FileManager() *file.Manager {
	return db.fileManager
}
