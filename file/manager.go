package file

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Manager struct {
	dbDir     string
	blockSize int32
	isNew     bool
	openFiles map[string]*os.File
	mu        *sync.Mutex
}

func NewManager(dbDir string, blockSize int32) *Manager {
	_, err := os.Stat(dbDir)
	isNew := err != nil

	if isNew {
		if os.MkdirAll(dbDir, 0o700) != nil {
			// TODO: エラーハンドリング
			panic(err)
		}
	}

	files, err := os.ReadDir(dbDir)
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "temp") {
			err = os.Remove(filepath.Join(dbDir, file.Name()))
			if err != nil {
				panic(err)
			}
		}
	}

	return &Manager{
		dbDir:     dbDir,
		blockSize: blockSize,
		isNew:     isNew,
		openFiles: make(map[string]*os.File),
		mu:        &sync.Mutex{},
	}
}

func (mng *Manager) Read(block BlockID, p *Page) error {
	mng.mu.Lock()
	defer mng.mu.Unlock()

	f, err := mng.getFile(block.filename)
	if err != nil {
		return err
	}
	_, err = f.Seek(int64(int32(block.Number())*int32(mng.blockSize)), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = f.Read(p.buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("f.Read: %w", err)
	}

	return nil
}

func (mng *Manager) Write(block BlockID, p *Page) error {
	mng.mu.Lock()
	defer mng.mu.Unlock()

	f, err := mng.getFile(block.filename)
	if err != nil {
		return err
	}

	_, err = f.Seek(int64((block.Number())*int32(mng.blockSize)), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = f.Write(p.buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("f.Read: %w", err)
	}

	return nil
}

func (mng *Manager) Append(filename string) (BlockID, error) {
	mng.mu.Lock()
	defer mng.mu.Unlock()

	newBlockNum, err := mng.Length(filename)
	if err != nil {
		return BlockID{}, err
	}

	block := NewBlockID(filename, int32(newBlockNum))
	b := make([]byte, mng.BlockSize())

	f, err := mng.getFile(filename)
	if err != nil {
		return BlockID{}, fmt.Errorf("fmng.openFile: %w", err)
	}

	_, err = f.Seek(int64(int32(block.Number())*int32(block.blockNum)), io.SeekStart)
	if err != nil {
		return BlockID{}, fmt.Errorf("f.Seek: %w", err)
	}

	_, err = f.Write(b)
	if err != nil {
		return BlockID{}, fmt.Errorf("f.Write: %w", err)
	}

	return *block, nil
}

// ファイルのブロック数を計算する
func (mng *Manager) Length(filename string) (int32, error) {
	f, err := mng.getFile(filename)
	if err != nil {
		return 0, err
	}

	fileStat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return int32(fileStat.Size()) / int32(mng.blockSize), nil
}

func (mng *Manager) IsNew() bool {
	return mng.isNew
}

func (mng *Manager) BlockSize() int32 {
	return mng.blockSize
}

func (mng *Manager) getFile(filename string) (*os.File, error) {
	if randomAccessFile, ok := mng.openFiles[filename]; ok {
		return randomAccessFile, nil
	}

	target := filepath.Join(mng.dbDir, filename)
	f, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w, target path: %s", err, target)
	}

	mng.openFiles[filename] = f

	return f, nil
}
