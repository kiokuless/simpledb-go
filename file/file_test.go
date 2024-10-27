// modified: https://github.com/yokomotod/database-design-and-implementation-go/blob/dfef78b124981b72d8c3287d5c0598ef416eb930/simpledb/file/file_test.go
package file_test

import (
	"os"
	"testing"

	"github.com/kiokuless/simpledb-go/file"
	"github.com/kiokuless/simpledb-go/server"
)

func TestGetSetInt(t *testing.T) {
	t.Parallel()

	db, err := server.NewSimpleDB("./testgetsetint", 400)
	if err != nil {
		t.Fatalf("NewSimpleDB: %v", err)
	}

	t.Cleanup(func() {
		err = os.RemoveAll("./testgetsetint")
	})

	fm := db.FileManager()
	p1 := file.NewPageFromBlockSize(fm.BlockSize())
	pos1 := int32(88)

	// zero-value initialized?
	if p1.GetInt(pos1) != 0 {
		t.Fatal("???")
	}

	p1.SetInt(pos1, 42) // == 0x2a
	if p1.GetInt(pos1) != 42 {
		t.Fatal("???")
	}

	block := file.NewBlockID("test-setInt", 1)
	err = fm.Write(*block, p1)
	if err != nil {
		t.Fatalf("fm.Write: %v", err)
	}
}

func TestGetSetBytes(t *testing.T) {
	t.Parallel()

	db, err := server.NewSimpleDB("./testgetsetbytes", 400)
	if err != nil {
		t.Fatalf("NewSimpleDB: %v", err)
	}

	t.Cleanup(func() {
		err = os.RemoveAll("./testgetsetbytes")
	})

	fm := db.FileManager()
	p1 := file.NewPageFromBlockSize(fm.BlockSize())
	pos1 := int32(88)

	// zero-value initialized?
	if len(p1.GetBytes(pos1)) != len(make([]byte, 0)) {
		t.Fatal("???")
	}

	p1.SetBytes(pos1, []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'})
	if string(p1.GetBytes(pos1)) != string("abcdefghi") {
		t.Fatal("string(p1.GetBytes(pos1)) != string(\"abcdefghi\")", string(p1.GetBytes(pos1)), string("abcdefghi"))
	}

	block := file.NewBlockID("test-setByte", 1)
	err = fm.Write(*block, p1)
	if err != nil {
		t.Fatalf("fm.Write: %v", err)
	}
}

func TestFile(t *testing.T) {
	t.Parallel()

	db, err := server.NewSimpleDB("./testdir", 400)
	if err != nil {
		t.Fatalf("NewSimpleDB: %v", err)
	}

	t.Cleanup(func() {
		err = os.RemoveAll("./testdir")
	})

	fm := db.FileManager()

	p1 := file.NewPageFromBlockSize(fm.BlockSize())
	pos1 := int32(88)
	strVal := "abcdefghijklm"
	p1.SetString(pos1, strVal)

	size := file.MaxLength(int32(len(strVal)))
	pos2 := pos1 + size
	intVar := int32(345)
	p1.SetInt(pos2, intVar)

	blk := *file.NewBlockID("testfile", 2)
	err = fm.Write(blk, p1)
	if err != nil {
		t.Fatalf("fm.Write: %v", err)
	}

	p2 := file.NewPageFromBlockSize(fm.BlockSize())
	err = fm.Read(blk, p2)
	if err != nil {
		t.Fatalf("fm.Read: %v", err)
	}

	if p2.GetInt(pos2) != intVar {
		t.Errorf("expected %d, got %d", intVar, p2.GetInt(pos2))
	}
	if p2.GetString(pos1) != strVal {
		t.Errorf("expected %q, got %q", strVal, p2.GetString(pos1))
	}
}
