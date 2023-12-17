package file_manager

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileManager struct {
	dbDirectory string
	blockSize   uint64
	isNew       bool
	openFiles   map[string]*os.File
	mu          sync.Mutex
}

func NewFileManager(dbDirectory string, blockSize uint64) (*FileManager, error) {
	fm := FileManager{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		isNew:       false,
		openFiles:   make(map[string]*os.File),
	}

	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		//目录不存在则创建
		fm.isNew = true
		err = os.MkdirAll(dbDirectory, 0755)
		if err != nil {
			return nil, err
		}
	} else {
		//目录存在，则先清楚目录下的临时文件
		err := filepath.Walk(dbDirectory, func(path string, info os.FileInfo, err error) error {
			mode := info.Mode()
			if mode.IsRegular() {
				name := info.Name()
				if strings.HasPrefix(name, "temp") {
					//删除临时文件
					err := os.Remove(filepath.Join(path, name))
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return &fm, nil
}

func (fm *FileManager) GetFile(fileName string) (*os.File, error) {
	path := filepath.Join(fm.dbDirectory, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	fm.openFiles[path] = file

	return file, nil
}

func (fm *FileManager) Read(blk *BlockId, p *Page) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := fm.GetFile(blk.FileName())
	if err != nil {
		return 0, err
	}
	defer file.Close()
	count, err := file.ReadAt(p.contents(), int64(blk.Number()*fm.blockSize))
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (fm *FileManager) Write(blk *BlockId, p *Page) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := fm.GetFile(blk.FileName())
	if err != nil {
		return 0, err
	}
	defer file.Close()

	n, err := file.WriteAt(p.contents(), int64(blk.Number()*fm.blockSize))
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (fm *FileManager) size(fileName string) (uint64, error) {
	file, err := fm.GetFile(fileName)
	if err != nil {
		return 0, err
	}

	fs, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return uint64(fs.Size()) / fm.blockSize, nil
}

func (fm *FileManager) Append(fileName string) (BlockId, error) {
	newBlockNum, err := fm.size(fileName)
	if err != nil {
		return BlockId{}, err
	}

	blk := NewBlockId(fileName, newBlockNum)
	file, err := fm.GetFile(blk.FileName())
	if err != nil {
		return BlockId{}, err
	}

	b := make([]byte, fm.blockSize)
	_, err = file.WriteAt(b, int64(blk.Number()*fm.blockSize)) //读入空数据相当于扩大文件长度
	if err != nil {
		return BlockId{}, nil
	}

	return *blk, nil
}

func (fm *FileManager) IsNew() bool {
	return fm.isNew
}

func (fm *FileManager) BlockSize() uint64 {
	return fm.blockSize
}
