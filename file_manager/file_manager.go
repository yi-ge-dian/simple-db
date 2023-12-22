package file_manager

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileManager struct {
	dbDirectory string              // 数据库目录
	blockSize   uint64              // 区块大小
	isNew       bool                // 是否是新创建的数据库目录
	openFiles   map[string]*os.File // 打开的文件
	mu          sync.Mutex          // 互斥锁
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
		//目录存在，则先删除目录下的临时文件
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
	// 把这个数据块中读取数据到缓冲区，第一个参数是缓冲区，第二个参数是偏移量
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
	// 把缓冲区中的数据写入到这个数据块, 第一个参数是数据块的内容, 第二个参数是偏移量
	n, err := file.WriteAt(p.contents(), int64(blk.Number()*fm.blockSize))
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (fm *FileManager) Size(fileName string) (uint64, error) {
	file, err := fm.GetFile(fileName)
	if err != nil {
		return 0, err
	}

	fs, err := file.Stat()
	if err != nil {
		return 0, err
	}
	// 返回文件大小，一共有多少个区块
	return uint64(fs.Size()) / fm.blockSize, nil
}

func (fm *FileManager) Append(fileName string) (BlockId, error) {
	newBlockNum, err := fm.Size(fileName)
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
