package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func main() {
	// 1. 创建数据库目录
	dir := filepath.Join("example", "journal_data")
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	// 2. 打开数据库
	db, err := leveldb.OpenFile(dir, &opt.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 3. 创建一个batch并写入一些KV数据
	batch := new(leveldb.Batch)
	batch.Put([]byte("key1"), []byte("This is value 1"))
	batch.Put([]byte("key2"), []byte("This is value 2"))
	// 写入一个大value，这样可以看到跨block的情况
	largeValue := make([]byte, 40*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	batch.Put([]byte("key3"), largeValue)
	
	// 4. 写入batch
	if err := db.Write(batch, nil); err != nil {
		panic(err)
	}

	// 5. 关闭数据库，这样确保所有数据都写入磁盘
	db.Close()

	// 6. 现在我们来读取journal文件
	files, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	// 查找并读取journal文件
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".log" {
			fmt.Printf("Found journal file: %s\n", file.Name())
			
			// 打开journal文件
			stor, err := storage.OpenFile(dir, false)
			if err != nil {
				panic(err)
			}
			defer stor.Close()

			// 创建文件描述符
			fileNum := storage.FileDesc{Type: storage.TypeJournal, Num: 0}
			fileList, err := stor.List(storage.TypeJournal)
			if err != nil {
				panic(err)
			}
			if len(fileList) > 0 {
				fileNum = fileList[0]
			}

			// 打开文件
			reader, err := stor.Open(fileNum)
			if err != nil {
				panic(err)
			}
			defer reader.Close()

			// 创建journal reader
			journalReader := journal.NewReader(reader, nil, true, true)
			recordNum := 0

			// 读取记录
			for {
				r, err := journalReader.Next()
				if err != nil {
					break
				}

				// 读取记录内容
				content := make([]byte, 1024)
				n, _ := r.Read(content)
				recordNum++
				fmt.Printf("Record #%d, first %d bytes: % x\n", recordNum, n, content[:n])
			}
		}
	}

	fmt.Println("\nSuccessfully read all records!")
}
