// Copyright (c) 2016, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"errors"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var errTransactionDone = errors.New("leveldb: transaction already closed")

// Transaction is the transaction handle.
type Transaction struct {
	db        *DB
	lk        sync.RWMutex
	seq       uint64
	mem       *memDB // 每次trx都是会新建memtable
	tables    tFiles
	ikScratch []byte
	rec       sessionRecord
	stats     cStatStaging
	closed    bool
}

// Get gets the value for the given key. It returns ErrNotFound if the
// DB does not contains the key.
//
// The returned slice is its own copy, it is safe to modify the contents
// of the returned slice.
// It is safe to modify the contents of the argument after Get returns.
func (tr *Transaction) Get(key []byte, ro *opt.ReadOptions) ([]byte, error) {
	tr.lk.RLock()
	defer tr.lk.RUnlock()
	if tr.closed {
		return nil, errTransactionDone
	}
	return tr.db.get(tr.mem.DB, tr.tables, key, tr.seq, ro)
}

// Has returns true if the DB does contains the given key.
//
// It is safe to modify the contents of the argument after Has returns.
func (tr *Transaction) Has(key []byte, ro *opt.ReadOptions) (bool, error) {
	tr.lk.RLock()
	defer tr.lk.RUnlock()
	if tr.closed {
		return false, errTransactionDone
	}
	return tr.db.has(tr.mem.DB, tr.tables, key, tr.seq, ro)
}

// NewIterator returns an iterator for the latest snapshot of the transaction.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently while writes to the
// transaction. The resultant key/value pairs are guaranteed to be consistent.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// The returned iterator has locks on its own resources, so it can live beyond
// the lifetime of the transaction who creates them.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (tr *Transaction) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	tr.lk.RLock()
	defer tr.lk.RUnlock()
	if tr.closed {
		return iterator.NewEmptyIterator(errTransactionDone)
	}
	tr.mem.incref()
	return tr.db.newIterator(tr.mem, tr.tables, tr.seq, slice, ro)
}

// memtable转SSTable进行持久化，并存一下SSTable的元信息
func (tr *Transaction) flush() error {
	// Flush memdb.
	if tr.mem.Len() != 0 { //只要不是空事务，memtable都肯定会有数据的
		tr.stats.startTimer()
		iter := tr.mem.NewIterator(nil)
		// 注意：在事务还没提交的时候，但SSTable文件已经是被创建出来了的，
		// 如果事务回滚的话，SSTable文件就可以被删掉
		// 如果事务提交，那就把SSTable的信息写入到manifest里
		t, n, err := tr.db.s.tops.createFrom(iter) // 从迭代器里读出memtable的内容，生成SSTable
		iter.Release()
		tr.stats.stopTimer()
		if err != nil {
			return err
		}
		if tr.mem.getref() == 1 {
			tr.mem.Reset()
		} else {
			tr.mem.decref()
			tr.mem = tr.db.mpoolGet(0)
			tr.mem.incref()
		}
		tr.tables = append(tr.tables, t)
		tr.rec.addTableFile(0, t) // 存一下新增的SSTable的信息，此时SSTable文件已经是在磁盘上了的，这儿只是存一下SSTable的信息
		tr.stats.write += t.size
		tr.db.logf("transaction@flush created L0@%d N·%d S·%s %q:%q", t.fd.Num, n, shortenb(t.size), t.imin, t.imax)
	}
	return nil
}

// 事务的每次put，都会判断内存是否满了，满了的话就会触发memtable直接生成L0的SSTable
// 并进行memRotate,此时不是走memtable->immutable memtable的转换流程
func (tr *Transaction) put(kt keyType, key, value []byte) error {
	tr.ikScratch = makeInternalKey(tr.ikScratch, key, tr.seq+1, kt)
	if tr.mem.Free() < len(tr.ikScratch)+len(value) { // 内存无法容纳这次的写入
		if err := tr.flush(); err != nil {
			return err
		}
	}
	if err := tr.mem.Put(tr.ikScratch, value); err != nil {
		return err
	}
	tr.seq++ // 更新LSN
	return nil
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
// Please note that the transaction is not compacted until committed, so if you
// writes 10 same keys, then those 10 same keys are in the transaction.
//
// It is safe to modify the contents of the arguments after Put returns.
func (tr *Transaction) Put(key, value []byte, wo *opt.WriteOptions) error {
	tr.lk.Lock()
	defer tr.lk.Unlock()
	if tr.closed {
		return errTransactionDone
	}
	return tr.put(keyTypeVal, key, value)
}

// Delete deletes the value for the given key.
// Please note that the transaction is not compacted until committed, so if you
// writes 10 same keys, then those 10 same keys are in the transaction.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (tr *Transaction) Delete(key []byte, wo *opt.WriteOptions) error {
	tr.lk.Lock()
	defer tr.lk.Unlock()
	if tr.closed {
		return errTransactionDone
	}
	return tr.put(keyTypeDel, key, nil)
}

// Write apply the given batch to the transaction. The batch will be applied
// sequentially.
// Please note that the transaction is not compacted until committed, so if you
// writes 10 same keys, then those 10 same keys are in the transaction.
//
// It is safe to modify the contents of the arguments after Write returns.
func (tr *Transaction) Write(b *Batch, wo *opt.WriteOptions) error {
	if b == nil || b.Len() == 0 {
		return nil
	}

	tr.lk.Lock()
	defer tr.lk.Unlock()
	if tr.closed {
		return errTransactionDone
	}
	return b.replayInternal(func(i int, kt keyType, k, v []byte) error {
		return tr.put(kt, k, v)
	})
}

func (tr *Transaction) setDone() {
	tr.closed = true
	tr.db.tr = nil
	tr.mem.decref()
	<-tr.db.writeLockC
}

// Commit commits the transaction. If error is not nil, then the transaction is
// not committed, it can then either be retried or discarded.
//
// Other methods should not be called after transaction has been committed.
// 事务提交
func (tr *Transaction) Commit() error {
	if err := tr.db.ok(); err != nil {
		return err
	}

	tr.lk.Lock()
	defer tr.lk.Unlock()
	if tr.closed {
		return errTransactionDone
	}
	if err := tr.flush(); err != nil {
		// Return error, lets user decide either to retry or discard
		// transaction.
		return err
	}
	if len(tr.tables) != 0 { //本次事务触发了SSTable的新增，其实只要不是空事务，就一定会有新增SSTable的
		// Committing transaction.
		tr.rec.setSeqNum(tr.seq)
		tr.db.compCommitLk.Lock()
		tr.stats.startTimer()
		var cerr error
		for retry := 0; retry < 3; retry++ {
			// 因为leveldb是只允许单goroutine写入
			// 因此事务提交的时候，直接用当前的session作为版本数据即可
			cerr = tr.db.s.commit(&tr.rec, false)
			if cerr != nil {
				tr.db.logf("transaction@commit error R·%d %q", retry, cerr)
				select {
				case <-time.After(time.Second):
				case <-tr.db.closeC:
					tr.db.logf("transaction@commit exiting")
					tr.db.compCommitLk.Unlock()
					return cerr
				}
			} else {
				// Success. Set db.seq.
				tr.db.setSeq(tr.seq)
				break
			}
		}
		tr.stats.stopTimer()
		if cerr != nil {
			// Return error, lets user decide either to retry or discard
			// transaction.
			return cerr
		}

		// Update compaction stats. This is safe as long as we hold compCommitLk.
		tr.db.compStats.addStat(0, &tr.stats)

		// Trigger table auto-compaction.
		tr.db.compTrigger(tr.db.tcompCmdC)
		tr.db.compCommitLk.Unlock()

		// Additionally, wait compaction when certain threshold reached.
		// Ignore error, returns error only if transaction can't be committed.
		_ = tr.db.waitCompaction()
	}
	// Only mark as done if transaction committed successfully.
	tr.setDone()
	return nil
}

func (tr *Transaction) discard() {
	// Discard transaction.
	for _, t := range tr.tables {
		tr.db.logf("transaction@discard @%d", t.fd.Num)
		// Iterator may still use the table, so we use tOps.remove here.
		tr.db.s.tops.remove(t.fd)
	}
}

// Discard discards the transaction.
// This method is noop if transaction is already closed (either committed or
// discarded)
//
// Other methods should not be called after transaction has been discarded.
// 事务回滚的过程
func (tr *Transaction) Discard() {
	tr.lk.Lock()
	if !tr.closed {
		tr.discard()
		tr.setDone()
	}
	tr.lk.Unlock()
}

func (db *DB) waitCompaction() error {
	// L0层的SSTable个数过多，超过了设定的值，就会阻塞等待SSTable压缩，直至结束
	if db.s.tLen(0) >= db.s.o.GetWriteL0PauseTrigger() {
		return db.compTriggerWait(db.tcompCmdC)
	}
	return nil
}

// OpenTransaction opens an atomic DB transaction. Only one transaction can be
// opened at a time. Subsequent call to Write and OpenTransaction will be blocked
// until in-flight transaction is committed or discarded.
// The returned transaction handle is safe for concurrent use.
//
// Transaction is very expensive and can overwhelm compaction, especially if
// transaction size is small. Use with caution.
// The rule of thumb is if you need to merge at least same amount of
// `Options.WriteBuffer` worth of data then use transaction, otherwise don't.
//
// The transaction must be closed once done, either by committing or discarding
// the transaction.
// Closing the DB will discard open transaction.
// 1.只会有全局单事务，因为要抢占写锁
// 2.小的写入不要开事务，假设写入100KB的数据，那么就会直接产生100KB的SSTable文件；
// 同时L0层会有很多小文件，文件很多的时候就会触发LO SSTable压缩了
// 3.大数据量可以开事务，这样一次性生成大的SSTable，减少compaction的次数
func (db *DB) OpenTransaction() (*Transaction, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	// The write happen synchronously.
	select {
	case db.writeLockC <- struct{}{}: //阻塞等待抢占写锁
	case err := <-db.compPerErrC:
		return nil, err
	case <-db.closeC:
		return nil, ErrClosed
	}

	if db.tr != nil {
		panic("leveldb: has open transaction")
	}

	// Flush current memdb.
	// 一旦开始事务，如果memtable中存在数据的话，就一定会触发memtable的rotate！
	if db.mem != nil && db.mem.Len() != 0 {
		if _, err := db.rotateMem(0, true); err != nil {
			return nil, err
		}
	}

	// Wait compaction when certain threshold reached.
	// 如果L0层的SSTable个数过多，超过了设定的值，就会阻塞等待SSTable压缩，直至结束
	if err := db.waitCompaction(); err != nil {
		return nil, err
	}

	tr := &Transaction{
		db:  db,
		seq: db.seq,
		mem: db.mpoolGet(0),
	}
	tr.mem.incref()
	db.tr = tr
	return tr, nil
}
