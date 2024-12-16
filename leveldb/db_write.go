// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (db *DB) writeJournal(batches []*Batch, seq uint64, sync bool) error {
	wr, err := db.journal.Next() //获取到可用的block writer了
	if err != nil {
		return err
	}
	if err := writeBatchesWithHeader(wr, batches, seq); err != nil {
		return err
	}
	if err := db.journal.Flush(); err != nil {
		return err
	}
	if sync {
		return db.journalWriter.Sync() //刷盘
	}
	return nil
}

// rotateMem
// 走到这儿一定是memTable容量不足，需要扩容了
// 先将数据转成immutable memtable，同时创建一个memTable
// 这儿会两次往mcompCmdC这个channel里写数据来触发压缩，对应的两种情况是：
// case1:
// a.第一次触发，存在还未结束压缩的immutable memtable，那么第一次触发会阻塞等待immutable memtable结束压缩
// b.前序的压缩结束，开始新建memTable(newMemh函数)
// c. 新建memTable结束，第二次触发压缩，这个时候就看是否需要阻塞等待压缩结束了(通过参数wait控制)
// case2:
// a.第一次触发，不存在还未结束压缩的immutable memtable，
// 那么第一次触发并不会有immutable memtable会被压缩(因为db.getFrozenMem()==nil)
// b.开始新建memTable(newMemh函数)
// c. 新建memTable结束，第二次触发压缩，这个时候就看是否需要阻塞等待压缩结束了(通过参数wait控制)
// ========
// 总结一下，两次往mcompCmdC里写数据，
// 本质上是为了确保同一时间内只会有一个immutable memtable在被压缩
func (db *DB) rotateMem(n int, wait bool) (mem *memDB, err error) {
	retryLimit := 3
retry:
	// Wait for pending memdb compaction.
	// case1:触发immutable memtable 转换成 SSTable 文件并持久化；
	// case2:也有可能不存在immutable memtable，那block时间会很短
	err = db.compTriggerWait(db.mcompCmdC)
	if err != nil {
		return
	}
	retryLimit--

	// Create new memdb and journal.
	mem, err = db.newMem(n) // 创建一个新的memtable，n是即将写入的字节数
	if err != nil {
		if err == errHasFrozenMem {
			if retryLimit <= 0 {
				panic("BUG: still has frozen memdb")
			}
			goto retry
		}
		return
	}

	// Schedule memdb compaction.
	// 走到这儿的时候，memtable已经创建，同时也已经存在了immutable memtable
	// 那么往db.mcompCmdC里写数据，其实一定会有immutable memtable被压缩，
	// 这是这次操作是否需要等待，就看wait怎么传的了
	if wait {
		err = db.compTriggerWait(db.mcompCmdC)
	} else {
		//尝试去触发immutable memtable持久化
		//如果channel满了，那也不需要处理，可以直接返回
		//下次rotateMem函数再被调用时，则一定会触发对应的immutable memtable持久化
		// 因为本函数第一次是一定阻塞写db.mcompCmdC+阻塞等待immutable memtable持久化的结果
		db.compTrigger(db.mcompCmdC)
	}
	return
}

// flush
// 接收的参数n是即将写入的字节数，调用flush是为了确保内存可写，
// 如果memtable不足以支撑n字节的数据，就需要先把memtable的数据
// 转成immutable memtable，并尝试触发immutable memtable持久化
// 以便于即将到来的写入得以更新memtable。
// 除此之外，写入的流控也是在这实现的，如果写得太快，这儿会sleep；或者是强制触发
// 返回值：
// mdb: 可写的memtable,这样调用方就不需要关心memtable和immutable memtable的切换了
// mdbFree: 内存可用空间，至于为啥不直接返回 memDB.Free() 的值，
// 是因为这儿需要保证memtable内存可用空间大于等于即将写入的数据,同时对空memtable第一次写入做了不给merge的限制
func (db *DB) flush(n int) (mdb *memDB, mdbFree int, err error) {
	delayed := false
	slowdownTrigger := db.s.o.GetWriteL0SlowdownTrigger()
	pauseTrigger := db.s.o.GetWriteL0PauseTrigger()
	flush := func() (retry bool) {
		mdb = db.getEffectiveMem() //这里增加了对memTable的引用，如果需要rotate的话，需要在rotate之前decref
		if mdb == nil {
			err = ErrClosed
			return false
		}
		defer func() {
			if retry { // 操作失败，需要重试
				mdb.decref() // 引用计数减1
				mdb = nil    // 重置，下次for循环周期会重新赋值的
			}
		}()
		tLen := db.s.tLen(0) // 获取第0层SSTable的个数
		mdbFree = mdb.Free() // 获取memdb的可用空间
		switch {
		case tLen >= slowdownTrigger && !delayed: // level-0的文件较多，达到了写限流，sleep一下，触发下次循环
			delayed = true // 这儿标记一下写入已经是delay状态，防止写入一直卡在这个条件里不断触发for循环
			time.Sleep(time.Millisecond)
		case mdbFree >= n:
			// 内存可用空间大于等于即将写入的数据，可以直接退出for循环，让本次的写入后续去写memdb
			return false
		case tLen >= pauseTrigger: // level-0的SSTable较多，达到了写暂停的条件啦，需要对table进行压缩
			delayed = true
			// Set the write paused flag explicitly.
			atomic.StoreInt32(&db.inWritePaused, 1)
			err = db.compTriggerWait(db.tcompCmdC) // 主动触发对SSTable的压缩，然后触发下次for循环
			// Unset the write paused flag.
			atomic.StoreInt32(&db.inWritePaused, 0)
			if err != nil {
				return false
			}
		default: // 走到default的都会退出flush循环，往下走去触发写Log文件
			// tips：这儿一定是mdbFree<n，即memTable可用空间不足，
			// 必定需要先把memtable进行rotate！！！
			// Allow memdb to grow if it has no entry.
			if mdb.Len() == 0 {
				// 没数据，那就先设置成memtable空闲空间为要写入的字节数
				// 上游使用的时候就会认为内存只够这次写入(具体见db.writeLocked的mergeCap)，
				//不会去允许merge其他写操作了
				// 这块是个权衡吧，空memtable先保第一次写操作能顺利执行，不允许merge的话影响还好。
				// =============================
				// 会走到这儿的场景是：新创建的memtable，初始容量较小，且要写入的数据n比较大，需要扩容才能容纳新数据
				// 同时因为是空的memtable，并不需要rotate，
				// 因为memTable在写入的时候会自动扩容(Put方法里用的append,golang会自动扩容)
				mdbFree = n
			} else {
				// 会走到这儿的场景是：memtable已经有数据了，但是数据量不够，需要rotate
				mdb.decref() // rotate之前，减引用，避免内存泄露
				// 这儿传了false，就是指写操作不需要阻塞等待immutalbe memtable的压缩
				// 可能这就是LevelDB写入性能高的原因之一 :)
				mdb, err = db.rotateMem(n, false)
				if err == nil {
					mdbFree = mdb.Free()
				} else {
					mdbFree = 0
				}
			}
			return false
		}
		return true
	}
	start := time.Now()
	for flush() {
	}
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
	} else if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		atomic.AddInt32(&db.cWriteDelayN, int32(db.writeDelayN))
		atomic.AddInt64(&db.cWriteDelay, int64(db.writeDelay))
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	return
}

type writeMerge struct {
	sync       bool
	batch      *Batch
	keyType    keyType
	key, value []byte
}

func (db *DB) unlockWrite(overflow bool, merged int, err error) {
	for i := 0; i < merged; i++ {
		db.writeAckC <- err
	}
	if overflow {
		// Pass lock to the next write (that failed to merge).
		db.writeMergedC <- false //写入太大，不给merge，通知尝试merge的请求，让它自己写去，这次的大哥不带你上车了
	} else {
		// Release lock.
		<-db.writeLockC
	}
}

// ourBatch is batch that we can modify.
func (db *DB) writeLocked(batch, ourBatch *Batch, merge, sync bool) error {
	// Try to flush memdb. This method would also trying to throttle writes
	// if it is too fast and compaction cannot catch-up.
	mdb, mdbFree, err := db.flush(batch.internalLen)
	if err != nil {
		db.unlockWrite(false, 0, err)
		return err
	}
	defer mdb.decref()

	var (
		overflow bool
		merged   int
		batches  = []*Batch{batch}
	)

	if merge { //需要merge写操作，但并不是阻塞等待merge的，而是尝试merge，没成功也会继续往下走
		// Merge limit.
		var mergeLimit int
		if batch.internalLen > 128<<10 { //128KB
			mergeLimit = (1 << 20) - batch.internalLen // 1MB减去数据量
		} else {
			mergeLimit = 128 << 10 // 128KB
		}
		//memTable的可用空间减去数据量，得到的结果就是本次写入可以带多少数据一起上车
		mergeCap := mdbFree - batch.internalLen
		if mergeLimit > mergeCap { //超了，那就把memtable可用的全写满吧，不允许merge更多的数据
			mergeLimit = mergeCap
		}

	merge:
		for mergeLimit > 0 {
			// 存在default条件，因此是非阻塞的尝试merge写操作
			select {
			case incoming := <-db.writeMergeC:
				if incoming.batch != nil { //batch写，走 DB.Write方法进来的
					// Merge batch.
					if incoming.batch.internalLen > mergeLimit { //batch写太大了，那就不允许merge
						overflow = true
						break merge
					}
					batches = append(batches, incoming.batch)
					mergeLimit -= incoming.batch.internalLen
				} else { // 单条kv的写
					// Merge put.
					internalLen := len(incoming.key) + len(incoming.value) + 8
					if internalLen > mergeLimit {
						overflow = true
						break merge
					}
					// 我理解这个ourBatch的意义，是因为往下游写的最小单位是一个batch对象
					// 如果用户是单条kv的写入，那我们就帮忙封装出一个batch对象
					if ourBatch == nil {
						ourBatch = db.batchPool.Get().(*Batch)
						ourBatch.Reset()
						batches = append(batches, ourBatch)
					}
					// We can use same batch since concurrent write doesn't
					// guarantee write order.
					// //因为是在一个for循环里，有可能有多个并发的merge请求过来
					// 在这里不会去保证顺序性，谁到了就加到batch的kv里
					ourBatch.appendRec(incoming.keyType, incoming.key, incoming.value)
					mergeLimit -= internalLen
				}
				sync = sync || incoming.sync // 只要任意一个batch需要同步，那就全都需要同步刷盘
				merged++
				//通知这次merge的小弟说，你的merge请求大哥同意merge了，让小弟等着writeAckC的结果就知道有没有写成功了
				db.writeMergedC <- true

			default:
				break merge
			}
		}
	}

	// Release ourBatch if any.
	if ourBatch != nil {
		defer db.batchPool.Put(ourBatch)
	}

	// Seq number.
	seq := db.seq + 1

	// Write journal.
	// 开始顺序写log文件
	if err := db.writeJournal(batches, seq, sync); err != nil {
		db.unlockWrite(overflow, merged, err)
		return err
	}

	// 写log文件结束，开始更新内存memtable
	// Put batches.
	for _, batch := range batches {
		if err := batch.putMem(seq, mdb.DB); err != nil {
			panic(err)
		}
		seq += uint64(batch.Len()) // 每一次操作都需要自增seq，所以这增加了kv的个数
	}

	// Incr seq number.
	db.addSeq(uint64(batchesLen(batches))) //更新DB的seq number

	// Rotate memdb if it's reach the threshold.
	// 假设下次也写预测大小为batch.internalLen的数据，
	//如果本次写完之后空间不够了，那就异步rotate，
	// 兴许下次写操作过来的时候就不会需要触发memRotate了
	if batch.internalLen >= mdbFree {
		if _, err := db.rotateMem(0, false); err != nil {
			db.unlockWrite(overflow, merged, err)
			return err
		}
	}

	db.unlockWrite(overflow, merged, nil) //释放写锁
	return nil
}

// Write apply the given batch to the DB. The batch records will be applied
// sequentially. Write might be used concurrently, when used concurrently and
// batch is small enough, write will try to merge the batches. Set NoWriteMerge
// option to true to disable write merge.
//
// It is safe to modify the contents of the arguments after Write returns but
// not before. Write will not modify content of the batch.
func (db *DB) Write(batch *Batch, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil || batch == nil || batch.Len() == 0 {
		return err
	}

	// If the batch size is larger than write buffer, it may justified to write
	// using transaction instead. Using transaction the batch will be written
	// into tables directly, skipping the journaling.
	if batch.internalLen > db.s.o.GetWriteBuffer() && !db.s.o.GetDisableLargeBatchTransaction() {
		tr, err := db.OpenTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch, wo); err != nil {
			tr.Discard()
			return err
		}
		return tr.Commit()
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge { // 需要尝试merge写请求
		select {
		case db.writeMergeC <- writeMerge{sync: sync, batch: batch}: // 尝试把这次请求的batch发送给正在写的写请求，看能够merge成功
			if <-db.writeMergedC { // 阻塞等待，读取到了merge成功的信号，说明这次写请求成功上车啦，等前面的大哥帮忙写就行了
				// Write is merged.
				return <-db.writeAckC // 看看帮忙merge的大哥是否写成功
			}
			// 走到这儿说明没有大哥帮我们把写请求带上车，因为这次batch写太大了，那我们自己写吧！
			// 接下来就是调用DB.writeLocked方法开始去写Log文件
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
			// 走到这儿就是没有锁抢占，那也没人帮我们把写请求带上车，那我们自己写吧！
			// 接下来就是调用DB.writeLocked方法开始去写Log文件
		case err := <-db.compPerErrC: // 如果存在持久化Log的错误，那这次写请求就失败了
			// Compaction error.
			return err
		case <-db.closeC: // DB关掉了，那就不写了
			// Closed
			return ErrClosed
		}
	} else { // 不需要merge写请求，开始申请写锁
		select {
		case db.writeLockC <- struct{}{}: // 堵塞等待写锁
			// Write lock acquired.
			// 分到了写锁，调用DB.writeLocked方法开始去写Log文件
		case err := <-db.compPerErrC: // 如果存在持久化Log的错误，那这次写请求就失败了
			// Compaction error.
			return err
		case <-db.closeC: // DB关掉了，那就不写了
			// Closed
			return ErrClosed
		}
	}

	return db.writeLocked(batch, nil, merge, sync) // TODO(qinguizhan): 这儿为啥ourBatch是空？
}

func (db *DB) putRec(kt keyType, key, value []byte, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil {
		return err
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, keyType: kt, key: key, value: value}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	batch := db.batchPool.Get().(*Batch)
	batch.Reset()
	batch.appendRec(kt, key, value)
	return db.writeLocked(batch, batch, merge, sync)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map. Write merge also applies for Put, see
// Write.
//
// It is safe to modify the contents of the arguments after Put returns but not
// before.
func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeVal, key, value, wo)
}

// Delete deletes the value for the given key. Delete will not returns error if
// key doesn't exist. Write merge also applies for Delete, see Write.
//
// It is safe to modify the contents of the arguments after Delete returns but
// not before.
func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeDel, key, nil, wo)
}

func isMemOverlaps(icmp *iComparer, mem *memdb.DB, min, max []byte) bool {
	iter := mem.NewIterator(nil)
	defer iter.Release()
	return (max == nil || (iter.First() && icmp.uCompare(max, internalKey(iter.Key()).ukey()) >= 0)) &&
		(min == nil || (iter.Last() && icmp.uCompare(min, internalKey(iter.Key()).ukey()) <= 0))
}

// CompactRange compacts the underlying DB for the given key range.
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data. This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// A nil Range.Start is treated as a key before all keys in the DB.
// And a nil Range.Limit is treated as a key after all keys in the DB.
// Therefore if both is nil then it will compact entire DB.
func (db *DB) CompactRange(r util.Range) error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Check for overlaps in memdb.
	mdb := db.getEffectiveMem()
	if mdb == nil {
		return ErrClosed
	}
	defer mdb.decref()
	if isMemOverlaps(db.s.icmp, mdb.DB, r.Start, r.Limit) {
		// Memdb compaction.
		if _, err := db.rotateMem(0, false); err != nil {
			<-db.writeLockC
			return err
		}
		<-db.writeLockC
		if err := db.compTriggerWait(db.mcompCmdC); err != nil {
			return err
		}
	} else {
		<-db.writeLockC
	}

	// Table compaction.
	return db.compTriggerRange(db.tcompCmdC, -1, r.Start, r.Limit)
}

// SetReadOnly makes DB read-only. It will stay read-only until reopened.
func (db *DB) SetReadOnly() error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
		db.compWriteLocking = true
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Set compaction read-only.
	select {
	case db.compErrSetC <- ErrReadOnly:
	case perr := <-db.compPerErrC:
		return perr
	case <-db.closeC:
		return ErrClosed
	}

	return nil
}
