// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// ErrBatchCorrupted records reason of batch corruption. This error will be
// wrapped with errors.ErrCorrupted.
type ErrBatchCorrupted struct {
	Reason string
}

func (e *ErrBatchCorrupted) Error() string {
	return fmt.Sprintf("leveldb: batch corrupted: %s", e.Reason)
}

func newErrBatchCorrupted(reason string) error {
	return errors.NewErrCorrupted(storage.FileDesc{}, &ErrBatchCorrupted{reason})
}

const (
	batchHeaderLen = 8 + 4 // 定长的header：seq(8字节) + batchLen(4字节)
	batchGrowLimit = 3000
)

// BatchReplay wraps basic batch operations.
type BatchReplay interface {
	Put(key, value []byte)
	Delete(key []byte)
}

// batchIndex 是一次batch写操作中，单个kv在batch中的元信息
type batchIndex struct {
	keyType            keyType //kv操作的类型
	keyPos, keyLen     int     //kv key在batch data数组中的位置
	valuePos, valueLen int     //kv value在batch data数组中的位置
}

// 从包含了很多个kv操作的数据中，获取单个kv实际的key
func (index batchIndex) k(data []byte) []byte {
	return data[index.keyPos : index.keyPos+index.keyLen]
}

// 从包含了很多个kv操作的数据中，获取单个kv实际的数据
func (index batchIndex) v(data []byte) []byte {
	if index.valueLen != 0 {
		return data[index.valuePos : index.valuePos+index.valueLen]
	}
	return nil
}

// Batch is a write batch.
type Batch struct {
	data  []byte       // 本次batch对应的数据，顺序排列在一起，要切分去读的话，元信息在index里
	index []batchIndex // 这次batch所涉及的key/value对的元信息

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	// 本次batch写操作需要占用的字节数
	internalLen int

	// growLimit is the threshold in order to slow down the memory allocation
	// for batch when the number of accumulated entries exceeds value.
	//
	// batchGrowLimit is used as the default threshold if it's not configured.
	growLimit int
}

func (b *Batch) grow(n int) {
	o := len(b.data)
	if cap(b.data)-o < n { //剩余空间不足了
		limit := batchGrowLimit
		if b.growLimit > 0 {
			limit = b.growLimit
		}
		div := 1 //计算增长因子，既需要考虑本次扩容后能减少后续的扩容次数，也需要考虑不要占用太多内存
		if len(b.index) > limit {
			div = len(b.index) / limit
		}
		ndata := make([]byte, o, o+n+o/div)
		copy(ndata, b.data)
		b.data = ndata
	}
}

// 把kv操作加入到batch中
func (b *Batch) appendRec(kt keyType, key, value []byte) {
	n := 1 + binary.MaxVarintLen32 + len(key)
	if kt == keyTypeVal {
		n += binary.MaxVarintLen32 + len(value)
	}
	b.grow(n)
	index := batchIndex{keyType: kt}
	o := len(b.data)
	data := b.data[:o+n]
	data[o] = byte(kt)
	o++
	o += binary.PutUvarint(data[o:], uint64(len(key)))
	index.keyPos = o
	index.keyLen = len(key)
	o += copy(data[o:], key)
	if kt == keyTypeVal {
		o += binary.PutUvarint(data[o:], uint64(len(value)))
		index.valuePos = o
		index.valueLen = len(value)
		o += copy(data[o:], value)
	}
	b.data = data[:o]
	b.index = append(b.index, index)
	b.internalLen += index.keyLen + index.valueLen + 8
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(key, value []byte) {
	b.appendRec(keyTypeVal, key, value)
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(key []byte) {
	b.appendRec(keyTypeDel, key, nil)
}

// Dump dumps batch contents. The returned slice can be loaded into the
// batch using Load method.
// The returned slice is not its own copy, so the contents should not be
// modified.
func (b *Batch) Dump() []byte {
	return b.data
}

// Load loads given slice into the batch. Previous contents of the batch
// will be discarded.
// The given slice will not be copied and will be used as batch buffer, so
// it is not safe to modify the contents of the slice.
func (b *Batch) Load(data []byte) error {
	return b.decode(data, -1)
}

// Replay replays batch contents.
func (b *Batch) Replay(r BatchReplay) error {
	for _, index := range b.index {
		switch index.keyType {
		case keyTypeVal:
			r.Put(index.k(b.data), index.v(b.data))
		case keyTypeDel:
			r.Delete(index.k(b.data))
		}
	}
	return nil
}

// Len returns number of records in the batch.
// 本次batch写操作涉及的key/value对个数
func (b *Batch) Len() int {
	return len(b.index)
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.index = b.index[:0]
	b.internalLen = 0
}

func (b *Batch) replayInternal(fn func(i int, kt keyType, k, v []byte) error) error {
	for i, index := range b.index {
		if err := fn(i, index.keyType, index.k(b.data), index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) append(p *Batch) {
	ob := len(b.data)
	oi := len(b.index)
	b.data = append(b.data, p.data...)
	b.index = append(b.index, p.index...)
	b.internalLen += p.internalLen

	// Updating index offset.
	if ob != 0 {
		for ; oi < len(b.index); oi++ {
			index := &b.index[oi]
			index.keyPos += ob
			if index.valueLen != 0 {
				index.valuePos += ob
			}
		}
	}
}

func (b *Batch) decode(data []byte, expectedLen int) error {
	b.data = data
	b.index = b.index[:0]
	b.internalLen = 0
	err := decodeBatch(data, func(i int, index batchIndex) error {
		b.index = append(b.index, index)
		b.internalLen += index.keyLen + index.valueLen + 8
		return nil
	})
	if err != nil {
		return err
	}
	if expectedLen >= 0 && len(b.index) != expectedLen {
		return newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", expectedLen, len(b.index)))
	}
	return nil
}

// 在写log文件后，将batch写入到memdb中
func (b *Batch) putMem(seq uint64, mdb *memdb.DB) error {
	var ik []byte
	for i, index := range b.index {
		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.keyType)
		if err := mdb.Put(ik, index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func newBatch() interface{} {
	return &Batch{}
}

// MakeBatch returns empty batch with preallocated buffer.
func MakeBatch(n int) *Batch {
	return &Batch{data: make([]byte, 0, n)}
}

// BatchConfig contains the config options for batch.
type BatchConfig struct {
	// InitialCapacity is the batch initial capacity to preallocate.
	//
	// The default value is 0.
	InitialCapacity int

	// GrowLimit is the limit (in terms of entry) of how much buffer
	// can grow each cycle.
	//
	// Initially the buffer will grow twice its current size until
	// GrowLimit threshold is reached, after that the buffer will grow
	// up to GrowLimit each cycle. This buffer grow size in bytes is
	// loosely calculated from average entry size multiplied by GrowLimit.
	//
	// Generally, the memory allocation step is larger if this value
	// is configured large, vice versa.
	//
	// The default value is 3000.
	GrowLimit int
}

// MakeBatchWithConfig initializes a batch object with the given configs.
func MakeBatchWithConfig(config *BatchConfig) *Batch {
	var batch = new(Batch)
	if config != nil {
		if config.InitialCapacity > 0 {
			batch.data = make([]byte, 0, config.InitialCapacity)
		}
		if config.GrowLimit > 0 {
			batch.growLimit = config.GrowLimit
		}
	}
	return batch
}

func decodeBatch(data []byte, fn func(i int, index batchIndex) error) error {
	var index batchIndex
	for i, o := 0, 0; o < len(data); i++ {
		// Key type.
		index.keyType = keyType(data[o])
		if index.keyType > keyTypeVal {
			return newErrBatchCorrupted(fmt.Sprintf("bad record: invalid type %#x", uint(index.keyType)))
		}
		o++

		// Key.
		x, n := binary.Uvarint(data[o:])
		o += n
		if n <= 0 || o+int(x) > len(data) {
			return newErrBatchCorrupted("bad record: invalid key length")
		}
		index.keyPos = o
		index.keyLen = int(x)
		o += index.keyLen

		// Value.
		if index.keyType == keyTypeVal {
			x, n = binary.Uvarint(data[o:])
			o += n
			if n <= 0 || o+int(x) > len(data) {
				return newErrBatchCorrupted("bad record: invalid value length")
			}
			index.valuePos = o
			index.valueLen = int(x)
			o += index.valueLen
		} else {
			index.valuePos = 0
			index.valueLen = 0
		}

		if err := fn(i, index); err != nil {
			return err
		}
	}
	return nil
}

func decodeBatchToMem(data []byte, expectSeq uint64, mdb *memdb.DB) (seq uint64, batchLen int, err error) {
	seq, batchLen, err = decodeBatchHeader(data)
	if err != nil {
		return 0, 0, err
	}
	if seq < expectSeq {
		return 0, 0, newErrBatchCorrupted("invalid sequence number")
	}
	data = data[batchHeaderLen:]
	var ik []byte
	var decodedLen int
	err = decodeBatch(data, func(i int, index batchIndex) error {
		if i >= batchLen {
			return newErrBatchCorrupted("invalid records length")
		}
		ik = makeInternalKey(ik, index.k(data), seq+uint64(i), index.keyType)
		if err := mdb.Put(ik, index.v(data)); err != nil {
			return err
		}
		decodedLen++
		return nil
	})
	if err == nil && decodedLen != batchLen {
		err = newErrBatchCorrupted(fmt.Sprintf("invalid records length: %d vs %d", batchLen, decodedLen))
	}
	return
}

func encodeBatchHeader(dst []byte, seq uint64, batchLen int) []byte {
	dst = ensureBuffer(dst, batchHeaderLen)                  //确保一定能写12个字节
	binary.LittleEndian.PutUint64(dst, seq)                  //先写seq,uint64是8个字节
	binary.LittleEndian.PutUint32(dst[8:], uint32(batchLen)) //再写batchLen，也就是kv的个数，uint32是4个字节
	return dst
}

func decodeBatchHeader(data []byte) (seq uint64, batchLen int, err error) {
	if len(data) < batchHeaderLen {
		return 0, 0, newErrBatchCorrupted("too short")
	}

	seq = binary.LittleEndian.Uint64(data)
	batchLen = int(binary.LittleEndian.Uint32(data[8:]))
	if batchLen < 0 {
		return 0, 0, newErrBatchCorrupted("invalid records length")
	}
	return
}

// 返回kv对的个数
func batchesLen(batches []*Batch) int {
	batchLen := 0
	for _, batch := range batches {
		batchLen += batch.Len()
	}
	return batchLen
}

// 格式：
// [batch header: seq + total_count]
// [batch1 data]
// [batch2 data]
// [batch3 data]
func writeBatchesWithHeader(wr io.Writer, batches []*Batch, seq uint64) error {
	// 写header
	if _, err := wr.Write(encodeBatchHeader(nil, seq, batchesLen(batches))); err != nil {
		return err
	}
	for _, batch := range batches {
		// 注意，这儿其实就是在执行singleWriter的Write方法
		if _, err := wr.Write(batch.data); err != nil {
			return err
		}
	}
	return nil
}
