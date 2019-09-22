// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"sync"

	pb "go.etcd.io/etcd/raft/raftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
type Storage interface {
	// TODO(tbg): split this into two interfaces, LogStorage and StateStorage.

	// 返回 Storage 中记录状态信息，返回的是 HardState 和 ConfState 实例
	//
	// HardState: 在前面介绍 Raft 协议时提到，集群中每个节点都需要保存一些必需的基本信息，
	// 在etcd中将其封装 HardState, 其中主要封装了当前任期号(Term字段)、当前
	// 节点在该任期中将选票投给了哪个节点（Vote字段）、已提交Entry记录的位置
	//（Commit字段，即最后一条已提交的记录索引值）
	//
	// ConfState: 封装了当前集群中所有节点的ID(Nodes字段)
	// InitialState returns the saved HardState and ConfState information.
	InitialState() (pb.HardState, pb.ConfState, error)

	// 在 Storage 中记录了当前节点的所有 Entry 记录, Entries 方法返回指定范围
	// 的 Entry 记录([lo, hi])，第三个参数(maxSize)限定了返回 Entry 集合的字节数上限
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)

	// 查询指定index对应的entry的term值
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)

	//该方法返回Storage中记录的第一条Entry的索引值(Index)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)

	// 该方法返回Storage中记录的第一条Enrty的索引值，在该Entry之前的
	// 所有Entry都已经被包含进了最近的一次SnapShot中
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)

	// 返回最近一次生成的快照数据
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage 在内存上维护状态信息（hardState）、快照数据(snapshot)以及所有的
// Entry 记录（ents字段），在 MemoryStorage字段中维护了快照数据之后的所有Entry记录
// MemoryStorage implements the Storage interface backed by an
// in-memory array.
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: make([]pb.Entry, 1),
	}
}

// InitialState 方法直接返回 hardstate 字段中记录的HardState实例
// 并使用快照的元数据中记录信息创建ConfState实例
// InitialState implements the Storage interface.
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// 查询指定范围的entry
// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index

	// 如果待查询的最小index小于firstindex，直接抛出异常
	if lo <= offset {
		return nil, ErrCompacted
	}

	// 如果待查询的最大index小于lastIndex，直接抛出异常
	if hi > ms.lastIndex()+1 {
		raftLogger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}

	// 如果memorystorage.ents只包含一条Entry, 则其为空的entry，抛出异常
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	// 获取lo - hi 之间的entry
	ents := ms.ents[lo-offset : hi-offset]
	// 限制返回entry切片的总字节大小
	return limitSize(ents, maxSize), nil
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// 返回ents数组中最后一个元素的index
// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// 返回ents数组中第一个元素的index
// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// 当MemoryStorage需要更新快照，调用此方法将SnapShot实例保存在MemoryState中，
// 在节点重启时，会用过读取快照文件对应的snapshot实例，然后保存到memorystorage中
// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	// 通过快照的元数据比较当前 MemoryStorage 中记录的 snapshot
	// 与待处理的 snapshot 数据的新旧程度
	//handle check for old snapshot being applied
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index

	// 比较两个 pb.snapshot 所包含的最后一条记录的 Index 值
	if msIndex >= snapIndex {
		return ErrSnapOutOfDate // 如果待处理的snapshot数据比较旧，则直接抛出异常
	}

	// 更新snapshot字段
	// 重置 memorystorage.ents 字段， 此时在ents中只有一个空的 entry 实例
	ms.snapshot = snap
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// i 是新建 snapshot 包含的最大索引值，cs是当前集群的状态
// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()

	// i 必需大于单曲snapshot包含的最大index，并且小于memorystorage的lastindex
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		raftLogger.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()

	// 边界检查：
	// firstIndex < compactIndex < lastIndex
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		raftLogger.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset

	// 创建新的切片，用来存储compactIndex之后的entry
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {

	// 检查 entries 切片的长度，
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	// 获取当前memorystorage的FirstIndex值
	first := ms.firstIndex()

	// 获取待添加的最后一条entry的index值
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first { //enties切片中所有的entry都过时，无需添加任何entry
		return nil
	}

	//first之前的entry已经记录到snapshot中，不应该再记录到ents中，所以将这部分entry截掉
	// truncate compacted entries
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	// 计算entries切片中第一条可用entry与first之间的差距
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// 保留 MemoeyStage.ents 中 first-offset 的部分，offset之后的部分被抛弃
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		// 将待追加的entry追加到memorystorage.ents中
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		// 直接将待追加的日志记录追加到memorystorage中
		ms.ents = append(ms.ents, entries...)
	default: // 异常处理
		raftLogger.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
