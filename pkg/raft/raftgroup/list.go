package raftgroup

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

type linkedList struct {
	raftMap   map[string]IRaft // 使用 map 实现 O(1) 查找
	raftSlice []IRaft          // 缓存的切片，用于快速遍历
	mu        sync.RWMutex     // 保护并发访问
	dirty     bool             // 标记切片是否需要重建
	wklog.Log
}

// newLinkedList 创建新的链表
func newLinkedList() *linkedList {
	return &linkedList{
		raftMap:   make(map[string]IRaft),
		raftSlice: make([]IRaft, 0, 64), // 预分配容量
		Log:       wklog.NewWKLog("raftGroup.linkedList"),
	}
}

// push 添加 raft 实例
func (ll *linkedList) push(raft IRaft) {
	ll.mu.Lock()
	defer ll.mu.Unlock()

	key := raft.Key()
	if _, exists := ll.raftMap[key]; exists {
		ll.Foucs("push: raft exist", zap.String("key", key))
		return
	}

	ll.raftMap[key] = raft
	ll.dirty = true // 标记需要重建切片
}

// remove 从集合中移除 raft 实例
func (ll *linkedList) remove(key string) {
	ll.mu.Lock()
	defer ll.mu.Unlock()

	if _, exists := ll.raftMap[key]; !exists {
		return
	}

	delete(ll.raftMap, key)
	ll.dirty = true // 标记需要重建切片
}

// get 获取指定 key 的 raft 实例 - O(1) 复杂度
func (ll *linkedList) get(key string) IRaft {
	ll.mu.RLock()
	defer ll.mu.RUnlock()

	return ll.raftMap[key]
}

// count 返回 raft 实例数量
func (ll *linkedList) count() int {
	ll.mu.RLock()
	defer ll.mu.RUnlock()

	return len(ll.raftMap)
}

// rebuildSliceNoLock 重建缓存的切片（需在持有锁的情况下调用）
func (ll *linkedList) rebuildSliceNoLock() {
	if !ll.dirty {
		return
	}

	// 重用已有切片的容量
	ll.raftSlice = ll.raftSlice[:0]

	// 确保容量足够
	if cap(ll.raftSlice) < len(ll.raftMap) {
		ll.raftSlice = make([]IRaft, 0, len(ll.raftMap))
	}

	for _, raft := range ll.raftMap {
		ll.raftSlice = append(ll.raftSlice, raft)
	}

	ll.dirty = false
}

// readHandlers 将所有 raft 实例追加到提供的切片中
func (ll *linkedList) readHandlers(rafts *[]IRaft) {
	ll.mu.Lock() // 使用写锁以便在需要时重建切片
	ll.rebuildSliceNoLock()

	// 在持有锁的情况下复制切片内容
	*rafts = append(*rafts, ll.raftSlice...)
	ll.mu.Unlock()
}

// all 返回所有 raft 实例的副本
func (ll *linkedList) all() []IRaft {
	ll.mu.Lock()
	defer ll.mu.Unlock()

	ll.rebuildSliceNoLock()

	// 返回副本以避免外部修改
	result := make([]IRaft, len(ll.raftSlice))
	copy(result, ll.raftSlice)
	return result
}
