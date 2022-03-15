package utils

import (
	"bytes"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"sync"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int // max level for skip list
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	//implement me here!!!
	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxLevel - 1,
		rand:     r,
	}
}

type Element struct {
	levels []*Element   // level array e.g levels[i] is a list for level i
	entry  *codec.Entry // data
	score  float64      // to speed up
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	//implement me here!!!
	list.lock.Lock()
	defer list.lock.Unlock()
	prevs := make([]*Element, list.maxLevel+1)

	key := data.Key
	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header

	for i := maxLevel; i >= 0; i-- {
		for curr := prev.levels[i]; curr != nil; curr = prev.levels[i] {
			// find the position to insert
			if comp := list.compare(keyScore, key, curr); comp <= 0 {
				if comp == 0 {
					curr.entry = data
					return nil
				}
				// keyScore < next.score
				break
			}
			prev = curr
		}
		// update the position to insert for this level
		prevs[i] = prev
	}

	randLevel := list.randLevel()
	elem := newElement(keyScore, data, randLevel+1)

	for i := randLevel; i >= 0; i-- {
		next := prevs[i].levels[i]
		prevs[i].levels[i] = elem
		elem.levels[i] = next
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!
	list.lock.RLock()
	defer list.lock.RUnlock()
	keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	for i := maxLevel; i >= 0; i-- {
		for curr := prev.levels[i]; curr != nil; curr = prev.levels[i] {
			if comp := list.compare(keyScore, key, curr); comp <= 0 {
				if comp == 0 {
					return curr.entry
				}
				break
			}
			prev = curr
		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	//implement me here!!!
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
	return 0
}

func (list *SkipList) randLevel() int {
	//implement me here!!!
	level := 0
	for ; level < list.maxLevel; level++ {
		if list.rand.Intn(2) == 0 {
			return level
		}
	}
	return level
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}
