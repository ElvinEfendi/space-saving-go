package space_saving

import (
	"container/list"
	"fmt"
)

type Bucket struct {
	value       int
	counterList *list.List
}

type Counter struct {
	bucketElement *list.Element
	key           string
}

type StreamSummary struct {
	bucketList *list.List
	counters   map[string]*list.Element
}

func NewStreamSummary() *StreamSummary {
	return &StreamSummary{bucketList: list.New(), counters: make(map[string]*list.Element)}
}

func (ss *StreamSummary) HasKey(key string) bool {
	_, ok := ss.counters[key]
	return ok
}

func (ss *StreamSummary) Len() int {
	return len(ss.counters)
}

func (ss *StreamSummary) Add(key string) {
	ss.addWithCount(key, 1)
}

func (ss *StreamSummary) Increment(key string) error {
	counterElement, ok := ss.counters[key]
	if !ok {
		return fmt.Errorf("%s does not exist", key)
	}

	counter := counterElement.Value.(*Counter)
	bucketElement := counter.bucketElement
	count := bucketElement.Value.(*Bucket).value + 1
	if bucketElement = bucketElement.Prev(); bucketElement == nil || bucketElement.Value.(*Bucket).value > count {
		if bucket := counter.bucketElement.Value.(*Bucket); bucket.counterList.Len() == 1 {
			bucket.value++
			return nil
		}
		bucket := &Bucket{value: count}
		bucket.counterList = list.New()
		bucketElement = ss.bucketList.InsertBefore(bucket, counterElement.Value.(*Counter).bucketElement)
	}
	ss.removeCounterFromBucket(counterElement)
	counterElement.Value.(*Counter).bucketElement = bucketElement
	bucketElement.Value.(*Bucket).counterList.PushFront(counterElement.Value.(*Counter))
	return nil
}

func (ss *StreamSummary) ReplaceWith(key string) {
	bucketElement := ss.bucketList.Back()
	counterElement := bucketElement.Value.(*Bucket).counterList.Front()
	delete(ss.counters, counterElement.Value.(*Counter).key)
	ss.removeCounterFromBucket(counterElement)
	ss.addWithCount(key, bucketElement.Value.(*Bucket).value+1)
}

func (ss *StreamSummary) addWithCount(key string, count int) {
	counter := &Counter{key: key}
	bucketElement := ss.bucketList.Back()
	if bucketElement == nil || bucketElement.Value.(*Bucket).value > count {
		bucket := &Bucket{value: count}
		bucket.counterList = list.New()
		bucketElement = ss.bucketList.PushBack(bucket)
	}
	ss.counters[key] = bucketElement.Value.(*Bucket).counterList.PushFront(counter)
	counter.bucketElement = bucketElement
}

func (ss *StreamSummary) removeCounterFromBucket(counterElement *list.Element) {
	bucketElement := counterElement.Value.(*Counter).bucketElement
	counterList := bucketElement.Value.(*Bucket).counterList
	counterList.Remove(counterElement)
	// no need to delete bucket with value 1 as it is going to be created anyway
	if counterList.Len() == 0 {
		ss.bucketList.Remove(bucketElement)
	}
}
