package space_saving

import (
	"container/list"
	"fmt"
	log "github.com/Sirupsen/logrus"
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

func (ss *StreamSummary) Add(key string) error {
	log.WithField("key", key).Info("Add")
	return ss.addWithCount(key, 1)
}

func (ss *StreamSummary) Increment(key string) error {
	log.WithField("key", key).Info("Increment")
	counterElement, ok := ss.counters[key]
	if !ok {
		return fmt.Errorf("%s does not exist", key)
	}

	counter := counterElement.Value.(*Counter)
	bucketElement := counter.bucketElement
	bucket := bucketElement.Value.(*Bucket)
	newCount := bucket.value + 1
	if bucketElement = bucketElement.Prev(); bucketElement == nil || bucketElement.Value.(*Bucket).value > newCount {
		if bucket.counterList.Len() == 1 {
			bucket.value = newCount
			counter.bucketElement.Value = bucket
			ss.counters[key] = counterElement
			return nil
		}
		bucket = &Bucket{value: newCount}
		bucket.counterList = list.New()
		bucketElement = ss.bucketList.InsertBefore(bucket, counter.bucketElement)
	}
	ss.removeCounterFromBucket(counterElement)
	bucketElement.Value.(*Bucket).value = newCount
	counter.bucketElement = bucketElement
	ss.counters[key] = bucketElement.Value.(*Bucket).counterList.PushFront(counter)
	return nil
}

func (ss *StreamSummary) ReplaceWith(key string) error {
	bucketElement := ss.bucketList.Back()
	counterElement := bucketElement.Value.(*Bucket).counterList.Front()
	keyToBeReplaced := counterElement.Value.(*Counter).key
	log.WithFields(log.Fields{"from_key": keyToBeReplaced, "to_key": key}).Info("ReplaceWith")
	delete(ss.counters, keyToBeReplaced)
	ss.removeCounterFromBucket(counterElement)
	ss.addWithCount(key, bucketElement.Value.(*Bucket).value)
	return ss.Increment(key)
}

func (ss *StreamSummary) addWithCount(key string, count int) error {
	counter := &Counter{key: key, bucketElement: ss.bucketList.Back()}
	if counter.bucketElement == nil || counter.bucketElement.Value.(*Bucket).value > count {
		bucket := &Bucket{value: count}
		bucket.counterList = list.New()
		counter.bucketElement = ss.bucketList.PushBack(bucket)
	}
	ss.counters[key] = counter.bucketElement.Value.(*Bucket).counterList.PushFront(counter)
	return nil
}

func (ss *StreamSummary) removeCounterFromBucket(counterElement *list.Element) {
	bucketElement := counterElement.Value.(*Counter).bucketElement
	counterList := bucketElement.Value.(*Bucket).counterList
	counterList.Remove(counterElement)
	if counterList.Len() == 0 {
		ss.bucketList.Remove(bucketElement)
	}
}
