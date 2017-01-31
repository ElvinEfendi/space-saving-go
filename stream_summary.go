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

func (ss *StreamSummary) Add(key string) error {
	fmt.Printf("Adding %s\n", key)
	return ss.addWithCount(key, 1)
}

func (ss *StreamSummary) Increment(key string) error {
	fmt.Printf("Incrementing %s\n", key)
	counterElement, ok := ss.counters[key]
	if !ok {
		return fmt.Errorf("%s does not exist", key)
	}

	counter := counterElement.Value.(*Counter)
	bucketElement := counter.bucketElement
	bucket := bucketElement.Value.(*Bucket)
	fmt.Printf("  count for %s is %d counterList.size = %d\n", key, bucket.value, bucket.counterList.Len())
	newCount := bucket.value + 1
	if bucketElement = bucketElement.Prev(); bucketElement == nil || bucketElement.Value.(*Bucket).value > newCount {
		if bucket.counterList.Len() == 1 {
			bucket.value = newCount
			counter.bucketElement.Value = bucket
			ss.counters[key] = counterElement
			fmt.Printf("  [bucket.counterList.Len() == 1] new count for %s is %d, counterList.size = %d\n", key, bucket.value, bucket.counterList.Len())
			return nil
		}
		bucket = &Bucket{value: newCount}
		bucket.counterList = list.New()
		fmt.Printf("  Creating a bucket %s, %d\n", key, newCount)
		bucketElement = ss.bucketList.InsertBefore(bucket, counter.bucketElement)
	}
	ss.removeCounterFromBucket(counterElement)
	fmt.Printf("  PrevBucketValue = %d, newCount = %d\n", bucketElement.Value.(*Bucket).value, newCount)
	bucketElement.Value.(*Bucket).value = newCount
	counter.bucketElement = bucketElement
	ss.counters[key] = bucketElement.Value.(*Bucket).counterList.PushFront(counter)
	fmt.Printf("  new count for %s is %d, counterList.size = %d, ss.counters.counterList.size = %d\n",
		key, bucketElement.Value.(*Bucket).value, bucket.counterList.Len(), ss.counters[key].Value.(*Counter).bucketElement.Value.(*Bucket).counterList.Len())
	return nil
}

func (ss *StreamSummary) ReplaceWith(key string) error {
	bucketElement := ss.bucketList.Back()
	counterElement := bucketElement.Value.(*Bucket).counterList.Front()
	keyToBeReplaced := counterElement.Value.(*Counter).key
	fmt.Printf("Replacing %s with %s\n", keyToBeReplaced, key)
	delete(ss.counters, keyToBeReplaced)
	ss.removeCounterFromBucket(counterElement)
	ss.addWithCount(key, bucketElement.Value.(*Bucket).value)
	return ss.Increment(key)
}

func (ss *StreamSummary) addWithCount(key string, count int) error {
	counter := &Counter{key: key, bucketElement: ss.bucketList.Back()}
	fmt.Printf("  addWithCount %s, %d\n", key, count)
	if counter.bucketElement == nil || counter.bucketElement.Value.(*Bucket).value > count {
		fmt.Printf("  Creating a bucket %s, %d\n", key, count)
		bucket := &Bucket{value: count}
		bucket.counterList = list.New()
		counter.bucketElement = ss.bucketList.PushBack(bucket)
	}
	ss.counters[key] = counter.bucketElement.Value.(*Bucket).counterList.PushFront(counter)
	return nil
}

func (ss *StreamSummary) removeCounterFromBucket(counterElement *list.Element) {
	bucketElement := counterElement.Value.(*Counter).bucketElement
	fmt.Printf("  removing counter element with bucket value %d\n", bucketElement.Value.(*Bucket).value)
	counterList := bucketElement.Value.(*Bucket).counterList
	counterList.Remove(counterElement)
	if counterList.Len() == 0 {
		fmt.Printf("  removing bucketElement\n")
		ss.bucketList.Remove(bucketElement)
	}
}
