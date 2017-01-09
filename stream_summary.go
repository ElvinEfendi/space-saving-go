package space_saving

import "container/list"

type Bucket struct {
	value    int
	counters *list.List
}

type Counter struct {
	bucket *Bucket
}

type StreamSummary struct {
	buckets *List
	keys    map[string]*Counter
}

func NewStreamSummary() *StreamSummary {
	return &StreamSummary{buckets: make(Bucket), keys: make(map[string]*Counter)}
}

func (ss *StreamSummary) HasKey(key string) bool {
	_, ok := ss.keys[key]
	return ok
}

func (ss *StreamSummary) Len() int {
	return len(ss.keys)
}

func (ss *StreamSummary) Add(key string) {
	ss.addWithCount(key, 1)
}

func (ss *StreamSummary) Increment(key string) {
	counter, ok := ss.keys[key]
	if !ok {
		return fmt.Errorf("%s does not exist", key)
	}

	count := counter.bucket.Value + 1
	var bucket *Bucket
	if bucket = counter.bucket.Prev(); bucket == nil || bucket.value > count {
		bucket = &Bucket{value: count}
		ss.buckets.InsertBefore(bucket, counter.bucket)
	}
	ss.removeCounterFromBucket(bucket, counter)
	counter.bucket = bucket
	bucket.counters.PushFront(counter)
}

func (ss *StreamSummary) ReplaceWith(key string) {
	bucket := ss.buckets.Back()
	counter := bucket.counters.Front()
	ss.removeCounterFromBucket(bucket, counter)
	ss.addWithCount(key, bucket.Value+1)
}

func (ss *StreamSummary) addWithCount(key string, count int) {
	counter := &Counter{}
	bucket := ss.buckets.Back()
	if bucket == nil || bucket.Value > count {
		bucket := &Bucket{Value: count}
		ss.buckets.PushBack(bucket)
	}
	bucket.counters.PushFront(counter)
	counter.bucket = bucket
	ss.keys[key] = counter
}

func (ss *StreamSummary) removeCounterFromBucket(counter *Counter, bucket *Bucket) {
	counter.bucket.counters.Remove(counter)
	if counter.bucket.counters.Len() == 0 {
		// TODO it would give some perf benefits to not remove a bucket with len = 1, as this going to be created often(always?)
		ss.buckets.Remove(counter.bucket)
	}
}
