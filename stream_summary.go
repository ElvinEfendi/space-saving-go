package space_saving

type Bucket struct {
	Value int
	// this is used to maintain the link from a bucket to its singly linked list of counters
	// when a new counter is added to the bukcet its key becomes LatestCounterKey of the bucket
	LatestCounterKey string
}

type Counter struct {
	Bucket *Bucket
	Next   *Counter
}

type StreamSummary struct {
	buckets  []Bucket
	counters map[string]*Counter
}

func NewStreamSummary() *StreamSummary {
	return &StreamSummary{buckets: make(Bucket), counters: make(map[string]*Counter)}
}

func (ss *StreamSummary) HasKey(key string) bool {
	_, ok := ss.counters[key]
	return ok
}

func (ss *StreamSummary) Increment(key string) error {
	counter, ok := ss.counters[key]
	if !ok {
		return fmt.Errorf("%s does not exist", key)
	}
	count := counter.Bucket.Value + 1
	bucket := counter.Bucket.Prev()
	if bucket == nil {
		new_bucket = &Bucket{Value: count}
		ss.buckets.PushBack(new_bucket)
		counter.Bucket = new_bucket
		new_bucket.LatestCounterKey = key
		return nil
	}
	if bucket.Value == count {
		counter.Bucket = bucket
		counter.Next = ss.counters[bucket.LatestCounterKey]
		bucket.LatestCounterKey = key
		return nil
	}
	if bucket.Value > count {
		new_bucket = &Bucket{Value: count}
		ss.buckets.InsertAfter(new_bucket, bucket)
		counter.Bucket = new_bucket
		new_bucket.LatestCounterKey = key
		return nil
	}
	// bucket.Value < count can not be the case because bucket values are unique and ordered.
	// previous bucket value is always greater
	return nil
}

func (ss *StreamSummary) Len() int {
	return len(ss.counters)
}

func (ss *StreamSummary) Add(key string) error {
	counter := &Counter{}
	bucket := ss.buckets.Front()
	if bucket == nil || bucket.Value > 1 {
		bucket := &Bucket{Value: 1}
		ss.buckets.PushFront(bucket)
	}
	bucket.LatestCounter = counter
	counter.Bucket = bucket
	ss.counters[key] = counter
	return nil
}

// Delete LatestCounterKey from the Bucket with minimal value
// and add a new counter with value + 1
func (ss *StreamSummary) ReplaceWith(key string) error {
	bucket := ss.buckets.Front()
	counter := ss.counters[bucket.LatestCounterKey]
	// We need to have single links between counters just for this!
	// i.e to detect if there's more counter left in a bucket in constant time
	if counter.Next == nil {
		// this bucket does not hold any counter anymore
		ss.buckets.Remove(bucket)
	}
	delete(ss.counters, bucket.LatestCounterKey)

	counter := &Counter{}
	if bucket.Prev() && bucket.Prev().Value == bucket.Value+1 {
		counter.Bucket = bucket.Prev()
		counter.Next = ss.counters[bucket.Prev().LatestCounterKey]
		bucket.Prev().LatestCounterKey = key
	}
	// create a new bucket as a bucket with Value+1 does not exist
	new_bucket := &Bucket{Value: bucket.Value + 1, LatestCounterKey: key}
	ss.buckets.InsertBefore(new_bucket, bucket)
	counter.Bucket = new_bucket
	ss.counters[key] = counter
	return nil
}
