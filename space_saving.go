package space_saving

import (
	"log"
)

type SpaceSaving struct {
	streamSummary  *StreamSummary
	maxCountersLen int
}

func New(maxCountersLen int) *SpaceSaving {
	ss := &SpaceSaving{
		maxCountersLen: maxCountersLen,
		streamSummary:  NewStreamSummary(),
	}
	return ss
}

func (ss *SpaceSaving) Process(key string) error {
	var err error
	if ss.streamSummary.HasKey(key) {
		err = ss.streamSummary.Increment(key)
	} else if ss.streamSummary.Len() < ss.maxCountersLen {
		err = ss.streamSummary.Add(key)
	} else {
		err = ss.streamSummary.ReplaceWith(key)
	}

	return err
}

func (ss *SpaceSaving) Summary() {
	log.Printf("number_of_buckets: %d\n"+
		"number_of_counters: %d\n"+
		"front_key: %s\n"+
		"front_value: %d\n"+
		"back_key: %s\n"+
		"back_value: %d\n",
		ss.streamSummary.bucketList.Len(),
		ss.streamSummary.Len(),
		ss.streamSummary.bucketList.Front().Value.(*Bucket).counterList.Back().Value.(*Counter).key,
		ss.streamSummary.bucketList.Front().Value.(*Bucket).value,
		ss.streamSummary.bucketList.Back().Value.(*Bucket).counterList.Front().Value.(*Counter).key,
		ss.streamSummary.bucketList.Back().Value.(*Bucket).value)
}

func (ss *SpaceSaving) Top(k int) ([]string, []int) {
	i := 0
	keys := make([]string, k)
	counts := make([]int, k)
	for bucketElement := ss.streamSummary.bucketList.Front(); bucketElement != nil && i < k; bucketElement = bucketElement.Next() {
		bucket := bucketElement.Value.(*Bucket)
		for counterElement := bucket.counterList.Front(); counterElement != nil && i < k; counterElement = counterElement.Next() {
			keys[i] = counterElement.Value.(*Counter).key
			counts[i] = bucket.value
			i++
		}
	}
	return keys, counts
}
