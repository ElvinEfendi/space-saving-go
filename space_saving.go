package space_saving

import (
	log "github.com/Sirupsen/logrus"
)

type SpaceSaving struct {
	streamSummary  *StreamSummary
	maxCountersLen int
}

func init() {
	log.SetLevel(log.DebugLevel)
}

func New(maxCountersLen int) *SpaceSaving {
	log.WithField("max_counters_len", maxCountersLen).Debug("Initialized a new SpaceSaving instance")
	return &SpaceSaving{maxCountersLen: maxCountersLen, streamSummary: NewStreamSummary()}
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

	/*
		log.WithFields(log.Fields{"number_of_buckets": ss.streamSummary.bucketList.Len(),
			"number_of_counters": ss.getNumberOfCounters(),
			"front_key":          ss.streamSummary.bucketList.Front().Value.(*Bucket).counterList.Back().Value.(*Counter).key,
			"front_value":        ss.streamSummary.bucketList.Front().Value.(*Bucket).value,
			"back_key":           ss.streamSummary.bucketList.Back().Value.(*Bucket).counterList.Front().Value.(*Counter).key,
			"back_value":         ss.streamSummary.bucketList.Back().Value.(*Bucket).value}).
			Debug("Stats")
	*/
	return err
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

func (ss *SpaceSaving) getNumberOfCounters() int {
	count := 0
	for bucketElement := ss.streamSummary.bucketList.Front(); bucketElement != nil; bucketElement = bucketElement.Next() {
		bucket := bucketElement.Value.(*Bucket)
		count += bucket.counterList.Len()
	}
	return count
}
