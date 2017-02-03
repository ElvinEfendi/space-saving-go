package space_saving

import (
	log "github.com/Sirupsen/logrus"
)

type SpaceSaving struct {
	streamSummary  *StreamSummary
	maxCountersLen int
}

func New(maxCountersLen int, verbose bool) *SpaceSaving {
	if verbose {
		log.SetLevel(log.DebugLevel)
	}
	log.WithFields(log.Fields{"max_counters_len": maxCountersLen, "verbose": verbose}).
		Info("Initialized a new SpaceSaving instance")

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

	log.WithFields(log.Fields{
		"number_of_buckets":  ss.streamSummary.bucketList.Len(),
		"number_of_counters": ss.streamSummary.Len(),
		"front_key": ss.streamSummary.bucketList.Front().Value.(*Bucket).
			counterList.Back().Value.(*Counter).key,
		"front_value": ss.streamSummary.bucketList.Front().Value.(*Bucket).value,
		"back_key": ss.streamSummary.bucketList.Back().Value.(*Bucket).
			counterList.Front().Value.(*Counter).key,
		"back_value": ss.streamSummary.bucketList.Back().Value.(*Bucket).value}).Debug("Stats")

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
