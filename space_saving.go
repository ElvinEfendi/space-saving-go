package space_saving

type SpaceSaving struct {
	streamSummary  *StreamSummary
	maxCountersLen int
}

func New(maxCountersLen int) *SpaceSaving {
	return &SpaceSaving{maxCountersLen: maxCountersLen, streamSummary: NewStreamSummary()}
}

func (ss *SpaceSaving) Process(key string) error {
	if ss.streamSummary.HasKey(key) {
		return ss.streamSummary.Increment(key)
	}
	if ss.streamSummary.Len() < ss.maxCountersLen {
		return ss.streamSummary.Add(key)
	}
	return ss.streamSummary.ReplaceWith(key)
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
