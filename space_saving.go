package space_saving

type SpaceSaving struct {
	streamSummary  *StreamSummary
	maxCountersLen int
}

func NewSpaceSaving(maxCountersLen int) *SpaceSaving {
	return &SpaceSaving{maxCountersLen: maxCountersLen, streamSummary: &StreamSummary{}}
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
