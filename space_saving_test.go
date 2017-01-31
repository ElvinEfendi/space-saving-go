package space_saving

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTopWithEnoughCounters(t *testing.T) {
	ss := New(3)
	keys := []string{"a", "b", "a", "a", "a", "c", "c", "c", "a"}
	for _, k := range keys {
		err := ss.Process(k)
		require.NoError(t, err)
	}
	topKeys, topCounts := ss.Top(2)
	require.Equal(t, []string{"a", "c"}, topKeys)
	require.Equal(t, []int{5, 3}, topCounts)
}

func TestTopWhenTopElementChanges(t *testing.T) {
	ss := New(3)
	keys := []string{"a", "b", "a", "a", "a", "c", "c", "c", "a", "b", "b", "b", "b", "b"}
	for _, k := range keys {
		err := ss.Process(k)
		require.NoError(t, err)
	}
	topKeys, topCounts := ss.Top(2)
	require.Equal(t, []string{"b", "a"}, topKeys)
	require.Equal(t, []int{6, 5}, topCounts)
}

func TestTopWithoutEnoughCounters(t *testing.T) {
	ss := New(2)
	keys := []string{"a", "b", "a", "a", "a", "c", "c", "c", "d", "d"}
	for _, k := range keys {
		err := ss.Process(k)
		require.NoError(t, err)
	}
	topKeys, topCounts := ss.Top(2)
	require.Equal(t, []string{"d", "a"}, topKeys)
	require.Equal(t, []int{6, 4}, topCounts)
}
