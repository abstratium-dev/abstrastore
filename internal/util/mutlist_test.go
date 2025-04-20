package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMutList_Add(t *testing.T) {
	assert := assert.New(t)
	list := NewMutList[int]()
	list.Add(1)
	list.Add(2)
	list.Add(3)
	assert.Equal(3, list.Len())
	assert.Equal([]int{1, 2, 3}, list.Items())
}

func TestMutList_Len(t *testing.T) {
	assert := assert.New(t)
	list := NewMutList[int]()
	assert.Equal(0, list.Len())
	list.Add(1)
	assert.Equal(1, list.Len())
	list.Add(2)
	assert.Equal(2, list.Len())
}

func TestMutList_Items(t *testing.T) {
	assert := assert.New(t)
	list := NewMutList[int]()
	list.Add(1)
	list.Add(2)
	list.Add(3)
	items := list.Items()
	assert.Equal([]int{1, 2, 3}, items)
	// Modify the returned slice and ensure the original list is not modified
	items[0] = 99
	assert.Equal(1, list.Items()[0])
}
