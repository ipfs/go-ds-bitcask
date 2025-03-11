package bitcaskds

import (
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
)

func newDS(t *testing.T) *Datastore {
	tmpPath := t.TempDir()

	d, err := NewDatastore(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestSuite(t *testing.T) {
	d := newDS(t)
	defer d.Close()

	dstest.SubtestAll(t, d)
}
