package bitcaskds

import (
	"os"
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
)

func newDS(t *testing.T) (*Datastore, func()) {
	path, err := os.MkdirTemp(os.TempDir(), "testing_bitcask_")
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path)
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		d.Close()
		os.RemoveAll(path)
	}
}

func TestSuite(t *testing.T) {
	d, done := newDS(t)
	defer done()

	dstest.SubtestAll(t, d)
}
