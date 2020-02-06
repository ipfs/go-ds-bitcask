package bitcaskds

import (
	"errors"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/jbenet/goprocess"
	"github.com/prologic/bitcask"
)

type Datastore struct {
	db *bitcask.Bitcask
}

func NewDatastore(path string) (*Datastore, error) {
	db, err := bitcask.Open(path, bitcask.WithMaxKeySize(128), bitcask.WithMaxValueSize(1<<20))
	if err != nil {
		return nil, err
	}

	return &Datastore{
		db,
	}, nil
}

func (d *Datastore) Get(key datastore.Key) (value []byte, err error) {
	k, err := d.db.Get(key.Bytes())
	switch err {
	case nil:
		return k, nil
	case bitcask.ErrKeyNotFound:
		return nil, datastore.ErrNotFound
	default:
		return nil, err
	}
}

func (d *Datastore) Has(key datastore.Key) (exists bool, err error) {
	return d.db.Has(key.Bytes()), nil
}

func (d *Datastore) GetSize(key datastore.Key) (size int, err error) {
	b, err := d.Get(key)
	if err != nil {
		return -1, err
	}
	return len(b), nil
}

var ErrLimit = errors.New("query limit")

func (d *Datastore) Query(q query.Query) (query.Results, error) {
	qrb := query.NewResultBuilder(q)

	qrb.Process.Go(func(proc goprocess.Process) {
		err := d.db.Scan([]byte(q.Prefix), func(key []byte) error {
			var value []byte

			if !q.KeysOnly || q.ReturnsSizes {
				var err error

				value, err = d.Get(datastore.RawKey(string(key)))
				if err != nil {
					return err
				}
			}

			select {
			case <-proc.Closing():
			case qrb.Output <- query.Result{
				Entry: query.Entry{
					Key:        string(key),
					Value:      value,
					Size:       len(value),
				},
			}:
			}

			return nil
		})

		if err != nil {
			if err == ErrLimit {
				return
			}
			select {
			case <-proc.Closing():
			case qrb.Output <- query.Result{
				Error: err,
			}:
			}
		}
	})

	go qrb.Process.CloseAfterChildren()

	return query.NaiveQueryApply(q, qrb.Results()), nil
}

func (d *Datastore) Put(key datastore.Key, value []byte) error {
	return d.db.Put(key.Bytes(), value)
}

func (d *Datastore) Delete(key datastore.Key) error {
	return d.db.Delete(key.Bytes())
}

func (d *Datastore) Sync(prefix datastore.Key) error {
	return d.db.Sync()
}

func (d *Datastore) Close() error {
	return d.db.Sync()
}
