package bitcaskds

import (
	"context"
	"errors"

	"git.mills.io/prologic/bitcask"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

type Datastore struct {
	db *bitcask.Bitcask
}

func NewDatastore(path string) (*Datastore, error) {
	db, err := bitcask.Open(path, bitcask.WithMaxKeySize(256), bitcask.WithMaxValueSize(1<<20))
	if err != nil {
		return nil, err
	}

	return &Datastore{
		db,
	}, nil
}

func (d *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	k, err := d.db.Get(key.Bytes())
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			return nil, datastore.ErrNotFound
		}
		return nil, err
	}
	return k, nil
}

func (d *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	return d.db.Has(key.Bytes()), nil
}

func (d *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	b, err := d.Get(ctx, key)
	if err != nil {
		return -1, err
	}
	return len(b), nil
}

var ErrLimit = errors.New("query limit")

func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	results := query.ResultsWithContext(q, func(ctx context.Context, output chan<- query.Result) {
		err := d.db.Scan([]byte(q.Prefix), func(key []byte) error {
			var value []byte

			if !q.KeysOnly || q.ReturnsSizes {
				var err error

				value, err = d.Get(ctx, datastore.RawKey(string(key)))
				if err != nil {
					return err
				}
			}

			select {
			case <-ctx.Done():
			case output <- query.Result{
				Entry: query.Entry{
					Key:   string(key),
					Value: value,
					Size:  len(value),
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
			case <-ctx.Done():
			case output <- query.Result{
				Error: err,
			}:
			}
		}
	})

	return query.NaiveQueryApply(q, results), nil
}

func (d *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	return d.db.Put(key.Bytes(), value)
}

func (d *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	return d.db.Delete(key.Bytes())
}

func (d *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return d.db.Sync()
}

type entry struct {
	key   []byte
	value []byte
	del   bool
}

type batch struct {
	db   *Datastore
	ents []entry
}

func (b *batch) Put(ctx context.Context, key datastore.Key, value []byte) error {
	b.ents = append(b.ents, entry{
		key:   key.Bytes(),
		value: value,
		del:   false,
	})
	return nil
}

func (b *batch) Delete(ctx context.Context, key datastore.Key) error {
	b.ents = append(b.ents, entry{
		key: key.Bytes(),
		del: true,
	})
	return nil
}

func (b *batch) Commit(ctx context.Context) error {
	for _, ent := range b.ents {
		if !ent.del {
			if err := b.db.db.Put(ent.key, ent.value); err != nil {
				return err
			}
		} else {
			if err := b.db.db.Delete(ent.key); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return &batch{
		db: d,
	}, nil
}

func (d *Datastore) Commit(ctx context.Context) error { // Batch
	return nil
}

func (d *Datastore) CollectGarbage(ctx context.Context) error {
	return d.db.Merge()
}

func (d *Datastore) Close() error {
	return d.db.Sync()
}

var _ datastore.Batching = (*Datastore)(nil)
var _ datastore.GCDatastore = (*Datastore)(nil)
