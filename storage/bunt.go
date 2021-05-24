package storage

import (
	"context"

	"github.com/tidwall/buntdb"
)

type BuntWrapper struct {
	db *buntdb.DB
}

func WrapBuntDB(db *buntdb.DB) *BuntWrapper {
	return &BuntWrapper{db}
}

func (b BuntWrapper) Get(ctx context.Context, key string) (val string, err error) {
	err = b.db.View(func(tx *buntdb.Tx) error {
		val, err = tx.Get(key)
		if err != nil {
			return err
		}

		return nil
	})

	return
}

func (b BuntWrapper) Set(ctx context.Context, key, val string, opts *SetOpts) error {
	err := b.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, val, nil)
		return err
	})

	return err
}

func (b BuntWrapper) Delete(ctx context.Context, key string) (val string, err error) {
	err = b.db.Update(func(tx *buntdb.Tx) error {
		val, err = tx.Delete(key)
		return err
	})

	return
}

var _ Storage = (*BuntWrapper)(nil)
