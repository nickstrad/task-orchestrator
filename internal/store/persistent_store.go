package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

type PersistentStore[T any] struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func NewPersistentStore[T any](dbFile string, fileMode os.FileMode, bucket string, freshStart bool) (*PersistentStore[T], error) {
	op := "store.PersistentStore.NewPersistentStore"
	if dbFile == "" {
		return nil, E(op, "must pass in 'dbFile'", nil)
	}

	if bucket == "" {
		return nil, E(op, "must pass in 'bucket'", nil)
	}

	db, err := bolt.Open(dbFile, fileMode, nil)
	if err != nil {
		return nil, E(op, fmt.Sprintf("unable to open bbolt db file %s", dbFile), err)
	}

	p := &PersistentStore[T]{
		Db:       db,
		DbFile:   dbFile,
		FileMode: fileMode,
		Bucket:   bucket,
	}

	if err := p.CreateBucket(freshStart); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *PersistentStore[T]) CreateBucket(freshStart bool) error {
	op := "store.PersistentStore.CreateBucket"

	err := p.Db.Update(func(tx *bolt.Tx) error {
		if freshStart {
			e := tx.DeleteBucket([]byte(p.Bucket))
			if e != nil && !errors.Is(e, bolterrors.ErrBucketNotFound) {
				return E(op, fmt.Sprintf("failed to delete bucket '%s'", p.Bucket), e)
			}
		}

		_, e := tx.CreateBucketIfNotExists([]byte(p.Bucket))
		if e != nil {
			return E(op, fmt.Sprintf("unable to create bucket '%s'", p.Bucket), e)
		}
		return nil
	})

	return err
}

func (p *PersistentStore[T]) Close() error {
	op := "store.PersistentStore.Close"

	err := p.Db.Close()
	if err != nil {
		return E(op, "unable to close db", err)
	}

	return nil
}

func (p *PersistentStore[T]) Put(key string, value T) error {
	op := "store.PersistentStore.Put"

	err := p.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(p.Bucket))

		buf, e := json.Marshal(value)
		if e != nil {
			return E(op, fmt.Sprintf("unable to marshal value for key '%s'", key), e)
		}

		e = b.Put([]byte(key), buf)
		if e != nil {
			return E(op, fmt.Sprintf("unable to put value for key '%s'", key), e)
		}
		return nil
	})

	return err
}

func (p *PersistentStore[T]) Get(key string) (T, error) {
	op := "store.PersistentStore.Get"

	var t T
	err := p.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(p.Bucket))

		v := b.Get([]byte(key))
		if v == nil {
			return E(op, fmt.Sprintf("unable to find value for key '%s'", key), ErrNotFound)
		}
		err := json.Unmarshal(v, &t)
		if err != nil {
			return E(op, fmt.Sprintf("unable to unmarshal value for key '%s'", key), err)
		}
		return nil
	})

	return t, err
}

func (p *PersistentStore[T]) List() ([]T, error) {
	op := "store.PersistentStore.List"

	var items []T
	err := p.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(p.Bucket))

		items = make([]T, 0, b.Stats().KeyN)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var t T
			e := json.Unmarshal(v, &t)
			if e != nil {
				return E(op, fmt.Sprintf("unable to unmarshal value for key '%s'", k), e)
			}
			items = append(items, t)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return items, nil
}

func (p *PersistentStore[T]) Count() (int, error) {
	op := "store.PersistentStore.Count"

	cnt := 0
	err := p.Db.View(func(tx *bolt.Tx) error {
		cnt = tx.Bucket([]byte(p.Bucket)).Stats().KeyN
		return nil
	})

	if err != nil {
		return 0, E(op, "unable to count keys in db", err)
	}

	return cnt, nil
}
