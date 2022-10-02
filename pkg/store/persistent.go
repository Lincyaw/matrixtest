package store

import (
	"fmt"
	"github.com/cockroachdb/pebble"
)

func NewPebbleDB(dir string) (*PebbleDB, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	p := &PebbleDB{
		Dir: dir,
		DB:  db,
	}
	return p, nil
}

type PebbleDB struct {
	Dir string
	DB  *pebble.DB
}

func (p *PebbleDB) Set(k, v []byte) error {
	fmt.Printf("pebbledb set key[%v] value[%v]\n", string(k), string(v))
	return p.DB.Set(k, v, pebble.Sync)
}

func (p *PebbleDB) Get(k []byte) ([]byte, error) {
	v, closer, err := p.DB.Get(k)
	var value []byte
	copy(value, v)
	err = closer.Close()
	if err != nil {
		return nil, err
	}
	fmt.Printf("pebbledb get key[%v] value[%v]\n", string(k), string(value))
	return value, nil
}
func (p *PebbleDB) Delete(k []byte) error {
	fmt.Printf("pebbledb delete key[%v]\n", string(k))
	return p.DB.Delete(k, pebble.Sync)
}
