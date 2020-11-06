package db

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// DB -> database type
type DB struct {
	id int64
	kv ethdb.Database
}

// NewDB -> create a new database instance
func NewDB() *DB {
	db := new(DB)
	db.kv = rawdb.NewMemoryDatabase()
	db.id = 0
	return db
}

// Get ->
func (db *DB) Get(Key string) ([]byte, error) {
	got, getErr := db.kv.Get([]byte(Key))
	return got, getErr
}

// Put ->
func (db *DB) Put(Key, Value string) error {

	insertErr := db.kv.Put([]byte(Key), []byte(Value))
	if insertErr != nil {
		fmt.Println("can't Put on open DB:", insertErr)
		return insertErr
	}

	return nil
}

// ToByteArray ->
func (db *DB) ToByteArray() []byte {
	// Iterate over the database
	contents := make(map[string]string)
	it := db.kv.NewIterator([]byte{}, []byte{})

	for it.Next() {
		thisKey := string(it.Key()[:])
		thisVal := string(it.Value()[:])

		contents[thisKey] = thisVal
	}

	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)

	// Encoding the contents map
	err := e.Encode(contents)
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}

// ByteArrayToMap ->
func (db *DB) ByteArrayToMap(contents []byte) map[string]string {
	b := new(bytes.Buffer)
	var decodedMap map[string]string
	d := gob.NewDecoder(b)

	// Decoding the serialized data
	err := d.Decode(&decodedMap)
	if err != nil {
		panic(err)
	}

	return decodedMap
}

// MergeDB ->
func (db *DB) MergeDB(newContents map[string]string) {
	var err error
	for k, v := range newContents {
		err = db.Put(k, v)

		if err != nil {
			panic(err)
		}
	}
}
