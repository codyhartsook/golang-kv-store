package db

import (
	"encoding/json"
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

	if Key == "" {
		return fmt.Errorf("Key can not be empty")
	}

	insertErr := db.kv.Put([]byte(Key), []byte(Value))
	if insertErr != nil {
		return insertErr
	}

	return nil
}

// ToByteArray ->
func (db *DB) ToByteArray() ([]byte, error) {
	// Iterate over the database
	contents := make(map[string]string)
	it := db.kv.NewIterator([]byte{}, []byte{})

	for it.Next() {
		thisKey := string(it.Key()[:])
		thisVal := string(it.Value()[:])

		contents[thisKey] = thisVal
	}

	return json.Marshal(contents)
}

// ByteArrayToMap ->
func (db *DB) ByteArrayToMap(contents []byte) (map[string]string, error) {
	m := make(map[string]string)
	err := json.Unmarshal(contents, &m)

	return m, err
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

// PrintDB ->
func (db *DB) PrintDB() {
	it := db.kv.NewIterator([]byte{}, []byte{})

	for it.Next() {
		thisKey := string(it.Key()[:])
		thisVal := string(it.Value()[:])

		fmt.Println(thisKey, thisVal)
	}
}
