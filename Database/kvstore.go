package db

import (
	"fmt"
	"net/http"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// DB -> database type
type DB struct {
	kv ethdb.Database
}

// NewDB -> create a new database instance
func NewDB() *DB {
	db := new(DB)
	db.kv = rawdb.NewMemoryDatabase()

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

// AllPairs ->
func (db *DB) AllPairs(w http.ResponseWriter) {
	// Iterate over the database
	it := db.kv.NewIterator([]byte{}, []byte{})
	for it.Next() {
		thisKey := string(it.Key()[:])
		thisVal := string(it.Value()[:])
		fmt.Println(thisKey, thisVal)

		fmt.Fprintf(w, "    %s -> %s\n", thisKey, thisVal)
	}
}
