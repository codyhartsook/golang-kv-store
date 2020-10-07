package DB

import (
	"github.com/ethereum/go-ethereum/ethdb"
    "github.com/ethereum/go-ethereum/core/rawdb"
    "fmt"
    "net/http"
)

type DB struct {
	kv ethdb.Database
}

func NewDB() *DB {
	db := new(DB)

    var kv ethdb.Database
    kv = rawdb.NewMemoryDatabase()

    db.kv = kv

    return db
}

func (db *DB) Get(Key string) ([]byte, error) {
	got, getErr := db.kv.Get([]byte(Key))
	return got, getErr
}

func (db *DB) Put(Key, Value string) {

    db.kv.Put([]byte(Key), []byte(Value))
    /*if insertErr != nil {
        fmt.Println("can't Put on open DB:", insertErr)
        return insertErr
    }*/
}

/*func (db *DB) Delete() {
	
}*/

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