package merkledb

import (
	"encoding/binary"
	"encoding/hex"
	. "github.com/protolambda/ztyp/tree"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"math/rand"
	"testing"
)

func newMemoryDB() *leveldb.DB {
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		panic(err)
	}
	return db
}

func randomRoot() *Root {
	var res Root
	rand.Read(res[:])
	return &res
}

func randomSlot() uint64 {
	return rand.Uint64()
}

func mustHex(v string) []byte {
	b, err := hex.DecodeString(v)
	if err != nil {
		panic(err)
	}
	return b
}

func toHex(v []byte) string {
	return hex.EncodeToString(v)
}

func slotHex(v uint64) string {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return toHex(b[:])
}

var testPrefix = [3]byte{0x42, 0x30, 0x78}

func TestMerkleDB_Put(t *testing.T) {
	db := newMemoryDB()
	mdb := New(testPrefix, db)
	foo := randomRoot()
	fooHex := toHex(foo[:])
	slot := randomSlot()
	err := mdb.Put(slot, foo, Hash)
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Get(mustHex("423078"+"0100"+"01"+fooHex), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := toHex(res)
	expected := "00" + slotHex(slot)
	if got != expected {
		t.Fatalf("got: %s, expected: %s", got, expected)
	}
}
