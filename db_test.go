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

func randomTree(depth uint) Node {
	if depth == 0 {
		return randomRoot()
	}
	v := rand.Uint32()
	var left Node
	if v&0b11 == 0 {
		left = randomRoot()
	} else {
		left = randomTree(depth - 1)
	}
	var right Node
	if v&0b1100 == 0 {
		right = randomRoot()
	} else {
		right = randomTree(depth - 1)
	}
	return NewPairNode(left, right)
}

func randomNode(n Node, gi Gindex, depth uint) (Node, Gindex) {
	if depth == 0 || n.IsLeaf() {
		return n, gi
	}
	v := rand.Uint32()
	if v%3 == 0 {
		left, err := n.Left()
		if err != nil {
			panic(err)
		}
		return randomNode(left, gi.Left(), depth-1)
	} else if v%3 == 1 {
		right, err := n.Right()
		if err != nil {
			panic(err)
		}
		return randomNode(right, gi.Right(), depth-1)
	} else {
		return n, gi
	}
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
	res, err := db.Get(mustHex(toHex(testPrefix[:])+"0100"+"80"+fooHex), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := toHex(res)
	expected := "00" + slotHex(slot)
	if got != expected {
		t.Fatalf("got: %s, expected: %s", got, expected)
	}
}

func TestMerkleDB_PutNested(t *testing.T) {
	db := newMemoryDB()
	mdb := New(testPrefix, db)
	foo := randomTree(17)
	hFn := GetHashFn()
	_ = foo.MerkleRoot(hFn)

	slot := randomSlot()
	err := mdb.Put(slot, foo, hFn)
	if err != nil {
		t.Fatal(err)
	}

	var traverse func(n Node, gi Gindex, slot uint64)
	traverse = func(n Node, gi Gindex, slot uint64) {
		nodeRoot := n.MerkleRoot(hFn)

		giBytes, giBitLen := gi.LeftAlignedBigEndian()
		var giBitLenBytes [2]byte
		binary.LittleEndian.PutUint16(giBitLenBytes[:], uint16(giBitLen))

		key := toHex(testPrefix[:]) + toHex(giBitLenBytes[:]) + toHex(giBytes) + toHex(nodeRoot[:])
		res, err := db.Get(mustHex(key), nil)
		if err != nil {
			t.Fatalf("failed to get node %x (%d bits): %v", giBytes, giBitLen, err)
		}
		got := toHex(res)
		if n.IsLeaf() {
			expected := "00" + slotHex(slot)
			if got != expected {
				t.Fatalf("got: %s, expected: %s", got, expected)
			}
		} else {
			left, err := n.Left()
			if err != nil {
				t.Fatal(err)
			}
			right, err := n.Right()
			if err != nil {
				t.Fatal(err)
			}
			leftRoot := left.MerkleRoot(hFn)
			rightRoot := right.MerkleRoot(hFn)
			expected := "01" + slotHex(slot) + toHex(leftRoot[:]) + toHex(rightRoot[:])
			if got != expected {
				t.Fatalf("got: %s, expected: %s", got, expected)
			}
			traverse(left, gi.Left(), slot)
			traverse(right, gi.Right(), slot)
		}
	}
	traverse(foo, RootGindex, slot)
}

func compareNodes(a Node, b Node, gi Gindex, hFn HashFn, t *testing.T) {
	aRoot := a.MerkleRoot(hFn)
	bRoot := b.MerkleRoot(hFn)
	if aRoot != bRoot {
		t.Errorf("different nodes different at %v: %s <> %s", gi, aRoot, bRoot)
	}
	if aL, bL := a.IsLeaf(), b.IsLeaf(); aL != bL {
		t.Errorf("a and b not same type: a leaf: %v, b leaf: %v", aL, bL)
	} else if !aL {
		{
			aN, aErr := a.Left()
			if aErr != nil {
				t.Errorf("failed to retrieve a left: %v", aErr)
			}
			bN, bErr := b.Left()
			if bErr != nil {
				t.Errorf("failed to retrieve a left: %v", bErr)
			}
			if aErr != nil || bErr != nil {
				return
			}
			compareNodes(aN, bN, gi.Left(), hFn, t)
		}
		{
			aN, aErr := a.Right()
			if aErr != nil {
				t.Errorf("failed to retrieve a right at %v: %v", gi, aErr)
			}
			bN, bErr := b.Right()
			if bErr != nil {
				t.Errorf("failed to retrieve a right at %v: %v", gi, bErr)
			}
			if aErr != nil || bErr != nil {
				return
			}
			compareNodes(aN, bN, gi.Right(), hFn, t)
		}
	}
}

func TestVirtualNode(t *testing.T) {
	db := newMemoryDB()
	mdb := New(testPrefix, db)
	foo := randomTree(17)
	hFn := GetHashFn()
	_ = foo.MerkleRoot(hFn)

	slot := randomSlot()
	err := mdb.Put(slot, foo, hFn)
	if err != nil {
		t.Fatal(err)
	}

	n, gi := randomNode(foo, RootGindex, 6)
	root := n.MerkleRoot(hFn)
	out, err := mdb.Get(gi, root)
	if err != nil {
		t.Fatal(err)
	}
	if out.Slot != slot {
		t.Fatalf("different slot: %d <> %d", out.Slot, slot)
	}
	compareNodes(n, out.Node, gi, hFn, t)
}
