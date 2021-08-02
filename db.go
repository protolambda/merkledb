package merkledb

import (
	"encoding/binary"
	"errors"
	"fmt"
	. "github.com/protolambda/ztyp/tree"
	"github.com/syndtr/goleveldb/leveldb"
)

type SlottedNode struct {
	Slot uint64
	Node Node
}

type MerkleDB interface {
	// Put a node and its subtree in the DB
	Put(slot uint64, node Node, fn HashFn) error
	// Get a node from the DB
	Get(gindex Gindex, key Root) (SlottedNode, error)
	// Has the node or not
	Has(gindex Gindex, key Root) (bool, error)
	// Delete the node at (gindex, key), does not remove any subtree
	Delete(gindex Gindex, key Root) error
	// Range retrieval of slotted values from the DB, between startSlot and endSlot, at the given gindex.
	// There may be multiple nodes per slot.
	Range(startSlot uint64, endSlot uint64, gindex Gindex) ([]SlottedNode, error)
}

// DB format
//
// All ints, incl gindex, are little-endian
//
// Root node:
// bytes(prefix) ++ uint16(gindex_bitlen) ++ bytes(gindex_leftbitaligned) ++ bytes32(self) -> uint8(0) ++ uint64(slot)
//
// Pair node:
// bytes(prefix) ++ uint16(gindex_bitlen) ++ bytes(gindex_leftbitaligned) ++ bytes32(self) -> uint8(1) ++ uint64(slot) ++ bytes32(left) ++ bytes32(right)

const prefixLen = 3
const gindexLenByteLen = 2
const maxGindexByteLen = 32

type merkleDB struct {
	prefix [prefixLen]byte
	db     *leveldb.DB
}

// Wrap the database with a binary-tree merkle interface.
func New(prefix [prefixLen]byte, db *leveldb.DB) MerkleDB {
	return &merkleDB{prefix, db}
}

func (db *merkleDB) Put(slot uint64, node Node, fn HashFn) error {
	// if we are just putting a single node, then we don't need the batch
	if node.IsLeaf() {
		var key [prefixLen + gindexLenByteLen + 1 + 32]byte
		// prefix
		copy(key[0:prefixLen], db.prefix[:])
		// gindex length in bytes
		key[prefixLen] = 1
		key[prefixLen+1] = 0
		// gindex
		key[prefixLen+gindexLenByteLen] = 1 << 7
		root := node.MerkleRoot(fn)
		copy(key[prefixLen+gindexLenByteLen+1:], root[:])

		var val [9]byte
		val[0] = 0
		binary.LittleEndian.PutUint64(val[1:], slot)
		return db.db.Put(key[:], val[:], nil)
	} else {
		b := new(leveldb.Batch)
		var keyScratch [prefixLen + gindexLenByteLen + maxGindexByteLen + 32]byte
		copy(keyScratch[0:prefixLen], db.prefix[:])

		var add func(gindexBitIndex uint32, node Node) error
		add = func(gindexBitIndex uint32, node Node) error {
			if gindexBitIndex >= maxGindexByteLen*8 {
				return errors.New("gindex too large")
			}

			if node.IsLeaf() {
				max := prefixLen + gindexLenByteLen + (1 + uint16(gindexBitIndex>>3)) + 32
				// update to the current gindex bit length
				binary.LittleEndian.PutUint16(keyScratch[prefixLen:prefixLen+gindexLenByteLen], uint16(gindexBitIndex+1))

				var val [9]byte
				val[0] = 0
				binary.LittleEndian.PutUint64(val[1:], slot)

				// Note that the key scratchpad is already prepared by the caller, no work left to do.
				b.Put(keyScratch[:max], val[:])
				return nil
			} else {
				var val [1 + 8 + 32 + 32]byte
				val[0] = 1
				binary.LittleEndian.PutUint64(val[1:1+8], slot)
				left, err := node.Left()
				if err != nil {
					return err
				}
				right, err := node.Right()
				if err != nil {
					return err
				}
				leftRoot := left.MerkleRoot(fn)
				rightRoot := right.MerkleRoot(fn)
				copy(val[1+8:1+8+32], leftRoot[:])
				copy(val[1+8+32:1+8+32+32], rightRoot[:])

				// update to the current gindex bit length
				binary.LittleEndian.PutUint16(keyScratch[prefixLen:prefixLen+gindexLenByteLen], uint16(gindexBitIndex+1))

				max := prefixLen + gindexLenByteLen + (1 + uint16(gindexBitIndex>>3)) + 32

				// insert the pair node
				b.Put(keyScratch[:max], val[:])

				// going deeper
				gindexBitIndex += 1
				lastGindexByteIndex := prefixLen + gindexLenByteLen + uint16(gindexBitIndex>>3)
				max = lastGindexByteIndex + 1 + 32

				currentBit := uint8(1) << (7 - (uint8(gindexBitIndex) & 7))
				// Reset current and trailing bits zero
				keyScratch[lastGindexByteIndex] &^= currentBit | (currentBit - 1)

				max -= 32
				copy(keyScratch[max:max+32], leftRoot[:])
				max += 32

				// check if the key exists already. If it does, we don't need to insert it again
				if exists, err := db.db.Has(keyScratch[:max], nil); err != nil {
					return err
				} else if !exists {
					if err := add(gindexBitIndex, left); err != nil {
						return fmt.Errorf("failed to add left node to batch: %v", err)
					}
				}

				// Set current bit to one, to identify the right node
				keyScratch[lastGindexByteIndex] |= currentBit
				// Reset trailing bits zero
				keyScratch[lastGindexByteIndex] &^= currentBit - 1

				max = lastGindexByteIndex + 1
				copy(keyScratch[max:max+32], rightRoot[:])
				max += 32

				// check if the key exists already. If it does, we don't need to insert it again
				if exists, err := db.db.Has(keyScratch[:max], nil); err != nil {
					return err
				} else if !exists {
					if err := add(gindexBitIndex, right); err != nil {
						return fmt.Errorf("failed to add right node to batch: %v", err)
					}
				}

				return nil
			}
		}
		// gindex length: 1 bit, takes just 1 byte
		keyScratch[prefixLen] = 1
		keyScratch[prefixLen+1] = 0
		// gindex: root node == 1 (left aligned)
		keyScratch[prefixLen+gindexLenByteLen] = 1 << 7
		root := node.MerkleRoot(fn)
		max := prefixLen + gindexLenByteLen + 1 + 32
		copy(keyScratch[prefixLen+gindexLenByteLen+1:max], root[:])
		if err := add(0, node); err != nil {
			return fmt.Errorf("failed to add anchor pair node: %v", err)
		}

		return db.db.Write(b, nil)
	}
}

func (db *merkleDB) buildKey(gindex Gindex, key Root) []byte {
	data, bitLen := gindex.LeftAlignedBigEndian()
	size := prefixLen + gindexLenByteLen + uint64(len(data)) + 32
	keyData := make([]byte, size, size)
	copy(keyData[0:prefixLen], db.prefix[:])
	binary.LittleEndian.PutUint16(keyData[prefixLen:prefixLen+gindexLenByteLen], uint16(bitLen))
	copy(keyData[prefixLen+gindexLenByteLen:prefixLen+gindexLenByteLen+len(data)], data)
	copy(keyData[prefixLen+gindexLenByteLen+uint64(len(data)):], key[:])
	return keyData
}

func (db *merkleDB) Get(gindex Gindex, key Root) (SlottedNode, error) {
	out, err := db.db.Get(db.buildKey(gindex, key), nil)
	if err != nil {
		return SlottedNode{}, err
	}
	if len(out) < 1+8 {
		return SlottedNode{}, fmt.Errorf("key '%x' has corrupt value, too short: '%x'", key, out)
	}
	typ := out[0]
	if typ == 0 {
		slot := binary.LittleEndian.Uint64(out[1 : 1+8])
		return SlottedNode{Slot: slot, Node: &key}, nil
	} else if typ == 1 {
		if len(out) != 1+8+32+32 {
			return SlottedNode{}, fmt.Errorf("key '%x' has corrupt pair value, invalid length: '%x'", key, out)
		}
		slot := binary.LittleEndian.Uint64(out[1 : 1+8])
		var left, right Root
		copy(left[:], out[1+8:1+8+32])
		copy(right[:], out[1+8+32:1+8+32+32])
		node := NewVirtualNode(db, gindex, key, left, right)
		return SlottedNode{Slot: slot, Node: node}, nil
	} else {
		return SlottedNode{}, fmt.Errorf("key '%x' has corrupt value, unrecognized typ: '%x'", key, out)
	}
}

func (db *merkleDB) Has(gindex Gindex, key Root) (bool, error) {
	return db.db.Has(db.buildKey(gindex, key), nil)
}

func (db *merkleDB) Delete(gindex Gindex, key Root) error {
	return db.db.Delete(db.buildKey(gindex, key), nil)
}

func (db *merkleDB) Range(startSlot uint64, endSlot uint64, gindex Gindex) ([]SlottedNode, error) {
	panic("implement me")
}

func (db *merkleDB) Close() error {
	return db.db.Close()
}

var _ MerkleDB = (*merkleDB)(nil)

type VirtualNode interface {
	Node
	Detach() error
}

type virtualNode struct {
	db         MerkleDB
	gindex     Gindex
	self       Root
	left       Root
	right      Root
	cacheLeft  Node
	cacheRight Node
}

func NewVirtualNode(db MerkleDB, gindex Gindex, key Root, left Root, right Root) VirtualNode {
	return &virtualNode{
		db:         db,
		gindex:     gindex,
		self:       key,
		left:       left,
		right:      right,
		cacheLeft:  nil,
		cacheRight: nil,
	}
}

// Loads the left and right nodes, caches them, and detaches the db reference
func (v virtualNode) Detach() error {
	_, err := v.Left()
	if err != nil {
		return err
	}
	_, err = v.Right()
	return err
}

func (v virtualNode) Left() (Node, error) {
	if v.cacheLeft != nil {
		return v.cacheLeft, nil
	}
	slotted, err := v.db.Get(v.gindex.Left(), v.left)
	if err != nil {
		return nil, err
	}
	v.cacheLeft = slotted.Node
	// if we also have the other node, get rid of the db reference
	if v.cacheRight != nil {
		v.db = nil
	}
	return slotted.Node, nil
}

func (v virtualNode) Right() (Node, error) {
	if v.cacheRight != nil {
		return v.cacheRight, nil
	}
	slotted, err := v.db.Get(v.gindex.Right(), v.right)
	if err != nil {
		return nil, err
	}
	v.cacheRight = slotted.Node
	// if we also have the other node, get rid of the db reference
	if v.cacheLeft != nil {
		v.db = nil
	}
	return slotted.Node, nil
}

func (v virtualNode) IsLeaf() bool {
	return false
}

func (v virtualNode) RebindLeft(left Node) (Node, error) {
	right, err := v.Right()
	if err != nil {
		return nil, err
	}
	return NewPairNode(left, right), nil
}

func (v virtualNode) RebindRight(right Node) (Node, error) {
	left, err := v.Left()
	if err != nil {
		return nil, err
	}
	return NewPairNode(left, right), nil
}

func (v virtualNode) Getter(target Gindex) (Node, error) {
	if target.IsRoot() {
		return v, nil
	}
	if target.IsLeft() {
		left, err := v.Left()
		if err != nil {
			return nil, err
		}
		return left.Getter(target.Left())
	} else {
		right, err := v.Right()
		if err != nil {
			return nil, err
		}
		return right.Getter(target.Right())
	}
}

func (v virtualNode) Setter(target Gindex, expand bool) (Link, error) {
	if target.IsRoot() {
		return Identity, nil
	}
	if target.IsClose() {
		if target.IsLeft() {
			return v.RebindLeft, nil
		} else {
			return v.RebindRight, nil
		}
	}
	// TODO: maybe make this depth 1 part lazy-loading as well?
	if target.IsLeft() {
		left, err := v.Left()
		if err != nil {
			return nil, err
		}
		return DeeperSetter(v.RebindLeft, left, target, expand)
	} else {
		right, err := v.Right()
		if err != nil {
			return nil, err
		}
		return DeeperSetter(v.RebindRight, right, target, expand)
	}
}

func (v virtualNode) SummarizeInto(target Gindex, h HashFn) (SummaryLink, error) {
	return SummaryInto(v, target, h)
}

func (v virtualNode) MerkleRoot(HashFn) Root {
	return v.self
}

var _ Node = (*virtualNode)(nil)
