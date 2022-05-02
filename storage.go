package paxi

import (
	"encoding/binary"
	"github.com/ailidani/paxi/log"
	"github.com/dgraph-io/badger/v3"
)

const DefaultPath = "/tmp/paxi"

// TODO: finish the implementation
type PersistentStorage struct {
	db *badger.DB
}

func NewPersistentStorage(id ID) PersistentStorage {
	path := GetConfig().StoragePath
	if path == "" {
		path = DefaultPath + "_" + string(id)
	}
	d, err := badger.Open(badger.DefaultOptions(path).WithLogger(nil))
	if err != nil {
		log.Errorf("failed to open persistent file: %v", err)
	}
	return PersistentStorage{
		db: d,
	}
}

func (s *PersistentStorage) PersistBallot(ballot Ballot) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		ballotByte := make([]byte, 8)
		binary.BigEndian.PutUint64(ballotByte, uint64(ballot))
		err := txn.Set([]byte("max_ballot"), ballotByte)
		return err
	})
	_ = s.db.Sync()
	return err
}

func (s *PersistentStorage) GetBallot() (Ballot, error) {
	ballotByte := make([]byte, 8)
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("max_ballot"))
		if err != nil {
			return err
		}

		if _, err = item.ValueCopy(ballotByte); err != nil {
			return err
		}

		return nil
	})

	return Ballot(binary.BigEndian.Uint64(ballotByte)), err
}

func (s *PersistentStorage) PersistValue(slot int, ballot Ballot, value []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		keyByte := make([]byte, 8)
		binary.BigEndian.PutUint64(keyByte, uint64(slot))
		err := txn.Set(keyByte, value)
		return err
	})
	_ = s.db.Sync()
	return err
}

func (s *PersistentStorage) GetValue() map[int][]byte {
	// TODO
	return nil
}

func (s *PersistentStorage) ClearValue(slot int) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		keyByte := make([]byte, 8)
		binary.BigEndian.PutUint64(keyByte, uint64(slot))
		err := txn.Delete(keyByte)
		return err
	})
	_ = s.db.Sync()
	return err
}
