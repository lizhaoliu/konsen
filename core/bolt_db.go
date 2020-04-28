package core

import (
	"encoding/binary"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/sirupsen/logrus"
)

var (
	logsBucketName   = []byte("logs")
	statesBucketName = []byte("states")

	currentTermKey = []byte("current_term")
	votedForKey    = []byte("voted_for")
)

// BoltDB is a storage implementation utilizing Bolt file database.
type BoltDB struct {
	filePath string
	db       *bolt.DB
}

type BoltDBConfig struct {
	FilePath string
}

func NewBoltDB(config BoltDBConfig) (*BoltDB, error) {
	logrus.Infof("BoltDB storage file set to: %s", config.FilePath)
	db, err := bolt.Open(config.FilePath, 0600, nil)
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(logsBucketName)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(statesBucketName)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	storage := &BoltDB{
		filePath: config.FilePath,
		db:       db,
	}

	return storage, nil
}

func (b *BoltDB) GetCurrentTerm() (uint64, error) {
	var term uint64
	if err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(statesBucketName)
		buf := b.Get(currentTermKey)
		if buf == nil {
			return nil
		}
		term = bytesToUint64(buf)
		return nil
	}); err != nil {
		return 0, err
	}
	return term, nil
}

func (b *BoltDB) SetCurrentTerm(term uint64) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(statesBucketName)
		return b.Put(currentTermKey, uint64ToBytes(term))
	})
}

func (b *BoltDB) GetVotedFor() (string, error) {
	var votedFor string
	if err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(statesBucketName)
		buf := b.Get(votedForKey)
		votedFor = string(buf)
		return nil
	}); err != nil {
		return "", err
	}
	return votedFor, nil
}

func (b *BoltDB) SetVotedFor(candidateID string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(statesBucketName)
		v := []byte(candidateID)
		return b.Put(votedForKey, v)
	})
}

func (b *BoltDB) GetLog(logIndex uint64) (*konsen.Log, error) {
	var log *konsen.Log
	if err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucketName)
		k := uint64ToBytes(logIndex)
		buf := b.Get(k)
		if buf == nil {
			return nil
		}
		log = &konsen.Log{}
		return proto.Unmarshal(buf, log)
	}); err != nil {
		return nil, err
	}
	return log, nil
}

func (b *BoltDB) GetLogsFrom(minLogIndex uint64) ([]*konsen.Log, error) {
	var logs []*konsen.Log
	if err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(logsBucketName).Cursor()
		for k, buf := c.Seek(uint64ToBytes(minLogIndex)); k != nil; k, buf = c.Next() {
			log := &konsen.Log{}
			if err := proto.Unmarshal(buf, log); err != nil {
				return err
			}
			logs = append(logs, log)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return logs, nil
}

func (b *BoltDB) GetLogTerm(logIndex uint64) (uint64, error) {
	log, err := b.GetLog(logIndex)
	if err != nil {
		return 0, err
	}
	if log == nil {
		return 0, nil
	}
	return log.GetTerm(), nil
}

func (b *BoltDB) WriteLog(log *konsen.Log) error {
	return b.WriteLogs([]*konsen.Log{log})
}

func (b *BoltDB) WriteLogs(logs []*konsen.Log) error {
	if len(logs) == 0 {
		return nil
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logsBucketName)
		for _, log := range logs {
			k := uint64ToBytes(log.Index)
			v, err := proto.Marshal(log)
			if err != nil {
				return err
			}
			if err := b.Put(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltDB) DeleteLogsFrom(minLogIndex uint64) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket(logsBucketName).Cursor()
		for k, _ := c.Seek(uint64ToBytes(minLogIndex)); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltDB) FirstLogIndex() (uint64, error) {
	var index uint64
	if err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(logsBucketName).Cursor()
		buf, _ := c.First()
		if buf == nil {
			return nil
		}
		index = bytesToUint64(buf)
		return nil
	}); err != nil {
		return 0, err
	}
	return index, nil
}

func (b *BoltDB) LastLogIndex() (uint64, error) {
	var index uint64
	if err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(logsBucketName).Cursor()
		buf, _ := c.Last()
		if buf == nil {
			return nil
		}
		index = bytesToUint64(buf)
		return nil
	}); err != nil {
		return 0, err
	}
	return index, nil
}

func (b *BoltDB) FirstLogTerm() (uint64, error) {
	var term uint64
	if err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(logsBucketName).Cursor()
		_, buf := c.First()
		if buf == nil {
			return nil
		}
		log := &konsen.Log{}
		if err := proto.Unmarshal(buf, log); err != nil {
			return err
		}
		term = log.GetTerm()
		return nil
	}); err != nil {
		return 0, err
	}
	return term, nil
}

func (b *BoltDB) LastLogTerm() (uint64, error) {
	var term uint64
	if err := b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(logsBucketName).Cursor()
		_, buf := c.Last()
		if buf == nil {
			return nil
		}
		log := &konsen.Log{}
		if err := proto.Unmarshal(buf, log); err != nil {
			return err
		}
		term = log.GetTerm()
		return nil
	}); err != nil {
		return 0, err
	}
	return term, nil
}

func (b *BoltDB) Close() error {
	return b.db.Close()
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
