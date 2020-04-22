package core

import (
	"encoding/binary"

	"github.com/boltdb/bolt"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
	"github.com/sirupsen/logrus"
)

var (
	logsBucketName   = []byte("logs")
	statesBucketName = []byte("states")

	currentTermKey = []byte("current_term")
	votedForKey    = []byte("voted_for")
)

type LogsStorage struct {
	filePath string
	db       *bolt.DB
}

type LogsStorageConfig struct {
	FilePath string
}

func NewLogsStorage(config LogsStorageConfig) (*LogsStorage, error) {
	logrus.Infof("Creating/loading local storage at: %q", config.FilePath)
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

	storage := &LogsStorage{
		filePath: config.FilePath,
		db:       db,
	}

	return storage, nil
}

func (b *LogsStorage) GetCurrentTerm() (uint64, error) {
	var term uint64
	if err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(statesBucketName)
		term = bytesToUint64(b.Get(currentTermKey))
		return nil
	}); err != nil {
		return 0, err
	}
	return term, nil
}

func (b *LogsStorage) SetCurrentTerm(term uint64) error {
	if err := b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(statesBucketName)
		return b.Put(currentTermKey, uint64ToBytes(term))
	}); err != nil {
		return err
	}
	return nil
}

func (b *LogsStorage) GetVotedFor() (string, error) {
	var votedFor string
	if err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(statesBucketName)
		votedFor = string(b.Get(votedForKey))
		return nil
	}); err != nil {
		return "", err
	}
	return votedFor, nil
}

func (b *LogsStorage) SetVotedFor(candidateID string) error {
	if err := b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(statesBucketName)
		return b.Put(votedForKey, []byte(candidateID))
	}); err != nil {
		return err
	}
	return nil
}

func (b *LogsStorage) GetLog(logIndex uint64) (*konsen.Log, error) {
	panic("implement me")
}

func (b *LogsStorage) AppendLog(log *konsen.Log) error {
	panic("implement me")
}

func (b *LogsStorage) DeleteLogs(minLogIndex uint64) error {
	panic("implement me")
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
