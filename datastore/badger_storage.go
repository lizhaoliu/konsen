package datastore

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"github.com/sirupsen/logrus"
)

type Badger struct {
	logDB   *badger.DB
	stateDB *badger.DB
}

type BadgerConfig struct {
	LogDir   string
	StateDir string
}

func NewBadger(config BadgerConfig) (*Badger, error) {
	logrus.Infof("Badger log DB file set to: %s", config.LogDir)
	logDB, err := badger.Open(badger.DefaultOptions(config.LogDir))
	if err != nil {
		return nil, err
	}

	logrus.Infof("Badger state DB file set to: %s", config.StateDir)
	stateDB, err := badger.Open(badger.DefaultOptions(config.StateDir))
	if err != nil {
		return nil, err
	}

	return &Badger{
		logDB:   logDB,
		stateDB: stateDB,
	}, nil
}

func (b *Badger) GetCurrentTerm() (uint64, error) {
	var term uint64
	if err := b.stateDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(currentTermKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			term = bytesToUint64(val)
			return nil
		})
	}); err != nil && err != badger.ErrKeyNotFound {
		return 0, err
	}
	return term, nil
}

func (b *Badger) SetCurrentTerm(term uint64) error {
	return b.stateDB.Update(func(txn *badger.Txn) error {
		return txn.Set(currentTermKey, uint64ToBytes(term))
	})
}

func (b *Badger) GetVotedFor() (string, error) {
	var votedFor string
	if err := b.stateDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(votedForKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			votedFor = string(val)
			return nil
		})
	}); err != nil && err != badger.ErrKeyNotFound {
		return "", err
	}
	return votedFor, nil
}

func (b *Badger) SetVotedFor(candidateID string) error {
	return b.stateDB.Update(func(txn *badger.Txn) error {
		return txn.Set(votedForKey, []byte(candidateID))
	})
}

func (b *Badger) GetLog(logIndex uint64) (*konsen.Log, error) {
	var log *konsen.Log
	if err := b.logDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(uint64ToBytes(logIndex))
		if err != nil {
			return err
		}
		log = &konsen.Log{}
		return item.Value(func(val []byte) error {
			return proto.Unmarshal(val, log)
		})
	}); err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	return log, nil
}

func (b *Badger) GetLogsFrom(minLogIndex uint64) ([]*konsen.Log, error) {
	var logs []*konsen.Log
	if err := b.logDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(uint64ToBytes(minLogIndex)); it.Valid(); it.Next() {
			if err := it.Item().Value(func(val []byte) error {
				log := &konsen.Log{}
				if err := proto.Unmarshal(val, log); err != nil {
					return err
				}
				logs = append(logs, log)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return logs, nil
}

func (b *Badger) GetLogTerm(logIndex uint64) (uint64, error) {
	log, err := b.GetLog(logIndex)
	if err != nil {
		return 0, err
	}
	if log == nil {
		return 0, nil
	}
	return log.GetTerm(), nil
}

func (b *Badger) WriteLog(log *konsen.Log) error {
	return b.WriteLogs([]*konsen.Log{log})
}

func (b *Badger) WriteLogs(logs []*konsen.Log) error {
	return b.logDB.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			buf, err := proto.Marshal(log)
			if err != nil {
				return err
			}
			if err := txn.Set(uint64ToBytes(log.GetIndex()), buf); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *Badger) LastLogIndex() (uint64, error) {
	var index uint64
	if err := b.logDB.View(func(txn *badger.Txn) error {
		itOpt := badger.DefaultIteratorOptions
		itOpt.Reverse = true
		itOpt.PrefetchValues = false
		it := txn.NewIterator(itOpt)
		defer it.Close()
		it.Rewind()
		if it.Valid() {
			index = bytesToUint64(it.Item().Key())
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return index, nil
}

func (b *Badger) LastLogTerm() (uint64, error) {
	var term uint64
	if err := b.logDB.View(func(txn *badger.Txn) error {
		itOpt := badger.DefaultIteratorOptions
		itOpt.Reverse = true
		itOpt.PrefetchValues = false
		it := txn.NewIterator(itOpt)
		defer it.Close()
		it.Rewind()
		if it.Valid() {
			if err := it.Item().Value(func(val []byte) error {
				log := &konsen.Log{}
				if err := proto.Unmarshal(val, log); err != nil {
					return err
				}
				term = log.GetTerm()
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return term, nil
}

func (b *Badger) DeleteLogsFrom(minLogIndex uint64) error {
	if err := b.logDB.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(uint64ToBytes(minLogIndex)); it.Valid(); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil

}

func (b *Badger) SetValue(key []byte, value []byte) error {
	panic("implement me")
}

func (b *Badger) GetValue(key []byte) ([]byte, error) {
	panic("implement me")
}

func (b *Badger) Close() error {
	stateDBErr := b.stateDB.Close()
	logDBErr := b.logDB.Close()
	if stateDBErr != nil {
		return stateDBErr
	}
	if logDBErr != nil {
		return logDBErr
	}
	return nil
}
