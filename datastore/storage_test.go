package datastore

import (
	"os"
	"path/filepath"
	"testing"

	konsen "github.com/lizhaoliu/konsen/v2/proto"
	"google.golang.org/protobuf/proto"
)

// storageFactory creates a new Storage instance for testing and returns a cleanup function.
type storageFactory func(t *testing.T) (Storage, func())

func memFactory(t *testing.T) (Storage, func()) {
	return NewMemStorage(), func() {}
}

func boltFactory(t *testing.T) (Storage, func()) {
	dir := t.TempDir()
	s, err := NewBoltDB(BoltDBConfig{FilePath: filepath.Join(dir, "test.db")})
	if err != nil {
		t.Fatalf("failed to create BoltDB: %v", err)
	}
	return s, func() { s.Close() }
}

func badgerFactory(t *testing.T) (Storage, func()) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "log")
	stateDir := filepath.Join(dir, "state")
	os.MkdirAll(logDir, 0o755)
	os.MkdirAll(stateDir, 0o755)
	s, err := NewBadger(BadgerConfig{LogDir: logDir, StateDir: stateDir})
	if err != nil {
		t.Fatalf("failed to create Badger: %v", err)
	}
	return s, func() { s.Close() }
}

// allFactories returns test cases for each storage backend.
func allFactories() []struct {
	name    string
	factory storageFactory
} {
	return []struct {
		name    string
		factory storageFactory
	}{
		{"MemStorage", memFactory},
		{"BoltDB", boltFactory},
		{"Badger", badgerFactory},
	}
}

func TestCurrentTerm(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Initial term should be 0.
			term, err := s.GetCurrentTerm()
			if err != nil {
				t.Fatalf("GetCurrentTerm: %v", err)
			}
			if term != 0 {
				t.Errorf("initial term = %d, want 0", term)
			}

			// Set and get term.
			if err := s.SetCurrentTerm(5); err != nil {
				t.Fatalf("SetCurrentTerm: %v", err)
			}
			term, err = s.GetCurrentTerm()
			if err != nil {
				t.Fatalf("GetCurrentTerm: %v", err)
			}
			if term != 5 {
				t.Errorf("term = %d, want 5", term)
			}
		})
	}
}

func TestVotedFor(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Initial votedFor should be empty.
			vf, err := s.GetVotedFor()
			if err != nil {
				t.Fatalf("GetVotedFor: %v", err)
			}
			if vf != "" {
				t.Errorf("initial votedFor = %q, want empty", vf)
			}

			// Set and get votedFor.
			if err := s.SetVotedFor("node1"); err != nil {
				t.Fatalf("SetVotedFor: %v", err)
			}
			vf, err = s.GetVotedFor()
			if err != nil {
				t.Fatalf("GetVotedFor: %v", err)
			}
			if vf != "node1" {
				t.Errorf("votedFor = %q, want %q", vf, "node1")
			}
		})
	}
}

func TestSetTermAndVotedFor(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			if err := s.SetTermAndVotedFor(3, "candidateA"); err != nil {
				t.Fatalf("SetTermAndVotedFor: %v", err)
			}

			term, err := s.GetCurrentTerm()
			if err != nil {
				t.Fatalf("GetCurrentTerm: %v", err)
			}
			if term != 3 {
				t.Errorf("term = %d, want 3", term)
			}

			vf, err := s.GetVotedFor()
			if err != nil {
				t.Fatalf("GetVotedFor: %v", err)
			}
			if vf != "candidateA" {
				t.Errorf("votedFor = %q, want %q", vf, "candidateA")
			}
		})
	}
}

func TestWriteAndGetLog(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			log := &konsen.Log{Index: 1, Term: 1, Data: []byte("hello")}
			if err := s.WriteLog(log); err != nil {
				t.Fatalf("WriteLog: %v", err)
			}

			got, err := s.GetLog(1)
			if err != nil {
				t.Fatalf("GetLog: %v", err)
			}
			if got == nil {
				t.Fatal("GetLog returned nil")
			}
			if got.GetIndex() != 1 || got.GetTerm() != 1 || string(got.GetData()) != "hello" {
				t.Errorf("GetLog = %v, want index=1 term=1 data=hello", got)
			}

			// Non-existent log should return nil.
			got, err = s.GetLog(999)
			if err != nil {
				t.Fatalf("GetLog(999): %v", err)
			}
			if got != nil {
				t.Errorf("GetLog(999) = %v, want nil", got)
			}
		})
	}
}

func TestWriteLogsAndGetLogsFrom(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			logs := []*konsen.Log{
				{Index: 1, Term: 1, Data: []byte("a")},
				{Index: 2, Term: 1, Data: []byte("b")},
				{Index: 3, Term: 2, Data: []byte("c")},
			}
			if err := s.WriteLogs(logs); err != nil {
				t.Fatalf("WriteLogs: %v", err)
			}

			// Get all logs from index 1.
			got, err := s.GetLogsFrom(1)
			if err != nil {
				t.Fatalf("GetLogsFrom(1): %v", err)
			}
			if len(got) != 3 {
				t.Fatalf("GetLogsFrom(1) returned %d logs, want 3", len(got))
			}

			// Get logs from index 2.
			got, err = s.GetLogsFrom(2)
			if err != nil {
				t.Fatalf("GetLogsFrom(2): %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("GetLogsFrom(2) returned %d logs, want 2", len(got))
			}
			if got[0].GetIndex() != 2 || got[1].GetIndex() != 3 {
				t.Errorf("GetLogsFrom(2) indices = [%d, %d], want [2, 3]", got[0].GetIndex(), got[1].GetIndex())
			}

			// Get logs from index beyond range.
			got, err = s.GetLogsFrom(100)
			if err != nil {
				t.Fatalf("GetLogsFrom(100): %v", err)
			}
			if len(got) != 0 {
				t.Errorf("GetLogsFrom(100) returned %d logs, want 0", len(got))
			}
		})
	}
}

func TestGetLogTerm(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			s.WriteLog(&konsen.Log{Index: 1, Term: 3, Data: []byte("x")})

			term, err := s.GetLogTerm(1)
			if err != nil {
				t.Fatalf("GetLogTerm(1): %v", err)
			}
			if term != 3 {
				t.Errorf("GetLogTerm(1) = %d, want 3", term)
			}

			// Non-existent log should return term 0.
			term, err = s.GetLogTerm(999)
			if err != nil {
				t.Fatalf("GetLogTerm(999): %v", err)
			}
			if term != 0 {
				t.Errorf("GetLogTerm(999) = %d, want 0", term)
			}
		})
	}
}

func TestLastLogIndexAndTerm(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Empty storage.
			idx, err := s.LastLogIndex()
			if err != nil {
				t.Fatalf("LastLogIndex: %v", err)
			}
			if idx != 0 {
				t.Errorf("LastLogIndex on empty = %d, want 0", idx)
			}
			term, err := s.LastLogTerm()
			if err != nil {
				t.Fatalf("LastLogTerm: %v", err)
			}
			if term != 0 {
				t.Errorf("LastLogTerm on empty = %d, want 0", term)
			}

			// After writing logs.
			s.WriteLogs([]*konsen.Log{
				{Index: 1, Term: 1},
				{Index: 2, Term: 3},
				{Index: 3, Term: 5},
			})

			idx, err = s.LastLogIndex()
			if err != nil {
				t.Fatalf("LastLogIndex: %v", err)
			}
			if idx != 3 {
				t.Errorf("LastLogIndex = %d, want 3", idx)
			}
			term, err = s.LastLogTerm()
			if err != nil {
				t.Fatalf("LastLogTerm: %v", err)
			}
			if term != 5 {
				t.Errorf("LastLogTerm = %d, want 5", term)
			}
		})
	}
}

func TestDeleteLogsFrom(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			s.WriteLogs([]*konsen.Log{
				{Index: 1, Term: 1},
				{Index: 2, Term: 1},
				{Index: 3, Term: 2},
				{Index: 4, Term: 2},
			})

			if err := s.DeleteLogsFrom(3); err != nil {
				t.Fatalf("DeleteLogsFrom(3): %v", err)
			}

			idx, _ := s.LastLogIndex()
			if idx != 2 {
				t.Errorf("LastLogIndex after delete = %d, want 2", idx)
			}

			// Logs 1 and 2 should still exist.
			log1, _ := s.GetLog(1)
			log2, _ := s.GetLog(2)
			if log1 == nil || log2 == nil {
				t.Error("logs 1 and 2 should still exist after DeleteLogsFrom(3)")
			}

			// Logs 3 and 4 should be deleted.
			log3, _ := s.GetLog(3)
			log4, _ := s.GetLog(4)
			if log3 != nil || log4 != nil {
				t.Error("logs 3 and 4 should be deleted after DeleteLogsFrom(3)")
			}
		})
	}
}

func TestKeyValue(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Non-existent key.
			val, err := s.GetValue([]byte("missing"))
			if err != nil {
				t.Fatalf("GetValue: %v", err)
			}
			if val != nil {
				t.Errorf("GetValue(missing) = %v, want nil", val)
			}

			// Set and get.
			if err := s.SetValue([]byte("key1"), []byte("value1")); err != nil {
				t.Fatalf("SetValue: %v", err)
			}
			val, err = s.GetValue([]byte("key1"))
			if err != nil {
				t.Fatalf("GetValue: %v", err)
			}
			if string(val) != "value1" {
				t.Errorf("GetValue(key1) = %q, want %q", val, "value1")
			}

			// Overwrite.
			if err := s.SetValue([]byte("key1"), []byte("value2")); err != nil {
				t.Fatalf("SetValue: %v", err)
			}
			val, err = s.GetValue([]byte("key1"))
			if err != nil {
				t.Fatalf("GetValue: %v", err)
			}
			if string(val) != "value2" {
				t.Errorf("GetValue(key1) after overwrite = %q, want %q", val, "value2")
			}
		})
	}
}

func TestListKeys(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Empty store returns no keys.
			keys, err := s.ListKeys(nil, 0)
			if err != nil {
				t.Fatalf("ListKeys empty: %v", err)
			}
			if len(keys) != 0 {
				t.Fatalf("expected 0 keys, got %d", len(keys))
			}

			// Insert some keys.
			for _, kv := range []struct{ k, v string }{
				{"apple", "1"},
				{"app", "2"},
				{"banana", "3"},
				{"band", "4"},
				{"cherry", "5"},
			} {
				if err := s.SetValue([]byte(kv.k), []byte(kv.v)); err != nil {
					t.Fatalf("SetValue(%q): %v", kv.k, err)
				}
			}

			// List all keys.
			keys, err = s.ListKeys(nil, 0)
			if err != nil {
				t.Fatalf("ListKeys all: %v", err)
			}
			if len(keys) != 5 {
				t.Fatalf("expected 5 keys, got %d", len(keys))
			}

			// List with prefix.
			keys, err = s.ListKeys([]byte("app"), 0)
			if err != nil {
				t.Fatalf("ListKeys prefix: %v", err)
			}
			if len(keys) != 2 {
				t.Errorf("expected 2 keys with prefix 'app', got %d: %v", len(keys), keys)
			}

			// List with prefix "ban".
			keys, err = s.ListKeys([]byte("ban"), 0)
			if err != nil {
				t.Fatalf("ListKeys prefix ban: %v", err)
			}
			if len(keys) != 2 {
				t.Errorf("expected 2 keys with prefix 'ban', got %d: %v", len(keys), keys)
			}

			// List with limit.
			keys, err = s.ListKeys(nil, 3)
			if err != nil {
				t.Fatalf("ListKeys limit: %v", err)
			}
			if len(keys) != 3 {
				t.Errorf("expected 3 keys with limit, got %d", len(keys))
			}

			// No match prefix.
			keys, err = s.ListKeys([]byte("zzz"), 0)
			if err != nil {
				t.Fatalf("ListKeys no match: %v", err)
			}
			if len(keys) != 0 {
				t.Errorf("expected 0 keys for prefix 'zzz', got %d", len(keys))
			}
		})
	}
}

func TestClose(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := tc.factory(t)
			if err := s.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
}

func TestSnapshotMeta(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Initial snapshot meta should be (0, 0).
			idx, term, err := s.GetSnapshotMeta()
			if err != nil {
				t.Fatalf("GetSnapshotMeta: %v", err)
			}
			if idx != 0 || term != 0 {
				t.Errorf("initial snapshot meta = (%d, %d), want (0, 0)", idx, term)
			}

			// Set and get.
			if err := s.SetSnapshotMeta(10, 3); err != nil {
				t.Fatalf("SetSnapshotMeta: %v", err)
			}
			idx, term, err = s.GetSnapshotMeta()
			if err != nil {
				t.Fatalf("GetSnapshotMeta: %v", err)
			}
			if idx != 10 || term != 3 {
				t.Errorf("snapshot meta = (%d, %d), want (10, 3)", idx, term)
			}

			// Overwrite.
			if err := s.SetSnapshotMeta(50, 7); err != nil {
				t.Fatalf("SetSnapshotMeta: %v", err)
			}
			idx, term, err = s.GetSnapshotMeta()
			if err != nil {
				t.Fatalf("GetSnapshotMeta: %v", err)
			}
			if idx != 50 || term != 7 {
				t.Errorf("snapshot meta = (%d, %d), want (50, 7)", idx, term)
			}
		})
	}
}

func TestDeleteLogsUpTo(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			s.WriteLogs([]*konsen.Log{
				{Index: 1, Term: 1, Data: []byte("a")},
				{Index: 2, Term: 1, Data: []byte("b")},
				{Index: 3, Term: 2, Data: []byte("c")},
				{Index: 4, Term: 2, Data: []byte("d")},
				{Index: 5, Term: 3, Data: []byte("e")},
			})

			// Delete logs up to index 3.
			if err := s.DeleteLogsUpTo(3); err != nil {
				t.Fatalf("DeleteLogsUpTo(3): %v", err)
			}

			// Logs 1-3 should be gone.
			for i := uint64(1); i <= 3; i++ {
				log, err := s.GetLog(i)
				if err != nil {
					t.Fatalf("GetLog(%d): %v", i, err)
				}
				if log != nil {
					t.Errorf("GetLog(%d) should be nil after DeleteLogsUpTo(3)", i)
				}
			}

			// Logs 4-5 should remain.
			for i := uint64(4); i <= 5; i++ {
				log, err := s.GetLog(i)
				if err != nil {
					t.Fatalf("GetLog(%d): %v", i, err)
				}
				if log == nil {
					t.Errorf("GetLog(%d) should not be nil after DeleteLogsUpTo(3)", i)
				}
			}

			// LastLogIndex should still be 5.
			idx, _ := s.LastLogIndex()
			if idx != 5 {
				t.Errorf("LastLogIndex = %d, want 5", idx)
			}
		})
	}
}

func TestSnapshotKVData(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Write some KV pairs.
			s.SetValue([]byte("key1"), []byte("val1"))
			s.SetValue([]byte("key2"), []byte("val2"))
			s.SetValue([]byte("key3"), []byte("val3"))

			// Snapshot.
			data, err := s.SnapshotKVData()
			if err != nil {
				t.Fatalf("SnapshotKVData: %v", err)
			}
			if len(data) == 0 {
				t.Fatal("SnapshotKVData returned empty data")
			}

			// Unmarshal and verify.
			kvList := &konsen.KVList{}
			if err := proto.Unmarshal(data, kvList); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if len(kvList.GetKvList()) != 3 {
				t.Errorf("snapshot contains %d KV pairs, want 3", len(kvList.GetKvList()))
			}

			// Build a map for verification.
			kvMap := make(map[string]string)
			for _, kv := range kvList.GetKvList() {
				kvMap[string(kv.GetKey())] = string(kv.GetValue())
			}
			for _, k := range []string{"key1", "key2", "key3"} {
				expected := "val" + k[len(k)-1:]
				if kvMap[k] != expected {
					t.Errorf("snapshot[%s] = %q, want %q", k, kvMap[k], expected)
				}
			}
		})
	}
}

func TestRestoreKVData(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Write initial KV data.
			s.SetValue([]byte("old1"), []byte("oldval1"))
			s.SetValue([]byte("old2"), []byte("oldval2"))

			// Prepare new KV data.
			newKVList := &konsen.KVList{
				KvList: []*konsen.KV{
					{Key: []byte("new1"), Value: []byte("newval1")},
					{Key: []byte("new2"), Value: []byte("newval2")},
				},
			}
			data, err := proto.Marshal(newKVList)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}

			// Restore.
			if err := s.RestoreKVData(data); err != nil {
				t.Fatalf("RestoreKVData: %v", err)
			}

			// Old keys should be gone.
			for _, k := range []string{"old1", "old2"} {
				v, err := s.GetValue([]byte(k))
				if err != nil {
					t.Fatalf("GetValue(%s): %v", k, err)
				}
				if v != nil {
					t.Errorf("GetValue(%s) = %q, want nil after restore", k, v)
				}
			}

			// New keys should exist.
			v, _ := s.GetValue([]byte("new1"))
			if string(v) != "newval1" {
				t.Errorf("GetValue(new1) = %q, want %q", v, "newval1")
			}
			v, _ = s.GetValue([]byte("new2"))
			if string(v) != "newval2" {
				t.Errorf("GetValue(new2) = %q, want %q", v, "newval2")
			}
		})
	}
}

func TestSnapshotFile(t *testing.T) {
	for _, tc := range allFactories() {
		t.Run(tc.name, func(t *testing.T) {
			s, cleanup := tc.factory(t)
			defer cleanup()

			// Initially no snapshot file.
			data, err := s.LoadSnapshotFile()
			if err != nil {
				t.Fatalf("LoadSnapshotFile: %v", err)
			}
			if data != nil {
				t.Errorf("initial LoadSnapshotFile = %v, want nil", data)
			}

			// Save and load.
			payload := []byte("snapshot-content-here")
			if err := s.SaveSnapshotFile(payload); err != nil {
				t.Fatalf("SaveSnapshotFile: %v", err)
			}
			data, err = s.LoadSnapshotFile()
			if err != nil {
				t.Fatalf("LoadSnapshotFile: %v", err)
			}
			if string(data) != string(payload) {
				t.Errorf("LoadSnapshotFile = %q, want %q", data, payload)
			}

			// Overwrite.
			payload2 := []byte("updated-snapshot")
			if err := s.SaveSnapshotFile(payload2); err != nil {
				t.Fatalf("SaveSnapshotFile: %v", err)
			}
			data, err = s.LoadSnapshotFile()
			if err != nil {
				t.Fatalf("LoadSnapshotFile: %v", err)
			}
			if string(data) != string(payload2) {
				t.Errorf("LoadSnapshotFile after overwrite = %q, want %q", data, payload2)
			}
		})
	}
}
