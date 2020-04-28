package core

import (
	"reflect"
	"testing"

	"github.com/boltdb/bolt"
	konsen "github.com/lizhaoliu/konsen/v2/proto_gen"
)

func TestBoltDB_Close(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			if err := b.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Stop() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBoltDB_DeleteLogs(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	type args struct {
		minLogIndex uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			if err := b.DeleteLogsFrom(tt.args.minLogIndex); (err != nil) != tt.wantErr {
				t.Errorf("DeleteLogsFrom() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBoltDB_FirstLogIndex(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	tests := []struct {
		name    string
		fields  fields
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			got, err := b.FirstLogIndex()
			if (err != nil) != tt.wantErr {
				t.Errorf("FirstLogIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FirstLogIndex() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoltDB_FirstLogTerm(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	tests := []struct {
		name    string
		fields  fields
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			got, err := b.FirstLogTerm()
			if (err != nil) != tt.wantErr {
				t.Errorf("FirstLogTerm() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FirstLogTerm() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoltDB_GetCurrentTerm(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	tests := []struct {
		name    string
		fields  fields
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			got, err := b.GetCurrentTerm()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCurrentTerm() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetCurrentTerm() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoltDB_GetLog(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	type args struct {
		logIndex uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *konsen.Log
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			got, err := b.GetLog(tt.args.logIndex)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetLog() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoltDB_GetVotedFor(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			got, err := b.GetVotedFor()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetVotedFor() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetVotedFor() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoltDB_LastLogIndex(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	tests := []struct {
		name    string
		fields  fields
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			got, err := b.LastLogIndex()
			if (err != nil) != tt.wantErr {
				t.Errorf("LastLogIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LastLogIndex() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoltDB_LastLogTerm(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	tests := []struct {
		name    string
		fields  fields
		want    uint64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			got, err := b.LastLogTerm()
			if (err != nil) != tt.wantErr {
				t.Errorf("LastLogTerm() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LastLogTerm() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBoltDB_PutLog(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	type args struct {
		log *konsen.Log
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			if err := b.WriteLog(tt.args.log); (err != nil) != tt.wantErr {
				t.Errorf("WriteLog() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBoltDB_PutLogs(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	type args struct {
		logs []*konsen.Log
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			if err := b.WriteLogs(tt.args.logs); (err != nil) != tt.wantErr {
				t.Errorf("WriteLogs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBoltDB_SetCurrentTerm(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	type args struct {
		term uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			if err := b.SetCurrentTerm(tt.args.term); (err != nil) != tt.wantErr {
				t.Errorf("SetCurrentTerm() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBoltDB_SetVotedFor(t *testing.T) {
	type fields struct {
		filePath string
		db       *bolt.DB
	}
	type args struct {
		candidateID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BoltDB{
				filePath: tt.fields.filePath,
				db:       tt.fields.db,
			}
			if err := b.SetVotedFor(tt.args.candidateID); (err != nil) != tt.wantErr {
				t.Errorf("SetVotedFor() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewBoltDB(t *testing.T) {
	type args struct {
		config BoltDBConfig
	}
	tests := []struct {
		name    string
		args    args
		want    *BoltDB
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewBoltDB(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBoltDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBoltDB() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bytesToUint64(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name string
		args args
		want uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bytesToUint64(tt.args.b); got != tt.want {
				t.Errorf("bytesToUint64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_uint64ToBytes(t *testing.T) {
	type args struct {
		v uint64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := uint64ToBytes(tt.args.v); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("uint64ToBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}
