package wal

import (
	"fmt"
	"os"
	"sync"

	"github.com/Aran404/KV-Database/database/compression"
)

const (
	DEFAULT_WAL_DIR  = "tmp"
	DEFAULT_WAL_PATH = "WAL_LOG.dat"

	TYPE_SET    = "SET"
	TYPE_DELETE = "DELETE"
	TYPE_UPDATE = "UPDATE"

	FILE_FLAGS = os.O_APPEND | os.O_CREATE | os.O_RDWR
)

var (
	walDescriptor *os.File
	walMutex      *sync.Mutex
)

func init() {
	walMutex = &sync.Mutex{}

	if _, err := os.Stat(DEFAULT_WAL_DIR); os.IsNotExist(err) {
		if err := os.Mkdir(DEFAULT_WAL_DIR, 0777); err != nil {
			panic(err)
		}
	}

	if _, err := os.Stat(DEFAULT_WAL_PATH); os.IsNotExist(err) {
		f, err := os.Create(DEFAULT_WAL_PATH)
		if err != nil {
			panic(err)
		}
		f.Close()
	}

	var err error
	walDescriptor, err = os.OpenFile(fmt.Sprintf("%s/%s", DEFAULT_WAL_DIR, DEFAULT_WAL_PATH), FILE_FLAGS, 0666)
	if err != nil {
		panic(err)
	}
}

type LogEntry struct {
	LSN       int64
	OpType    string
	Key       []byte
	Value     []byte
	Timestamp int64
	Checksum  *uint32 // CRC32
}

// In a database there should only be one WAL instance.
type WAL struct {
	Compressor  compression.Compressor
	LSNPostion  int64
	UseChecksum bool
}

// NewWAL creates a new WAL instance
// useChecksum is true by default.
func NewWAL(compressor compression.Compressor, useChecksum ...bool) *WAL {
	cs := true
	if len(useChecksum) > 0 && !useChecksum[0] {
		cs = false
	}
	// Nil compression = no compression
	return &WAL{Compressor: compressor, LSNPostion: 0, UseChecksum: cs}
}
