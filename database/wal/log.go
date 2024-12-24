package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

func (l *LogEntry) ComputeChecksum() uint32 {
	data := append([]byte(l.OpType), l.Key...)
	data = append(data, l.Value...)
	return crc32.ChecksumIEEE(data)
}

// LogSet writes a log entry to the WAL with TYPE_SET
func (w *WAL) LogSet(key, value []byte) error {
	entry := &LogEntry{
		OpType:    TYPE_SET,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixMilli(),
	}
	return w.WriteLog(entry)
}

// LogDelete writes a log entry to the WAL with TYPE_DELETE
func (w *WAL) LogDelete(key, value []byte) error {
	entry := &LogEntry{
		OpType:    TYPE_DELETE,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixMilli(),
	}
	return w.WriteLog(entry)
}

// LogUpdate writes a log entry to the WAL with TYPE_UPDATE
// Although a KV store usually doesn't have an update operation, we will interpret this later.
func (w *WAL) LogUpdate(key, value []byte) error {
	entry := &LogEntry{
		OpType:    TYPE_UPDATE,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixMilli(),
	}
	return w.WriteLog(entry)
}

// WriteLog writes a log entry to the WAL Descriptor
// It immediately flushes the WAL Descriptor for durability.
func (w *WAL) WriteLog(entry *LogEntry) error {
	walMutex.Lock()
	defer walMutex.Unlock()

	if w.UseChecksum {
		cs := entry.ComputeChecksum()
		entry.Checksum = &cs
	}

	w.LSNPostion += 1
	entry.LSN = w.LSNPostion

	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(entry); err != nil {
		return fmt.Errorf("failed to encode log entry: %w", err)
	}

	raw := buffer.Bytes()
	if w.Compressor != nil {
		compressed, err := w.Compressor.Compress(buffer.Bytes())
		if err != nil {
			return fmt.Errorf("failed to compress log entry: %w", err)
		}
		raw = compressed
	}

	sizeHeader := make([]byte, 4) // 32 bits
	binary.BigEndian.PutUint32(sizeHeader, uint32(len(raw)))

	if _, err := walDescriptor.Write(append(sizeHeader, raw...)); err != nil {
		return err
	}
	return w.Flush()
}

// Seek seeks the WAL Descriptor to the beginning.
func (w *WAL) Seek() error {
	walMutex.Lock()
	defer walMutex.Unlock()

	if _, err := walDescriptor.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return nil
}

// ReadNextLog reads the next log entry from the WAL Descriptor.
func (w *WAL) ReadNextLog() (*LogEntry, error) {
	walMutex.Lock()
	defer walMutex.Unlock()

	sizeBuf := make([]byte, 4)
	if _, err := walDescriptor.Read(sizeBuf); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	entrySize := int(binary.BigEndian.Uint32(sizeBuf))
	compressedData := make([]byte, entrySize)
	if _, err := walDescriptor.Read(compressedData); err != nil {
		return nil, err
	}

	decompressedData, err := w.Compressor.Decompress(compressedData)
	if err != nil {
		return nil, err
	}

	var entry LogEntry
	decoder := gob.NewDecoder(bytes.NewReader(decompressedData))
	if err := decoder.Decode(&entry); err != nil {
		return nil, err
	}

	// Malformed Data
	if w.UseChecksum && entry.Checksum != nil && *entry.Checksum != entry.ComputeChecksum() {
		return nil, fmt.Errorf("integrity check failed")
	}

	return &entry, nil
}

// ReadAllLogs reads all log entries from the WAL Descriptor.
func (w *WAL) ReadAllLogs() ([]*LogEntry, error) {
	var entries = make([]*LogEntry, 0)
	for {
		entry, err := w.ReadNextLog()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// ClearLogs clears the WAL Descriptor
// If flush is true, it also flushes the WAL Descriptor
func (w *WAL) ClearLogs(flush bool) error {
	walMutex.Lock()
	defer walMutex.Unlock()

	if err := walDescriptor.Truncate(0); err != nil {
		return err
	}
	if flush {
		return w.Flush()
	}
	return nil
}

// Flush flushes the WAL Descriptor
// This does not hold the Mutex.
func (w *WAL) Flush() error {
	return walDescriptor.Sync()
}
