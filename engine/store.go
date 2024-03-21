package engine

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"time"
)

// below is the structure of a row in our log, along with the size it would take.
// +-----------+---------+-----------+-----------+----------+----------+
// | timestamp | keySize | valueSize | tombstone |   key    |  value   |
// +-----------+---------+-----------+-----------+----------+----------+
// | 4 bytes   | 4 bytes | 4 bytes   | 1 byte    | variable | variable |
// +-----------+---------+-----------+-----------+----------+----------+

const MAX_FILE_SIZE = 4000000000
const HEADER_SIZE int = 13
const TOMBSTONE = true

func encodeKV(key string, value string, tombstone bool) (uint32, []byte) {
	currentTimestamp := uint32(time.Now().Unix())
	data := make([]byte, HEADER_SIZE)
	binary.BigEndian.PutUint32(data[0:4], currentTimestamp)
	binary.BigEndian.PutUint32(data[4:8], uint32(len(key)))
	binary.BigEndian.PutUint32(data[8:12], uint32(len(value)))
	var tombstoneByte int8 = 0
	if tombstone {
		tombstoneByte = 1
	}
	data[12] = byte(tombstoneByte)
	data = append(data, []byte(key)...)
	data = append(data, []byte(value)...)
	return currentTimestamp, data
}

func decodeKV(key string, data []byte) (bool, string, string) {
	keySize := binary.BigEndian.Uint32(data[4:8])
	valueSize := binary.BigEndian.Uint32(data[8:12])
	tombstone := int8(data[12])
	value := string(data[uint32(HEADER_SIZE)+keySize : uint32(HEADER_SIZE)+keySize+valueSize])
	return tombstone == 1, key, value
}

// Set sets a key-value pair.
func (s *Store) Set(key string, value string) error {
	s.Lock()
	defer s.Unlock()
	currentTimestamp, data := encodeKV(key, value, !TOMBSTONE)
	totalSize, err := s.log.Write(data)
	if err != nil {
		return err
	}
	s.memory[key] = KeyInfo{
		timestamp: currentTimestamp,
		position:  s.writeNextAt,
		totalSize: uint32(totalSize),
	}
	s.writeNextAt += uint32(totalSize)
	return nil
}

// Get returns the value for a given key if it exists.
func (s *Store) Get(key string) (string, bool, error) {
	s.Lock()
	defer s.Unlock()
	info, exists := s.memory[key]
	if !exists {
		return "", true, nil
	}
	_, err := s.log.Seek(int64(info.position), io.SeekStart)
	if err != nil {
		return "", false, err
	}
	s.log.Seek(int64(info.position), io.SeekStart)
	data := make([]byte, info.totalSize)
	_, err = io.ReadFull(&s.log, data)
	if err != nil {
		return "", false, err
	}
	tombstone, key, value := decodeKV(key, data)
	return value, tombstone, nil
}

// Keys will list all keys.
func (s *Store) Keys() []string {
	keys := make([]string, len(s.memory))
	i := 0
	for key := range s.memory {
		keys[i] = key
		i++
	}
	return keys
}

// Del deletes the key.
func (s *Store) Del(key string) error {
	s.Lock()
	defer s.Unlock()
	_, data := encodeKV(key, "", TOMBSTONE)
	totalSize, err := s.log.Write(data)
	if err != nil {
		return err
	}
	delete(s.memory, key)
	s.writeNextAt += uint32(totalSize)
	return nil
}

// Exists tells you if the key exists.
func (s *Store) Exists(key string) bool {
	s.Lock()
	defer s.Unlock()
	_, exists := s.memory[key]
	return exists
}

func (s *Store) initMemory(filePath string) {
	file, _ := os.Open(filePath)
	defer file.Close()
	for {
		header := make([]byte, HEADER_SIZE)
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		timestamp := binary.BigEndian.Uint32(header[0:4])
		keySize := binary.BigEndian.Uint32(header[4:8])
		valueSize := binary.BigEndian.Uint32(header[8:12])
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		_, err = io.ReadFull(file, key)
		if err != nil {
			log.Fatal(err)
		}
		_, err = io.ReadFull(file, value)
		if err != nil {
			log.Fatal(err)
		}
		totalSize := uint32(HEADER_SIZE) + keySize + valueSize
		s.memory[string(key)] = KeyInfo{
			timestamp: timestamp,
			position:  s.writeNextAt,
			totalSize: uint32(totalSize),
		}
		s.writeNextAt += totalSize
	}
}

// NewStore returns a new store.
func NewStore(filePath string) *Store {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatal(err)
	}
	var mp map[string]KeyInfo = map[string]KeyInfo{}
	store := &Store{
		memory:      mp,
		log:         *file,
		maxFileSize: MAX_FILE_SIZE,
		writeNextAt: 0,
	}
	store.initMemory(filePath)
	return store
}
