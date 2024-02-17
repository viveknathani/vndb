package engine

import (
	"encoding/binary"
	"io"
	"time"
)

// below is the structure of a row in our log, along with the size it would take.
// +-----------+---------+-----------+-----------+----------+----------+
// | timestamp | keySize | valueSize | tombstone |   key    |  value   |
// +-----------+---------+-----------+-----------+----------+----------+
// | 4 bytes   | 4 bytes | 4 bytes   | 1 byte    | variable | variable |
// +-----------+---------+-----------+-----------+----------+----------+
//
//
//
//

const headerSize int = 12
const TOMBSTONE = true

func encodeKV(key string, value string, tombstone bool) (uint32, []byte) {
	currentTimestamp := uint32(time.Now().Unix())
	data := make([]byte, headerSize)
	binary.BigEndian.PutUint32(data[0:4], currentTimestamp)
	binary.BigEndian.PutUint32(data[4:8], uint32(len(key)))
	binary.BigEndian.PutUint32(data[4:12], uint32(len(value)))
	var tombstoneByte int8 = 0
	if tombstone {
		tombstoneByte = 1
	}
	data = append(data, byte(tombstoneByte))
	data = append(data, []byte(key)...)
	data = append(data, []byte(value)...)
	return currentTimestamp, data
}

func decodeKV(key string, data []byte) (bool, string) {
	keySize := binary.BigEndian.Uint32(data[4:8])
	valueSize := binary.BigEndian.Uint32(data[8:12])
	tombstone := int8(data[12])
	value := string(data[uint32(keySize)+keySize : uint32(keySize)+keySize+valueSize])
	return tombstone == 1, value
}

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

func (s *Store) Get(key string, value string) (string, bool, error) {
	s.Lock()
	defer s.Unlock()
	info, exists := s.memory[key]
	if !exists {
		return "", false, nil
	}
	_, err := s.log.Seek(int64(info.position), io.SeekStart)
	if err != nil {
		return "", false, err
	}
	s.log.Seek(int64(s.writeNextAt), io.SeekStart)
	data := make([]byte, info.totalSize)
	tombstone, value := decodeKV(key, data)
	return value, tombstone, nil
}

func (s *Store) Keys() []string {
	keys := make([]string, len(s.memory))
	i := 0
	for key := range s.memory {
		keys[i] = key
		i++
	}
	return keys
}

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

func (s *Store) Exists(key string) bool {
	s.Lock()
	defer s.Unlock()
	_, exists := s.memory[key]
	return exists
}
