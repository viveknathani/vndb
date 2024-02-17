package engine

import (
	"os"
	"sync"
)

type Engine interface {
	Get() (string, bool, error)
	Set(string, string) error
	Keys() []string
	Del(string) error
	Exists(string) bool
}

type KeyInfo struct {
	timestamp uint32
	position  uint32
	totalSize uint32
}

type Store struct {
	sync.Mutex
	memory      map[string]KeyInfo
	log         os.File
	maxFileSize uint32
	writeNextAt uint32
}
