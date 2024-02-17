package engine

import (
	"log"
	"testing"
)

var testCases = []struct {
	key   string
	value string
}{
	{key: "hello", value: "5"},
	{key: "hey", value: "56"},
	{key: "heybro", value: "5622"},
}

func TestGetAndSet(t *testing.T) {

	mp := NewStore("../data/vndb.data")
	for _, testCase := range testCases {

		err := mp.Set(testCase.key, testCase.value)
		if err != nil {
			log.Fatal()
		}

		v, tombstone, err := mp.Get(testCase.key)
		if err != nil {
			log.Fatal(err)
		}

		if tombstone || v != testCase.value {
			log.Fatalf("failed Get at %v, got: %v", testCase, v)
		}
	}
}

func TestDel(t *testing.T) {

	mp := NewStore("../data/vndb.data")
	err := mp.Set("TO_DELETE", "SOME_VALUE")
	if err != nil {
		log.Fatal()
	}
	err = mp.Del("TO_DELETE")
	if err != nil {
		log.Fatal()
	}
	v, tombstone, err := mp.Get("TO_DELETE")
	if err != nil {
		log.Fatal(err)
	}

	if !tombstone {
		log.Fatalf("failed to delete %s", v)
	}
}

func TestExists(t *testing.T) {

	mp := NewStore("../data/vndb.data")
	err := mp.Set("CHECK_EXISTS", "SOME_VALUE")
	if err != nil {
		log.Fatal()
	}
	if !mp.Exists("CHECK_EXISTS") {
		log.Fatal("check exists failed")
	}
	if mp.Exists("CHECK_DOES_NOT_EXIST") {
		log.Fatal("check does not exist failed")
	}
}
