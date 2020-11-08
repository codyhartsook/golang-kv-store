package db

import (
	"strconv"
	"testing"
)

// 01
func TestPut(t *testing.T) {
	db := NewDB()

	scenarios := []struct {
		key    string
		value  string
		expect error
	}{
		{key: "key0", value: "value0", expect: nil},
		{key: "keytemp", value: "", expect: nil},
	}

	for _, s := range scenarios {
		got := db.Put(s.key, s.value)
		if got != s.expect {
			t.Errorf("Did not get expected result for input '%v', '%v'. Expected %q, got %q", s.key, s.value, s.expect, got)
		}
	}

}

// 02
func TestGet(t *testing.T) {
	db := NewDB()

	scenarios := []struct {
		key    string
		value  string
		expect string
	}{
		{key: "key0", value: "value0", expect: "value0"},
		{key: "keytemp", value: "", expect: ""},
	}

	for _, s := range scenarios {
		db.Put(s.key, s.value)
	}

	for _, s := range scenarios {
		got, _ := db.Get(s.key)
		value := string(got)
		if value != s.expect {
			t.Errorf("Did not get expected result for input '%v', '%v'. Expected %q, got %q", s.key, s.value, s.expect, got)
		}
	}
}

// 03
func TestContentConvertion(t *testing.T) {
	db := NewDB()

	var baseKey string
	var baseVal string
	baseKey = "key"
	baseVal = "val"
	expect := make(map[string]string)

	for i := 0; i < 10; i++ {
		db.Put(baseKey, baseVal)
		expect[baseKey] = baseVal

		baseKey = (baseKey + strconv.Itoa(i))
		baseVal = (baseVal + strconv.Itoa(i))
	}

	contentBytes, _ := db.ToByteArray()
	contentMap, _ := db.ByteArrayToMap(contentBytes)

	for k, v := range contentMap {
		val, ok := expect[k]

		if !ok {
			t.Errorf("Key does not exist in converted store: '%v' -> '%v'", k, v)
		}

		if val != v {
			t.Errorf("Values do not match: Expected '%v', got '%v'", val, v)
		}
	}
}
