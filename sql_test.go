package sqlx

import (
	"testing"
)

const (
	game = "localhost1"
	log  = "localhost2"
)

var configs = map[string]Config{
	game: {
		DSN:          "root:123456@tcp(127.0.0.1:3306)/test?timeout=30s&charset=utf8mb4&parseTime=True&loc=Local",
		MaxLifetime:  100,
		MaxOpenConns: 100,
		MaxIdleConns: 20,
		MaxIdleTime:  3600,
	},
	log: {
		DSN:          "root:123456@tcp(127.0.0.1:3306)/unity?timeout=30s&charset=utf8mb4&parseTime=True&loc=Local",
		MaxLifetime:  100,
		MaxOpenConns: 100,
		MaxIdleConns: 20,
		MaxIdleTime:  3600,
	},
}

func TestRegisterDB(t *testing.T) {
	if err := RegisterDB(configs); err != nil {
		t.Error(err.Error())
		return
	}
	defer UnregisterDB()

	conn, err := Get(game)
	if err != nil {
		t.Error(err.Error())
		return
	}

	var firstName, lastName string
	row := conn.QueryRow("SELECT first_name,last_name FROM person WHERE id=?", []interface{}{1})
	if err = row.Scan(&firstName, &lastName); err != nil {
		t.Error(err.Error())
		return
	}

	println(firstName, lastName)
}

func TestValidate(t *testing.T) {}

func TestKeyValue_Split(t *testing.T) {
	kv := KeyValue{"Field1": 1, "Field2": "2", "Field3": 0.1}
	f, p, a := kv.Split()
	t.Log(p, f, a)
	f, a = kv.SplitWrap()
	t.Log(f, a)
}
