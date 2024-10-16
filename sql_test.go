package sqlx

import (
	"testing"
)

const (
	game = "localhost1"
)

var configs = ConfigMap{
	game: {
		DSN:          "root:123456@tcp(127.0.0.1:3306)/test?timeout=30s&charset=utf8mb4&parseTime=True&loc=Local",
		MaxLifetime:  100,
		MaxOpenConns: 100,
		MaxIdleConns: 20,
		MaxIdleTime:  3600,
	},
}

func TestRegisterDB(t *testing.T) {
	if err := New(configs); err != nil {
		t.Fatal(err)
		return
	}
	defer CloseAll()

	conn, err := DB(game)
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.Ping(); err != nil {
		t.Fatal(err)
	}
}
