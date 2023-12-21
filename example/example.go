package example

import (
	"fmt"
	"sqlx"
)

var configs = map[string]sqlx.Config{
	"game": {
		DSN:          "root:123456@tcp(127.0.0.1:3306)/test?timeout=30s&charset=utf8mb4&parseTime=True&loc=Local",
		MaxLifetime:  100,
		MaxOpenConns: 100,
		MaxIdleConns: 20,
		MaxIdleTime:  3600,
	},
	"log": {
		DSN:          "root:123456@tcp(127.0.0.1:3306)/unity?timeout=30s&charset=utf8mb4&parseTime=True&loc=Local",
		MaxLifetime:  100,
		MaxOpenConns: 100,
		MaxIdleConns: 20,
		MaxIdleTime:  3600,
	},
}

var (
	gameDB = "game" // 游戏服数据库名
	logDB  = "log"  // 日志服数据库名
)

// Profile 表
type Profile struct {
	Id                  int
	FirstName, LastName string
}

func Do() error {
	if err := sqlx.RegisterDB(configs); err != nil {
		return err
	}
	defer sqlx.UnregisterDB()

	conn, err := sqlx.DB(gameDB)
	if err != nil {
		return err
	}
	if err = createRow(conn); err != nil {
		return err
	}

	if err = updateRow(conn); err != nil {
		return err
	}

	if err = deleteRow(conn); err != nil {
		return err
	}

	if err = queryRow(conn); err != nil {
		return err
	}
	if err = queryRows(conn); err != nil {
		return err
	}

	if err = runTx(conn); err != nil {
		return err
	}

	return nil
}

// 创建/插入行
func createRow(conn *sqlx.Conn) error {
	meta := &sqlx.Meta{Op: sqlx.C, Table: "profile", Values: sqlx.KeyValue{"first_name": "foo", "last_name": "bar"}}
	return conn.Do(meta).Err
}

// 更新行
func updateRow(conn *sqlx.Conn) error {
	meta := &sqlx.Meta{
		Op: sqlx.U, Table: "profile",
		Values: sqlx.KeyValue{"first_name": "Tomi", "last_name": "Dog"},
		Where:  sqlx.KeyValue{"id": 1},
	}
	return conn.Do(meta).Err
}

// 删除行
func deleteRow(conn *sqlx.Conn) error {
	meta := &sqlx.Meta{Op: sqlx.D, Table: "profile", Where: sqlx.KeyValue{"id": 101}}
	return conn.Do(meta).Err
}

// 查询单行
func queryRow(conn *sqlx.Conn) error {
	meta := &sqlx.Meta{
		Op: sqlx.R, Table: "profile",
		Query: sqlx.Query{Fields: []string{"first_name", "last_name"}},
		Where: sqlx.KeyValue{"id": 1},
	}
	p := &Profile{}
	done := conn.Do(meta)
	if err := done.Row().Scan(&p.FirstName, &p.LastName); err != nil {
		return err
	}
	fmt.Printf("%s %s\n", p.FirstName, p.LastName)
	return nil
}

// 查询单行
func queryRows(conn *sqlx.Conn) error {
	meta := &sqlx.Meta{
		Op: sqlx.R, Table: "profile",
		Query: sqlx.Query{Fields: []string{"id", "first_name", "last_name"}, Batch: true},
		Where: sqlx.KeyValue{"first_name": "Tony"},
	}

	rows, err := conn.Do(meta).Rows()
	if err != nil {
		return err
	}
	defer sqlx.CloseRows(rows)

	for rows.Next() {
		p := &Profile{}
		if err = rows.Scan(&p.Id, &p.FirstName, &p.LastName); err != nil {
			return err
		}
		fmt.Printf("%d %s %s\n", p.Id, p.FirstName, p.LastName)
	}

	return nil
}

// 运行事务
func runTx(conn *sqlx.Conn) error {
	tx, err := conn.BeginTx()
	if err != nil {
		return err
	}
	defer func() {
		err = tx.Rollback()
	}()

	stat, err := tx.Prepare("UPDATE profile SET  last_name = ? WHERE id=?")
	if err != nil {
		return err
	}
	defer func() {
		_ = stat.Close()
	}()

	_, err = stat.Exec("Bill", 1)
	if err != nil {
		return err
	}
	return tx.Commit()
}
