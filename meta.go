package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrResultNil   = errors.New("sqlx:result is nil")
	ErrInvalidCurd = errors.New("sqlx:invalid operation")
)

type Curd uint8 // 操作码：新增、更新、查询、删除

const (
	C Curd = iota // Create
	U             // Update
	R             // Retrieve
	D             // Delete
)

// Query 查询选项
type Query struct {
	Fields []string // 查询字段
	Batch  bool     // 是否批量查询
}

type OptionFunc func(*Meta)

// Meta 执行单元
type Meta struct {
	Op            Curd     // 操作：增改查删
	Table         string   // 表名
	Query         Query    // 查询字段
	Values, Where KeyValue // 更新map，查询条件map
}

func WrapOp(curd Curd) OptionFunc {
	return func(m *Meta) {
		m.Op = curd
	}
}

func WrapTable(tableName string) OptionFunc {
	return func(m *Meta) {
		m.Table = tableName
	}
}

func WrapQuery(fields []string, batch bool) OptionFunc {
	return func(m *Meta) {
		m.Query.Fields = fields
		m.Query.Batch = batch
	}
}

func WrapValues(kv KeyValue) OptionFunc {
	return func(m *Meta) {
		m.Values = kv
	}
}

func WrapWhere(kv KeyValue) OptionFunc {
	return func(m *Meta) {
		m.Where = kv
	}
}

func NewMeta(fs ...OptionFunc) *Meta {
	m := &Meta{Query: Query{}}
	for _, f := range fs {
		f(m)
	}
	return m
}

// Do 执行
func (m *Meta) Do(conn *Conn) (done Done) {
	t := time.Now()
	defer func() { done.Runtime = time.Since(t).Seconds() }()

	switch m.Op {
	case C:
		fields, pd, args := m.Values.Split()
		done.result, done.Err = conn.Create(fmt.Sprintf(rawInsert, m.Table, fields, pd), args)
		return
	case U:
		fields, args := m.Values.SplitWrap()
		where, whereArgs := m.Where.SplitWrap()
		merge := append(args, whereArgs...)
		done.result, done.Err = conn.Update(fmt.Sprintf(rawUpdate, m.Table, fields, where), merge)
		return
	case R:
		var search = `*`
		fields, args := m.Where.SplitWrap()
		if len(m.Query.Fields) > 0 {
			search = strings.Join(m.Query.Fields, `,`)
		}
		if m.Query.Batch {
			done.rows, done.Err = conn.QueryRows(fmt.Sprintf(rawQuery, search, m.Table, fields), args)
			return
		}
		done.row = conn.QueryRow(fmt.Sprintf(rawQuery, search, m.Table, fields), args)
		return
	case D:
		field, args := m.Where.SplitWrap()
		done.result, done.Err = conn.Delete(fmt.Sprintf(rawDelete, m.Table, field), args)
		return
	}
	done.Err = ErrInvalidCurd
	return
}

// Done 执行结果
type Done struct {
	result  sql.Result
	row     *sql.Row
	rows    *sql.Rows
	Err     error
	Runtime float64 // 运行时间：秒
}

func (d Done) LastInsertId() (int64, error) {
	if d.result != nil {
		return d.result.LastInsertId()
	}
	return -1, ErrResultNil
}

func (d Done) RowsAffected() (int64, error) {
	if d.result != nil {
		return d.result.RowsAffected()
	}
	return -1, ErrResultNil
}

func (d Done) Row() *sql.Row {
	return d.row
}

func (d Done) Rows() (*sql.Rows, error) {
	return d.rows, d.Err
}

func (d Done) CloseRows() {
	if d.rows != nil {
		_ = d.rows.Close()
	}
}

func (d Done) Error() error {
	return d.Err
}

// SQL语句模板
const (
	rawInsert   = `INSERT INTO %s(%s)VALUES(%s)`
	rawUpdate   = `UPDATE %s SET %s WHERE %s`
	rawDelete   = `DELETE FROM %s WHERE %s`
	rawQuery    = `SELECT %s FROM %s WHERE %s` // 查询指定字段
	rawQueryAll = `SELECT * FROM %s WHERE %s`  // 查询所有字段
)
