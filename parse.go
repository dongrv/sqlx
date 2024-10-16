package sqlx

import (
	"database/sql"
	"fmt"
	"strings"
)

type Operator interface {
	Insert(kv KeyValue) (sql.Result, error)                       // 插入行
	Delete(where KeyValue) (sql.Result, error)                    // 删除行
	Update(update, where KeyValue) (sql.Result, error)            // 更新行
	QueryRow(fields []string, where KeyValue) *sql.Row            // 查询单行
	QueryRows(fields []string, where KeyValue) (*sql.Rows, error) // 查询多行
}

// Table 默认实现
type Table struct{}

func (t *Table) Insert(_ KeyValue) (sql.Result, error)               { return nil, nil }
func (t *Table) Delete(_ KeyValue) (sql.Result, error)               { return nil, nil }
func (t *Table) Update(_ KeyValue, _ KeyValue) (sql.Result, error)   { return nil, nil }
func (t *Table) QueryRow(_ []string, _ KeyValue) *sql.Row            { return nil }
func (t *Table) QueryRows(_ []string, _ KeyValue) (*sql.Rows, error) { return nil, nil }

type KeyValue map[string]interface{} // 格式：map[字段]值

// Split 切割参数返回：字段名字符串、占位符字符串、参数列表
func (kv KeyValue) Split() (string, string, []interface{}) {
	if len(kv) == 0 {
		return "", "", []interface{}{}
	}
	var (
		ps, field string
		args      = make([]interface{}, 0, len(kv))
	)
	for k, v := range kv {
		ps += "?,"
		field += "`" + k + "`,"
		args = append(args, v)
	}
	return field[:len(field)-1], ps[:len(ps)-1], args
}

// SplitWrap 切割参数，返回：字段=?格式字符串，参数列表
func (kv KeyValue) SplitWrap() (string, []interface{}) {
	if len(kv) == 0 {
		return "", []interface{}{}
	}
	var (
		ps   string
		args = make([]interface{}, 0, len(kv))
	)
	for k, v := range kv {
		ps += "`" + k + "`" + "=?,"
		args = append(args, v)
	}
	return ps[:len(ps)-1], args
}

type Fields []string // 提供安全的字段拼接

func (f Fields) Join() string { return "`" + strings.Join(f, "`,`") + "`" }

// FormatString 格式化为完整字符串
func FormatString(query string, args []interface{}) string {
	query = strings.ReplaceAll(query, "?", "%v")
	return fmt.Sprintf(query, args...)
}
