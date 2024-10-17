package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/dongrv/trace"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"time"
)

var (
	ErrUnregister = errors.New("unregistered database connection")
	ErrConnName   = errors.New("connection is not registered")
)

const driverDB = `mysql`

// Config 数据库连接配置
type Config struct {
	DSN          string // 连接信息：用户名:密码@tcp(IP:端口)/表名?timeout=30s&charset=utf8mb4&parseTime=True&loc=Local
	MaxOpenConns int    // 最大连接数
	MaxIdleConns int    // 最大闲置连接数
	MaxLifetime  int64  // 可重复使用的生命周期
	MaxIdleTime  int    // maximum amount of time a connection may be idle before being closed
}

func (c Config) Validate() bool {
	return c.DSN != "" && c.MaxOpenConns >= 0 && c.MaxIdleConns >= 0 && c.MaxLifetime >= 0 && c.MaxIdleTime >= 0
}

var (
	poolLock sync.RWMutex
	connPool = make(map[string]*sql.DB) // 数据库连接池
)

// New 注册数据库连接
func New(configs ConfigMap) error {
	poolLock.Lock()
	defer poolLock.Unlock()

	if !configs.Validate() {
		return errors.New("incorrect config parameters")
	}

	for connName, config := range configs {
		conn, err := Open(config)
		if err != nil {
			return err
		}
		if err = conn.Ping(); err != nil {
			return err
		}
		connPool[connName] = conn
	}
	return nil
}

// CloseAll 关闭所有连接
func CloseAll() {
	poolLock.Lock()
	defer poolLock.Unlock()
	for _, db := range connPool {
		_ = Close(db)
	}
}

func Open(c Config) (*sql.DB, error) {
	conn, err := sql.Open(driverDB, c.DSN)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(c.MaxOpenConns)
	conn.SetMaxIdleConns(c.MaxIdleConns)
	conn.SetConnMaxLifetime(time.Duration(c.MaxLifetime))
	conn.SetConnMaxIdleTime(time.Duration(c.MaxIdleTime))
	return conn, nil
}

func Close(db *sql.DB) error {
	if db != nil {
		err := db.Close()
		db = nil
		return err
	}
	return nil
}

func DB(connName string) (*Conn, error) {
	if connName == "" {
		return nil, ErrConnName
	}
	poolLock.RLock()
	defer poolLock.RUnlock()
	if p, ok := connPool[connName]; ok {
		return &Conn{db: p}, nil
	}
	return nil, ErrUnregister
}

type Conn struct {
	db *sql.DB
}

func (c *Conn) execute(query string, args []interface{}) (sql.Result, error) {
	stat, err := c.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stat.Close() }()

	return stat.Exec(args...)
}

// Create 创建/插入行
func (c *Conn) Create(query string, args []interface{}) (sql.Result, error) {
	return c.execute(query, args)
}

// Delete 删除
func (c *Conn) Delete(query string, args []interface{}) (sql.Result, error) {
	return c.execute(query, args)
}

// Update 更新
func (c *Conn) Update(query string, args []interface{}) (sql.Result, error) {
	return c.execute(query, args)
}

// QueryRow 查询行
func (c *Conn) QueryRow(query string, args []interface{}) *sql.Row {
	return c.db.QueryRow(query, args...)
}

// QueryRows 查询多行，调用方需要手动关闭资源句柄 rows.Close()
func (c *Conn) QueryRows(query string, args []interface{}) (*sql.Rows, error) {
	return c.db.Query(query, args...)
}

// Do 执行SQL
func (c *Conn) Do(m *Meta) Done {
	return m.Do(c)
}

// Ping ping check
func (c *Conn) Ping() error {
	return c.db.Ping()
}

// Stats 连接状态数据
func (c *Conn) Stats() sql.DBStats {
	return c.db.Stats()
}

// Close 关闭连接
func (c *Conn) Close() error {
	return Close(c.db)
}

// BeginTx 启动带上下文的事务
func (c *Conn) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return c.db.BeginTx(func() context.Context {
		if ctx == nil {
			ctx = context.Background()
		}
		return ctx
	}(), nil)
}

// ExecTx 执行完整事务
func (c *Conn) ExecTx(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tx, err := c.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	stat, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer func() { err = stat.Close() }()

	result, err := stat.Exec(args...)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	return result, err
}

type Tx struct {
	Query string
	Args  []interface{}
}

// ExecBatchTx 批量执行完整事务
func (c *Conn) ExecBatchTx(ctx context.Context, txs ...Tx) (sql.Result, error) {
	tx, err := c.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if len(txs) == 0 {
		return nil, errors.New("empty submit tx")
	}

	var (
		result sql.Result
		stat   *sql.Stmt
	)

	for _, t := range txs {
		stat, err = tx.Prepare(t.Query)
		if err != nil {
			return nil, err
		}

		result, err = stat.Exec(t.Args...)
		if err != nil {
			return nil, err
		}

		_ = stat.Close()
	}

	err = tx.Commit()
	return result, err
}

//-------------------------------------------------
//            Convenient call method              |
//-------------------------------------------------

func (c *Conn) TraceExec(ctx *trace.Context, query string, args []interface{}) (result sql.Result, err error) {
	var (
		rowsAffected int64
		newCtx       *trace.Context
	)
	if ctx != nil {
		newCtx = ctx.New(driverDB).Set(query, args)
		defer func() {
			detail(newCtx, query, args, err)
			newCtx.SetKV("rows-affected", rowsAffected).Stop()
		}()
	}
	result, err = c.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return nil, err
	}
	return result, err
}

func (c *Conn) TraceInsert(ctx *trace.Context, query string, args []interface{}) (sql.Result, error) {
	return c.TraceExec(ctx, query, args)
}

func (c *Conn) TraceDelete(ctx *trace.Context, query string, args []interface{}) (sql.Result, error) {
	return c.TraceExec(ctx, query, args)
}

func (c *Conn) TraceUpdate(ctx *trace.Context, query string, args []interface{}) (result sql.Result, err error) {
	return c.TraceExec(ctx, query, args)
}

func (c *Conn) TraceSelect(ctx *trace.Context, query string, args []interface{}) *sql.Row {
	var (
		err    error
		newCtx *trace.Context
	)
	if ctx != nil {
		newCtx = ctx.New(driverDB).Set(query, args)
		defer func() {
			detail(newCtx, query, args, err)
			newCtx.Stop()
		}()
	}
	row := c.db.QueryRow(query, args...)
	if row == nil {
		return &sql.Row{}
	}
	err = row.Err()
	return row
}

// TraceSelectBatch 批量查询，注意：调用方需要手动释放资源：defer rows.Close()
func (c *Conn) TraceSelectBatch(ctx *trace.Context, query string, args []interface{}) *sql.Rows {
	var (
		err    error
		newCtx *trace.Context
		rows   *sql.Rows
	)
	if ctx != nil {
		newCtx = ctx.New(driverDB).Set(query, args)
		defer func() {
			detail(newCtx, query, args, err)
			newCtx.Stop()
		}()
	}
	rows, err = c.db.Query(query, args...)
	if err != nil || rows == nil {
		return nil
	}
	err = rows.Err()
	return rows
}

func (c *Conn) EasyInsert(ctx *trace.Context, table string, kv KeyValue) (sql.Result, error) {
	if len(kv) == 0 {
		return nil, errors.New("invalid insert values")
	}
	fields, placeholders, args := kv.Split()
	return c.TraceExec(ctx, fmt.Sprintf(rawInsert, table, fields, placeholders), args)
}

func (c *Conn) EasyDelete(ctx *trace.Context, table string, where KeyValue) (sql.Result, error) {
	if len(where) == 0 {
		return nil, errors.New("invalid delete condition")
	}
	fields, args := where.SplitWrap()
	return c.TraceExec(ctx, fmt.Sprintf(rawDelete, table, fields), args)
}

func (c *Conn) EasyUpdate(ctx *trace.Context, table string, kv, where KeyValue) (sql.Result, error) {
	if len(kv) == 0 || len(where) == 0 {
		return nil, errors.New("invalid update KeyValue")
	}
	setFields, setArgs := kv.SplitWrap()
	whereFields, whereArgs := where.SplitWrap()
	setArgs = append(setArgs, whereArgs...)
	return c.TraceUpdate(ctx, fmt.Sprintf(rawUpdate, table, setFields, whereFields), setArgs)
}

func (c *Conn) EasySelect(ctx *trace.Context, table string, fields []string, where KeyValue) *sql.Row {
	if len(fields) == 0 || len(where) == 0 {
		return &sql.Row{} // 保证非空指针
	}
	whereFields, whereArgs := where.SplitWrap()
	query := fmt.Sprintf(rawQuery, Fields(fields).Join(), table, whereFields)
	return c.TraceSelect(ctx, query, whereArgs)
}

// EasySelectBatch 批量查询，注意：调用方需要手动释放defer rows.Close()
func (c *Conn) EasySelectBatch(ctx *trace.Context, table string, fields []string, where KeyValue) *sql.Rows {
	if len(fields) == 0 {
		return nil
	}
	fieldsW, args := where.SplitWrap()
	query := fmt.Sprintf(rawQuery, Fields(fields).Join(), table, fieldsW)
	return c.TraceSelectBatch(ctx, query, args)
}

func detail(ctx *trace.Context, query string, args []interface{}, err error) {
	if ctx == nil || err == nil {
		return
	}
	ctx.SetKV("err", err.Error()).SetKV("sql", FormatString(query, args))
}
