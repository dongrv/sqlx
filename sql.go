package sqlx

import (
	"context"
	"database/sql"
	"errors"
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
	// TODO
	return true
}

var (
	locker sync.RWMutex
	pool   = make(map[string]*sql.DB) // 数据库连接池
)

// RegisterDB 注册数据库连接
func RegisterDB(dbConfigs map[string]Config) error {
	locker.Lock()
	defer locker.Unlock()
	for s, config := range dbConfigs {
		conn, err := Open(config)
		if err != nil {
			return err
		}
		if err = conn.Ping(); err != nil {
			return err
		}
		pool[s] = conn
	}
	return nil
}

// UnregisterDB 注销数据库连接
func UnregisterDB() {
	locker.Lock()
	defer locker.Unlock()
	for _, db := range pool {
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
	locker.RLock()
	defer locker.RUnlock()
	if p, ok := pool[connName]; ok {
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
	defer func() {
		_ = stat.Close()
	}()

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
