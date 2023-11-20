package sqlx

import (
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

// Config 数据库连接配置
type Config struct {
	DSN          string // 连接信息：用户名:密码@tcp(IP:端口)/表名?timeout=30s&charset=utf8mb4&parseTime=True&loc=Local
	MaxOpenConns int    // 最大连接数
	MaxIdleConns int    // 最大闲置连接数
	MaxLifetime  int64  // 可重复使用的生命周期
	MaxIdleTime  int    // maximum amount of time a connection may be idle before being closed
	Timeout      int64  // 连接超时时间
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
	conn, err := sql.Open(`mysql`, c.DSN)
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

func Get(connName string) (*Conn, error) {
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

func (c *Conn) Insert(query string, args []interface{}) (sql.Result, error) {
	return c.execute(query, args)
}

func (c *Conn) Delete(query string, args []interface{}) (sql.Result, error) {
	return c.execute(query, args)
}

func (c *Conn) Update(query string, args []interface{}) (sql.Result, error) {
	return c.execute(query, args)
}

func (c *Conn) SelectRow(query string, args []interface{}) *sql.Row {
	return c.db.QueryRow(query, args...)
}

// SelectRows 查询多行，调用方需要手动关闭资源句柄 rows.Close()
func (c *Conn) SelectRows(query string, args []interface{}) (*sql.Rows, error) {
	return c.db.Query(query, args...)
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
