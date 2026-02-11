package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Error definitions following Go standard library conventions.
var (
	// ErrUnregistered indicates the requested database connection is not registered.
	ErrUnregistered = errors.New("sqlx: unregistered database connection")

	// ErrInvalidConfig indicates the configuration is invalid.
	ErrInvalidConfig = errors.New("sqlx: invalid configuration")

	// ErrEmptyConnectionName indicates an empty connection name was provided.
	ErrEmptyConnectionName = errors.New("sqlx: empty connection name")

	// ErrNoRows indicates no rows were returned from a query.
	ErrNoRows = errors.New("sqlx: no rows in result set")

	// ErrInvalidQuery indicates the query is invalid.
	ErrInvalidQuery = errors.New("sqlx: invalid query")

	// ErrInvalidArguments indicates the arguments are invalid.
	ErrInvalidArguments = errors.New("sqlx: invalid arguments")

	// ErrConnectionClosed indicates the connection is closed.
	ErrConnectionClosed = errors.New("sqlx: connection closed")

	// ErrPoolClosed indicates the connection pool is closed.
	ErrPoolClosed = errors.New("sqlx: connection pool closed")

	// ErrDuplicateEntry indicates a duplicate entry was attempted.
	ErrDuplicateEntry = errors.New("sqlx: duplicate entry")

	// ErrTimeout indicates a timeout occurred.
	ErrTimeout = errors.New("sqlx: timeout")

	// ErrTransactionFailed indicates a transaction failed.
	ErrTransactionFailed = errors.New("sqlx: transaction failed")

	// ErrInvalidOperation indicates an invalid operation was attempted.
	ErrInvalidOperation = errors.New("sqlx: invalid operation")

	// ErrMultipleRows indicates multiple rows were returned when only one was expected.
	ErrMultipleRows = errors.New("sqlx: multiple rows returned when only one was expected")

	// ErrPrepareFailed indicates preparing a statement failed.
	ErrPrepareFailed = errors.New("sqlx: prepare statement failed")

	// ErrExecFailed indicates executing a statement failed.
	ErrExecFailed = errors.New("sqlx: execute statement failed")

	// ErrQueryFailed indicates querying failed.
	ErrQueryFailed = errors.New("sqlx: query failed")

	// ErrBeginTxFailed indicates beginning a transaction failed.
	ErrBeginTxFailed = errors.New("sqlx: begin transaction failed")

	// ErrCommitFailed indicates committing a transaction failed.
	ErrCommitFailed = errors.New("sqlx: commit transaction failed")

	// ErrRollbackFailed indicates rolling back a transaction failed.
	ErrRollbackFailed = errors.New("sqlx: rollback failed")

	// ErrDriverNotSupported indicates the database driver is not supported.
	ErrDriverNotSupported = errors.New("sqlx: database driver not supported")

	// ErrContextCancelled indicates the context was cancelled.
	ErrContextCancelled = errors.New("sqlx: context cancelled")

	// ErrInvalidDataType indicates an invalid data type was provided.
	ErrInvalidDataType = errors.New("sqlx: invalid data type")

	// ErrInvalidTableName indicates an invalid table name was provided.
	ErrInvalidTableName = errors.New("sqlx: invalid table name")

	// ErrInvalidColumnName indicates an invalid column name was provided.
	ErrInvalidColumnName = errors.New("sqlx: invalid column name")

	// ErrInvalidIdentifier indicates an invalid SQL identifier was provided.
	ErrInvalidIdentifier = errors.New("sqlx: invalid identifier")

	// ErrForeignKeyViolation indicates a foreign key constraint violation.
	ErrForeignKeyViolation = errors.New("sqlx: foreign key constraint violation")

	// ErrNotNullViolation indicates a NOT NULL constraint violation.
	ErrNotNullViolation = errors.New("sqlx: not null constraint violation")

	// ErrCheckViolation indicates a CHECK constraint violation.
	ErrCheckViolation = errors.New("sqlx: check constraint violation")

	// ErrUniqueViolation indicates a UNIQUE constraint violation.
	ErrUniqueViolation = errors.New("sqlx: unique constraint violation")
)

// Is checks if the error is of a specific type.
// It wraps errors.Is from the standard library for consistent error checking.
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// Driver represents a database driver.
type Driver string

const (
	// MySQL driver.
	MySQL Driver = "mysql"
	// PostgreSQL driver.
	PostgreSQL Driver = "postgres"
	// SQLite driver.
	SQLite Driver = "sqlite3"
)

// Config holds database connection configuration.
type Config struct {
	// Driver is the database driver (e.g., "mysql", "postgres").
	Driver Driver

	// DSN is the Data Source Name.
	// Format for MySQL: username:password@tcp(host:port)/dbname?params
	DSN string

	// MaxOpenConns is the maximum number of open connections to the database.
	// Zero means unlimited.
	MaxOpenConns int

	// MaxIdleConns is the maximum number of connections in the idle connection pool.
	// Zero means no idle connections are retained.
	MaxIdleConns int

	// ConnMaxLifetime is the maximum amount of time a connection may be reused.
	// Zero means connections are reused forever.
	ConnMaxLifetime time.Duration

	// ConnMaxIdleTime is the maximum amount of time a connection may be idle.
	// Zero means connections are not closed due to idle time.
	ConnMaxIdleTime time.Duration

	// PingTimeout is the timeout for ping operations.
	// Zero means no timeout.
	PingTimeout time.Duration

	// QueryTimeout is the default timeout for queries.
	// Zero means no timeout.
	QueryTimeout time.Duration

	// TransactionTimeout is the default timeout for transactions.
	// Zero means no timeout.
	TransactionTimeout time.Duration

	// MaxRetries is the maximum number of retries for failed operations.
	MaxRetries int

	// RetryDelay is the delay between retries.
	RetryDelay time.Duration
}

// DefaultConfig returns a default configuration for MySQL.
func DefaultConfig() Config {
	return Config{
		Driver:             MySQL,
		MaxOpenConns:       10,
		MaxIdleConns:       5,
		ConnMaxLifetime:    30 * time.Minute,
		ConnMaxIdleTime:    5 * time.Minute,
		PingTimeout:        5 * time.Second,
		QueryTimeout:       30 * time.Second,
		TransactionTimeout: 60 * time.Second,
		MaxRetries:         3,
		RetryDelay:         100 * time.Millisecond,
	}
}

// Validate validates the configuration.
func (c Config) Validate() error {
	if c.Driver == "" {
		return fmt.Errorf("%w: driver is required", ErrInvalidConfig)
	}

	if c.DSN == "" {
		return fmt.Errorf("%w: DSN is required", ErrInvalidConfig)
	}

	if c.MaxOpenConns < 0 {
		return fmt.Errorf("%w: MaxOpenConns must be >= 0", ErrInvalidConfig)
	}

	if c.MaxIdleConns < 0 {
		return fmt.Errorf("%w: MaxIdleConns must be >= 0", ErrInvalidConfig)
	}

	if c.MaxIdleConns > c.MaxOpenConns && c.MaxOpenConns > 0 {
		return fmt.Errorf("%w: MaxIdleConns cannot exceed MaxOpenConns", ErrInvalidConfig)
	}

	if c.ConnMaxLifetime < 0 {
		return fmt.Errorf("%w: ConnMaxLifetime must be >= 0", ErrInvalidConfig)
	}

	if c.ConnMaxIdleTime < 0 {
		return fmt.Errorf("%w: ConnMaxIdleTime must be >= 0", ErrInvalidConfig)
	}

	if c.PingTimeout < 0 {
		return fmt.Errorf("%w: PingTimeout must be >= 0", ErrInvalidConfig)
	}

	if c.QueryTimeout < 0 {
		return fmt.Errorf("%w: QueryTimeout must be >= 0", ErrInvalidConfig)
	}

	if c.TransactionTimeout < 0 {
		return fmt.Errorf("%w: TransactionTimeout must be >= 0", ErrInvalidConfig)
	}

	if c.MaxRetries < 0 {
		return fmt.Errorf("%w: MaxRetries must be >= 0", ErrInvalidConfig)
	}

	if c.RetryDelay < 0 {
		return fmt.Errorf("%w: RetryDelay must be >= 0", ErrInvalidConfig)
	}

	return nil
}

// WithDSN returns a copy of the config with the given DSN.
func (c Config) WithDSN(dsn string) Config {
	c.DSN = dsn
	return c
}

// WithDriver returns a copy of the config with the given driver.
func (c Config) WithDriver(driver Driver) Config {
	c.Driver = driver
	return c
}

// WithMaxOpenConns returns a copy of the config with the given MaxOpenConns.
func (c Config) WithMaxOpenConns(n int) Config {
	c.MaxOpenConns = n
	return c
}

// WithMaxIdleConns returns a copy of the config with the given MaxIdleConns.
func (c Config) WithMaxIdleConns(n int) Config {
	c.MaxIdleConns = n
	return c
}

// WithConnMaxLifetime returns a copy of the config with the given ConnMaxLifetime.
func (c Config) WithConnMaxLifetime(d time.Duration) Config {
	c.ConnMaxLifetime = d
	return c
}

// WithConnMaxIdleTime returns a copy of the config with the given ConnMaxIdleTime.
func (c Config) WithConnMaxIdleTime(d time.Duration) Config {
	c.ConnMaxIdleTime = d
	return c
}

// WithQueryTimeout returns a copy of the config with the given QueryTimeout.
func (c Config) WithQueryTimeout(d time.Duration) Config {
	c.QueryTimeout = d
	return c
}

// WithTransactionTimeout returns a copy of the config with the given TransactionTimeout.
func (c Config) WithTransactionTimeout(d time.Duration) Config {
	c.TransactionTimeout = d
	return c
}

// WithRetries configures retry behavior.
func (c Config) WithRetries(maxRetries int, delay time.Duration) Config {
	c.MaxRetries = maxRetries
	c.RetryDelay = delay
	return c
}

// ConfigMap is a map of connection names to configurations.
type ConfigMap map[string]Config

// Validate validates all configurations in the map.
func (cm ConfigMap) Validate() error {
	if len(cm) == 0 {
		return fmt.Errorf("%w: at least one configuration is required", ErrInvalidConfig)
	}

	for name, config := range cm {
		if name == "" {
			return fmt.Errorf("%w: connection name cannot be empty", ErrInvalidConfig)
		}

		if err := config.Validate(); err != nil {
			return fmt.Errorf("config for connection %q: %w", name, err)
		}
	}

	return nil
}

// Executor defines the interface for executing SQL operations.
type Executor interface {
	// Exec executes a query without returning any rows.
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	// Query executes a query that returns rows.
	Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// QueryRow executes a query that is expected to return at most one row.
	QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row

	// BeginTx starts a transaction.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// CRUDExecutor defines the interface for CRUD operations.
type CRUDExecutor interface {
	Executor

	// Insert inserts a row into the specified table.
	Insert(ctx context.Context, table string, data map[string]interface{}) (sql.Result, error)

	// Update updates rows in the specified table.
	Update(ctx context.Context, table string, data, where map[string]interface{}) (sql.Result, error)

	// Delete deletes rows from the specified table.
	Delete(ctx context.Context, table string, where map[string]interface{}) (sql.Result, error)

	// Select executes a SELECT query.
	Select(ctx context.Context, table string, columns []string, where map[string]interface{}) (*sql.Rows, error)

	// SelectOne executes a SELECT query that returns at most one row.
	SelectOne(ctx context.Context, table string, columns []string, where map[string]interface{}) *sql.Row
}

// DB represents a database connection with enhanced functionality.
type DB struct {
	db     *sql.DB
	config Config
}

// NewDB creates a new database connection.
func NewDB(config Config) (*DB, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	db, err := sql.Open(string(config.Driver), config.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), config.PingTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &DB{
		db:     db,
		config: config,
	}, nil
}

// Exec executes a query without returning any rows.
func (db *DB) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if db.db == nil {
		return nil, ErrConnectionClosed
	}

	if query == "" {
		return nil, ErrInvalidQuery
	}

	ctx, cancel := db.withTimeout(ctx, db.config.QueryTimeout)
	defer cancel()

	return db.db.ExecContext(ctx, query, args...)
}

// Query executes a query that returns rows.
func (db *DB) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if db.db == nil {
		return nil, ErrConnectionClosed
	}

	if query == "" {
		return nil, ErrInvalidQuery
	}

	ctx, cancel := db.withTimeout(ctx, db.config.QueryTimeout)
	defer cancel()

	return db.db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
func (db *DB) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if db.db == nil || query == "" {
		// Return a row that will error when scanned
		return &sql.Row{}
	}

	ctx, cancel := db.withTimeout(ctx, db.config.QueryTimeout)
	defer cancel()

	return db.db.QueryRowContext(ctx, query, args...)
}

// BeginTx starts a transaction.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	if db.db == nil {
		return nil, ErrConnectionClosed
	}

	ctx, cancel := db.withTimeout(ctx, db.config.TransactionTimeout)
	defer cancel()

	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	return tx, nil
}

// Transaction executes a function within a transaction.
func (db *DB) Transaction(ctx context.Context, fn func(*sql.Tx) error, opts *sql.TxOptions) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// Execute the function
	if err := fn(tx); err != nil {
		// Rollback on error
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return fmt.Errorf("%w: %v (rollback error: %v)", ErrTransactionFailed, err, rbErr)
		}
		return fmt.Errorf("%w: %w", ErrTransactionFailed, err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// Insert inserts a row into the specified table.
func (db *DB) Insert(ctx context.Context, table string, data map[string]interface{}) (sql.Result, error) {
	if table == "" {
		return nil, fmt.Errorf("%w: table name cannot be empty", ErrInvalidArguments)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("%w: no data to insert", ErrInvalidArguments)
	}

	// Use driver-specific escaping for enhanced security
	escapedTable, err := EscapeTableName(db.config.Driver, table)
	if err != nil {
		return nil, fmt.Errorf("escape table name %q: %w", table, err)
	}

	columns, placeholders, args, err := buildInsertDataWithDriver(db.config.Driver, data)
	if err != nil {
		return nil, fmt.Errorf("build insert data: %w", err)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		escapedTable,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	return db.Exec(ctx, query, args...)
}

// Update updates rows in the specified table.
func (db *DB) Update(ctx context.Context, table string, data, where map[string]interface{}) (sql.Result, error) {
	if table == "" {
		return nil, fmt.Errorf("%w: table name cannot be empty", ErrInvalidArguments)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("%w: no data to update", ErrInvalidArguments)
	}

	if len(where) == 0 {
		return nil, fmt.Errorf("%w: WHERE clause is required for UPDATE", ErrInvalidArguments)
	}

	// Use driver-specific escaping for enhanced security
	escapedTable, err := EscapeTableName(db.config.Driver, table)
	if err != nil {
		return nil, fmt.Errorf("escape table name %q: %w", table, err)
	}

	setClause, setArgs, err := buildSetClauseWithDriver(db.config.Driver, data)
	if err != nil {
		return nil, fmt.Errorf("build set clause: %w", err)
	}

	whereClause, whereArgs, err := buildWhereClauseWithDriver(db.config.Driver, where)
	if err != nil {
		return nil, fmt.Errorf("build where clause: %w", err)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		escapedTable,
		setClause,
		whereClause,
	)

	args := append(setArgs, whereArgs...)
	return db.Exec(ctx, query, args...)
}

// Delete deletes rows from the specified table.
func (db *DB) Delete(ctx context.Context, table string, where map[string]interface{}) (sql.Result, error) {
	if table == "" {
		return nil, fmt.Errorf("%w: table name cannot be empty", ErrInvalidArguments)
	}

	if len(where) == 0 {
		return nil, fmt.Errorf("%w: WHERE clause is required for DELETE", ErrInvalidArguments)
	}

	// Use driver-specific escaping for enhanced security
	escapedTable, err := EscapeTableName(db.config.Driver, table)
	if err != nil {
		return nil, fmt.Errorf("escape table name %q: %w", table, err)
	}

	whereClause, args, err := buildWhereClauseWithDriver(db.config.Driver, where)
	if err != nil {
		return nil, fmt.Errorf("build where clause: %w", err)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		escapedTable,
		whereClause,
	)

	return db.Exec(ctx, query, args...)
}

// Select executes a SELECT query.
// Select selects rows from the specified table.
func (db *DB) Select(ctx context.Context, table string, columns []string, where map[string]interface{}) (*sql.Rows, error) {
	if table == "" {
		return nil, fmt.Errorf("%w: table name cannot be empty", ErrInvalidArguments)
	}

	// Use driver-specific escaping for enhanced security
	escapedTable, err := EscapeTableName(db.config.Driver, table)
	if err != nil {
		return nil, fmt.Errorf("escape table name %q: %w", table, err)
	}

	columnList, err := buildColumnListWithDriver(db.config.Driver, columns)
	if err != nil {
		return nil, fmt.Errorf("build column list: %w", err)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, escapedTable)

	if len(where) > 0 {
		whereClause, args, err := buildWhereClauseWithDriver(db.config.Driver, where)
		if err != nil {
			return nil, fmt.Errorf("build where clause: %w", err)
		}
		query += " WHERE " + whereClause
		return db.Query(ctx, query, args...)
	}

	return db.Query(ctx, query)
}

// SelectOne executes a SELECT query that returns at most one row.
// SelectOne selects a single row from the specified table.
func (db *DB) SelectOne(ctx context.Context, table string, columns []string, where map[string]interface{}) *sql.Row {
	if table == "" {
		return &sql.Row{}
	}

	// Use driver-specific escaping for enhanced security
	escapedTable, err := EscapeTableName(db.config.Driver, table)
	if err != nil {
		// Return empty row on error to maintain backward compatibility
		return &sql.Row{}
	}

	columnList, err := buildColumnListWithDriver(db.config.Driver, columns)
	if err != nil {
		// Return empty row on error to maintain backward compatibility
		return &sql.Row{}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, escapedTable)

	if len(where) > 0 {
		whereClause, args, err := buildWhereClauseWithDriver(db.config.Driver, where)
		if err != nil {
			// Return empty row on error to maintain backward compatibility
			return &sql.Row{}
		}
		query += " WHERE " + whereClause
		return db.QueryRow(ctx, query, args...)
	}

	return db.QueryRow(ctx, query)
}

// Ping verifies the connection is still alive.
func (db *DB) Ping(ctx context.Context) error {
	if db.db == nil {
		return ErrConnectionClosed
	}

	ctx, cancel := db.withTimeout(ctx, db.config.PingTimeout)
	defer cancel()

	return db.db.PingContext(ctx)
}

// Close closes the database connection.
func (db *DB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

// Stats returns database statistics.
func (db *DB) Stats() sql.DBStats {
	if db.db == nil {
		return sql.DBStats{}
	}
	return db.db.Stats()
}

// Config returns the configuration.
func (db *DB) Config() Config {
	return db.config
}

// RawDB returns the underlying sql.DB.
func (db *DB) RawDB() *sql.DB {
	return db.db
}

// withTimeout adds a timeout to the context if configured.
func (db *DB) withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

// Helper functions for building SQL queries

// buildInsertData builds INSERT query components.
func buildInsertData(data map[string]interface{}) (columns []string, placeholders []string, args []interface{}) {
	columns = make([]string, 0, len(data))
	placeholders = make([]string, 0, len(data))
	args = make([]interface{}, 0, len(data))

	for column, value := range data {
		columns = append(columns, escapeIdentifier(column))
		placeholders = append(placeholders, "?")
		args = append(args, value)
	}

	return columns, placeholders, args
}

// buildInsertDataWithDriver builds INSERT query components with driver-specific escaping.
func buildInsertDataWithDriver(driver Driver, data map[string]interface{}) (columns []string, placeholders []string, args []interface{}, err error) {
	columns = make([]string, 0, len(data))
	placeholders = make([]string, 0, len(data))
	args = make([]interface{}, 0, len(data))

	for column, value := range data {
		escaped, err := EscapeColumnName(driver, column)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("escape column %q: %w", column, err)
		}
		columns = append(columns, escaped)
		placeholders = append(placeholders, "?")
		args = append(args, value)
	}

	return columns, placeholders, args, nil
}

// buildSetClause builds SET clause for UPDATE queries.

// buildWhereClauseWithDriver builds WHERE clause components with driver-specific escaping.
func buildWhereClauseWithDriver(driver Driver, where map[string]interface{}) (clause string, args []interface{}, err error) {
	clauses := make([]string, 0, len(where))
	args = make([]interface{}, 0, len(where))

	for column, value := range where {
		escaped, err := EscapeColumnName(driver, column)
		if err != nil {
			return "", nil, fmt.Errorf("escape column %q: %w", column, err)
		}
		clauses = append(clauses, fmt.Sprintf("%s = ?", escaped))
		args = append(args, value)
	}

	return strings.Join(clauses, " AND "), args, nil
}

// buildSetClauseWithDriver builds UPDATE SET clause components with driver-specific escaping.
func buildSetClauseWithDriver(driver Driver, data map[string]interface{}) (clause string, args []interface{}, err error) {
	clauses := make([]string, 0, len(data))
	args = make([]interface{}, 0, len(data))

	for column, value := range data {
		escaped, err := EscapeColumnName(driver, column)
		if err != nil {
			return "", nil, fmt.Errorf("escape column %q: %w", column, err)
		}
		clauses = append(clauses, fmt.Sprintf("%s = ?", escaped))
		args = append(args, value)
	}

	return strings.Join(clauses, ", "), args, nil
}

// buildWhereClause builds WHERE clause for queries.
func buildWhereClause(where map[string]interface{}) (string, []interface{}) {
	if len(where) == 0 {
		return "1=1", nil
	}

	clauses := make([]string, 0, len(where))
	args := make([]interface{}, 0, len(where))

	for column, value := range where {
		clauses = append(clauses, fmt.Sprintf("%s = ?", escapeIdentifier(column)))
		args = append(args, value)
	}

	return strings.Join(clauses, " AND "), args
}

// buildColumnList builds a column list for SELECT queries.
func buildColumnList(columns []string) string {
	if len(columns) == 0 {
		return "*"
	}

	escaped := make([]string, len(columns))
	for i, col := range columns {
		escaped[i] = escapeIdentifier(col)
	}
	return strings.Join(escaped, ", ")
}

// buildColumnListWithDriver builds a column list for SELECT queries with driver-specific escaping.
func buildColumnListWithDriver(driver Driver, columns []string) (string, error) {
	if len(columns) == 0 {
		return "*", nil
	}

	escaped := make([]string, len(columns))
	for i, col := range columns {
		escapedCol, err := EscapeColumnName(driver, col)
		if err != nil {
			return "", fmt.Errorf("escape column %q: %w", col, err)
		}
		escaped[i] = escapedCol
	}
	return strings.Join(escaped, ", "), nil
}

// escapeIdentifier escapes SQL identifiers to prevent SQL injection.
// Uses the new security module for enhanced protection.
func escapeIdentifier(identifier string) string {
	// For backward compatibility, use MySQL driver by default
	// This maintains existing behavior while providing enhanced security
	escaped, err := EscapeIdentifier(MySQL, identifier)
	if err != nil {
		// If validation fails, fall back to simple escaping for backward compatibility
		// but log or handle the error appropriately in production
		return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
	}
	return escaped
}

// Map is a convenience type for map[string]interface{}.
type Map map[string]interface{}

// NewMap creates a new Map.
func NewMap() Map {
	return make(Map)
}

// Set sets a key-value pair in the map and returns the map for chaining.
func (m Map) Set(key string, value interface{}) Map {
	m[key] = value
	return m
}

// Get gets a value from the map.
func (m Map) Get(key string) (interface{}, bool) {
	value, ok := m[key]
	return value, ok
}

// Delete deletes a key from the map.
func (m Map) Delete(key string) {
	delete(m, key)
}

// Keys returns all keys in the map.
func (m Map) Keys() []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// Values returns all values in the map.
func (m Map) Values() []interface{} {
	values := make([]interface{}, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}

// Clone creates a copy of the map.
func (m Map) Clone() Map {
	clone := make(Map, len(m))
	for key, value := range m {
		clone[key] = value
	}
	return clone
}

// Where creates a WHERE clause map.
func Where(conditions ...interface{}) Map {
	if len(conditions)%2 != 0 {
		panic("sqlx: Where conditions must be key-value pairs")
	}

	m := NewMap()
	for i := 0; i < len(conditions); i += 2 {
		key, ok := conditions[i].(string)
		if !ok {
			panic("sqlx: Where condition keys must be strings")
		}
		m[key] = conditions[i+1]
	}
	return m
}

// Data creates a data map for INSERT or UPDATE operations.
func Data(pairs ...interface{}) Map {
	if len(pairs)%2 != 0 {
		panic("sqlx: Data must be key-value pairs")
	}

	m := NewMap()
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			panic("sqlx: Data keys must be strings")
		}
		m[key] = pairs[i+1]
	}
	return m
}

// ScanRow scans a single row into the provided destinations.
func ScanRow(row *sql.Row, dest ...interface{}) error {
	if row == nil {
		return fmt.Errorf("%w: row is nil", ErrInvalidArguments)
	}

	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoRows
		}
		return fmt.Errorf("scan row: %w", err)
	}

	return nil
}

// ScanRows scans multiple rows.
func ScanRows(rows *sql.Rows, fn func(*sql.Rows) error) error {
	if rows == nil {
		return fmt.Errorf("%w: rows is nil", ErrInvalidArguments)
	}

	defer rows.Close()

	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	return nil
}

// IsDuplicateError checks if an error is a duplicate entry error.
func IsDuplicateError(err error) bool {
	if err == nil {
		return false
	}

	// Check error message for duplicate entry
	errStr := err.Error()
	return strings.Contains(errStr, "Duplicate entry") ||
		strings.Contains(errStr, "1062") ||
		strings.Contains(errStr, "unique constraint")
}

// IsForeignKeyError checks if an error is a foreign key constraint error.
func IsForeignKeyError(err error) bool {
	if err == nil {
		return false
	}

	// Check error message for foreign key
	errStr := err.Error()
	return strings.Contains(errStr, "foreign key constraint") ||
		strings.Contains(errStr, "1452")
}

// Pool manages multiple database connections.
type Pool struct {
	mu          sync.RWMutex
	connections map[string]*DB
	defaultName string
	closed      bool
}

// NewPool creates a new connection pool.
func NewPool() *Pool {
	return &Pool{
		connections: make(map[string]*DB),
	}
}

// Register registers a new database connection.
func (p *Pool) Register(name string, config Config) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	if name == "" {
		return ErrEmptyConnectionName
	}

	if _, exists := p.connections[name]; exists {
		return fmt.Errorf("%w: connection %q already exists", ErrInvalidConfig, name)
	}

	db, err := NewDB(config)
	if err != nil {
		return fmt.Errorf("create database connection %q: %w", name, err)
	}

	p.connections[name] = db

	// Set as default if it's the first connection
	if p.defaultName == "" {
		p.defaultName = name
	}

	return nil
}

// RegisterMultiple registers multiple database connections.
func (p *Pool) RegisterMultiple(configs ConfigMap) error {
	for name, config := range configs {
		if err := p.Register(name, config); err != nil {
			// Clean up any connections that were successfully registered
			p.Close()
			return fmt.Errorf("register connection %q: %w", name, err)
		}
	}
	return nil
}

// Get returns a database connection by name.
func (p *Pool) Get(name string) (*DB, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, ErrPoolClosed
	}

	if name == "" {
		name = p.defaultName
		if name == "" {
			return nil, fmt.Errorf("%w: no default connection set", ErrEmptyConnectionName)
		}
	}

	db, exists := p.connections[name]
	if !exists {
		return nil, fmt.Errorf("%w: connection %q not found", ErrUnregistered, name)
	}

	return db, nil
}

// Default returns the default database connection.
func (p *Pool) Default() (*DB, error) {
	return p.Get("")
}

// SetDefault sets the default connection name.
func (p *Pool) SetDefault(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	if name == "" {
		return ErrEmptyConnectionName
	}

	if _, exists := p.connections[name]; !exists {
		return fmt.Errorf("%w: connection %q not found", ErrUnregistered, name)
	}

	p.defaultName = name
	return nil
}

// Close closes all database connections in the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	var firstErr error
	for name, db := range p.connections {
		if err := db.Close(); err != nil {
			err = fmt.Errorf("close connection %q: %w", name, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Clear the map
	p.connections = make(map[string]*DB)
	p.defaultName = ""
	p.closed = true

	return firstErr
}

// Ping pings all database connections.
func (p *Pool) Ping(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrPoolClosed
	}

	var firstErr error
	for name, db := range p.connections {
		if err := db.Ping(ctx); err != nil {
			err = fmt.Errorf("ping connection %q: %w", name, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// Stats returns statistics for all connections.
func (p *Pool) Stats() map[string]sql.DBStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]sql.DBStats, len(p.connections))
	for name, db := range p.connections {
		stats[name] = db.Stats()
	}
	return stats
}

// Names returns the names of all registered connections.
func (p *Pool) Names() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	names := make([]string, 0, len(p.connections))
	for name := range p.connections {
		names = append(names, name)
	}
	return names
}

// Count returns the number of registered connections.
func (p *Pool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.connections)
}

// Global connection pool instance
var (
	globalPool     *Pool
	globalPoolOnce sync.Once
)

// Init initializes the global connection pool with the given configurations.
func Init(configs ConfigMap) error {
	var err error
	globalPoolOnce.Do(func() {
		globalPool = NewPool()
		err = globalPool.RegisterMultiple(configs)
	})
	return err
}

// GetDB returns a database connection by name.
func GetDB(name string) (*DB, error) {
	if globalPool == nil {
		return nil, ErrPoolClosed
	}
	return globalPool.Get(name)
}

// Default returns the default database connection.
func Default() (*DB, error) {
	return GetDB("")
}

// SetDefault sets the default connection name.
func SetDefault(name string) error {
	if globalPool == nil {
		return ErrPoolClosed
	}
	return globalPool.SetDefault(name)
}

// Close closes all database connections.
func Close() error {
	if globalPool == nil {
		return nil
	}
	return globalPool.Close()
}

// Ping pings all database connections.
func Ping(ctx context.Context) error {
	if globalPool == nil {
		return ErrPoolClosed
	}
	return globalPool.Ping(ctx)
}

// Stats returns statistics for all connections.
func Stats() map[string]sql.DBStats {
	if globalPool == nil {
		return nil
	}
	return globalPool.Stats()
}

// Names returns the names of all registered connections.
func Names() []string {
	if globalPool == nil {
		return nil
	}
	return globalPool.Names()
}

// Count returns the number of registered connections.
func Count() int {
	if globalPool == nil {
		return 0
	}
	return globalPool.Count()
}

// Transaction executes a function within a transaction.
func Transaction(ctx context.Context, name string, fn func(*sql.Tx) error, opts *sql.TxOptions) error {
	db, err := GetDB(name)
	if err != nil {
		return err
	}
	return db.Transaction(ctx, fn, opts)
}

// Insert inserts a row into the specified table.
func Insert(ctx context.Context, name, table string, data Map) (sql.Result, error) {
	db, err := GetDB(name)
	if err != nil {
		return nil, err
	}
	return db.Insert(ctx, table, data)
}

// Update updates rows in the specified table.
func Update(ctx context.Context, name, table string, data, where Map) (sql.Result, error) {
	db, err := GetDB(name)
	if err != nil {
		return nil, err
	}
	return db.Update(ctx, table, data, where)
}

// Delete deletes rows from the specified table.
func Delete(ctx context.Context, name, table string, where Map) (sql.Result, error) {
	db, err := GetDB(name)
	if err != nil {
		return nil, err
	}
	return db.Delete(ctx, table, where)
}

// Select executes a SELECT query.
func Select(ctx context.Context, name, table string, columns []string, where Map) (*sql.Rows, error) {
	db, err := GetDB(name)
	if err != nil {
		return nil, err
	}
	return db.Select(ctx, table, columns, where)
}

// SelectOne executes a SELECT query that returns at most one row.
func SelectOne(ctx context.Context, name, table string, columns []string, where Map) *sql.Row {
	db, err := GetDB(name)
	if err != nil {
		// Return a row that will error when scanned
		return &sql.Row{}
	}
	return db.SelectOne(ctx, table, columns, where)
}

// Exec executes a query without returning any rows.
func Exec(ctx context.Context, name, query string, args ...interface{}) (sql.Result, error) {
	db, err := GetDB(name)
	if err != nil {
		return nil, err
	}
	return db.Exec(ctx, query, args...)
}

// Query executes a query that returns rows.
func Query(ctx context.Context, name, query string, args ...interface{}) (*sql.Rows, error) {
	db, err := GetDB(name)
	if err != nil {
		return nil, err
	}
	return db.Query(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
func QueryRow(ctx context.Context, name, query string, args ...interface{}) *sql.Row {
	db, err := GetDB(name)
	if err != nil {
		// Return a row that will error when scanned
		return &sql.Row{}
	}
	return db.QueryRow(ctx, query, args...)
}
