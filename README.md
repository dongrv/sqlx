# SQLX - A Modern Go SQL Toolkit

SQLX is a modern, type-safe, and easy-to-use SQL toolkit for Go, built on top of the standard `database/sql` package. It provides a clean, intuitive API for working with MySQL databases while maintaining full compatibility with Go's standard library.

## Features

- 🚀 **Simple and Intuitive API** - Designed to be easy to use and understand
- 🔒 **Type Safety** - Leverages Go's type system for safer database operations
- 🛡️ **SQL Injection Protection** - Advanced identifier escaping and validation for multiple database drivers
- 🔄 **Connection Pool Management** - Support for multiple database connections with intelligent pooling
- 🔧 **Query Builder** - Fluent API for building complex SQL queries
- 🔄 **Transaction Support** - Full ACID transaction support with retry logic
- ⚡ **Performance** - Optimized for performance with minimal overhead
- 🛡️ **Error Handling** - Comprehensive error types and handling utilities
- 📊 **Monitoring** - Built-in connection statistics and health checks
- 🔌 **Extensible** - Designed to be extended for other database drivers

## Installation

```bash
go get github.com/dongrv/sqlx
```

## Quick Start

### Basic Initialization

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/dongrv/sqlx"
)

func main() {
    // Configure database connections
    configs := sqlx.ConfigMap{
        "default": sqlx.DefaultConfig().
            WithDSN("root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local").
            WithMaxOpenConns(20).
            WithMaxIdleConns(10),
    }
    
    // Initialize the connection pool
    err := sqlx.Init(configs, sqlx.WithTimeout(30*time.Second))
    if err != nil {
        log.Fatal(err)
    }
    defer sqlx.Close()
    
    // Ping all connections
    ctx := context.Background()
    if err := sqlx.Ping(ctx); err != nil {
        log.Printf("Warning: %v", err)
    }
    
    fmt.Println("Database initialized successfully!")
}
```

### CRUD Operations

```go
// Insert data
data := sqlx.Data(
    "name", "John Doe",
    "email", "john@example.com",
    "age", 30,
)

result, err := sqlx.Insert(ctx, "default", "users", data)
if err != nil {
    log.Fatal(err)
}
id, _ := result.LastInsertId()
fmt.Printf("Inserted user with ID: %d\n", id)

// Update data
updateData := sqlx.Data("email", "john.doe@example.com")
where := sqlx.Where("id", id)

result, err = sqlx.Update(ctx, "default", "users", updateData, where)
if err != nil {
    log.Fatal(err)
}

// Select data
rows, err := sqlx.Select(ctx, "default", "users", 
    []string{"id", "name", "email", "age"}, 
    sqlx.Where("age", ">", 25),
)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var user struct {
        ID    int64
        Name  string
        Email string
        Age   int
    }
    if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.Age); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("User: %+v\n", user)
}

// Delete data
result, err = sqlx.Delete(ctx, "default", "users", sqlx.Where("id", id))
if err != nil {
    log.Fatal(err)
}
```

### Query Builder

```go
// Build complex queries
qb := sqlx.Builder("users").
    Select("id", "name", "email", "created_at").
    Where("status", "=", "active").
    Where("age", ">=", 18).
    OrderBy("created_at", "DESC").
    Limit(100).
    Offset(0)

query, args := qb.Build()
fmt.Printf("Query: %s\n", query)
fmt.Printf("Args: %v\n", args)

// Execute the built query
rows, err := sqlx.Query(ctx, "default", query, args...)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()
```

### Transactions

```go
// Execute operations within a transaction
err = sqlx.Transaction(ctx, "default", func(tx *sql.Tx) error {
    // Insert user
    _, err := tx.ExecContext(ctx, 
        "INSERT INTO users (name, email) VALUES (?, ?)", 
        "Alice", "alice@example.com",
    )
    if err != nil {
        return err
    }
    
    // Insert user profile
    _, err = tx.ExecContext(ctx,
        "INSERT INTO profiles (user_id, bio) VALUES (LAST_INSERT_ID(), ?)",
        "Software Engineer",
    )
    return err
}, nil)

if err != nil {
    log.Fatal("Transaction failed:", err)
}
fmt.Println("Transaction completed successfully")
```

### SQL Security and Injection Protection

SQLX provides comprehensive protection against SQL injection attacks through advanced identifier escaping and validation.

#### Identifier Validation

All table names, column names, and other SQL identifiers are validated before use:

```go
// Valid identifiers
sqlx.ValidateTableName(sqlx.MySQL, "users")           // ✅ OK
sqlx.ValidateColumnName(sqlx.MySQL, "user_name")      // ✅ OK
sqlx.ValidateIdentifier(sqlx.MySQL, "created_at")     // ✅ OK

// Invalid identifiers (will return error)
sqlx.ValidateTableName(sqlx.MySQL, "123users")        // ❌ Starts with number
sqlx.ValidateTableName(sqlx.MySQL, "user-table")      // ❌ Contains dash
sqlx.ValidateTableName(sqlx.MySQL, "SELECT")          // ❌ SQL keyword
sqlx.ValidateTableName(sqlx.MySQL, "")                // ❌ Empty identifier
```

#### Safe Identifier Escaping

SQLX automatically escapes identifiers using driver-specific rules:

```go
// MySQL uses backticks
mysqlEscaped := sqlx.MustEscapeTableName(sqlx.MySQL, "users")
// Result: `users`

// PostgreSQL uses double quotes
pgEscaped := sqlx.MustEscapeTableName(sqlx.PostgreSQL, "users")
// Result: "users"

// SQLite uses double quotes
sqliteEscaped := sqlx.MustEscapeTableName(sqlx.SQLite, "users")
// Result: "users"
```

#### Protection Against SQL Injection

SQLX prevents common SQL injection attacks:

```go
// Malicious input attempts
maliciousInputs := []string{
    "users; DROP TABLE users",      // ❌ Blocked
    "users -- comment",             // ❌ Blocked
    "users` OR `1`=`1",             // ❌ Blocked
    "users UNION SELECT *",         // ❌ Blocked
}

for _, input := range maliciousInputs {
    _, err := sqlx.NewSafeIdentifier(sqlx.MySQL, input)
    if err != nil {
        fmt.Printf("✅ Blocked: %v\n", err)
    }
}
```

#### Safe Query Building

All query building functions use safe identifier escaping:

```go
// Safe INSERT query
data := map[string]any{
    "name":  "John Doe",
    "email": "john@example.com",
    "age":   30,
}
query, args, _ := sqlx.BuildInsertQueryWithDriver(sqlx.MySQL, "users", data)
// Result: INSERT INTO `users` (`name`, `email`, `age`) VALUES (?, ?, ?)

// Safe UPDATE query
updateData := map[string]any{"age": 31}
where := map[string]any{"id": 1}
query, args, _ := sqlx.BuildUpdateQueryWithDriver(sqlx.MySQL, "users", updateData, where)
// Result: UPDATE `users` SET `age` = ? WHERE `id` = ?

// Safe SELECT query
columns := []string{"id", "name", "email"}
where := map[string]any{"active": true}
query, args, _ := sqlx.BuildSelectQueryWithDriver(sqlx.MySQL, "users", columns, where)
// Result: SELECT `id`, `name`, `email` FROM `users` WHERE `active` = ?
```

#### Driver-Specific Security

SQLX supports multiple database drivers with appropriate escaping:

```go
// Get the appropriate escaper for your driver
escaper := sqlx.GetIdentifierEscaper(sqlx.MySQL)
escaped, _ := escaper.Escape("table_name")  // Returns: `table_name`

escaper = sqlx.GetIdentifierEscaper(sqlx.PostgreSQL)
escaped, _ = escaper.Escape("table_name")   // Returns: "table_name"
```

#### SafeIdentifier Type

For maximum safety, use the `SafeIdentifier` type:

```go
// Create a safe identifier
si, err := sqlx.NewSafeIdentifier(sqlx.MySQL, "users")
if err != nil {
    log.Fatal(err)
}

// Use it safely in queries
fmt.Printf("Original: %s\n", si.Original())  // users
fmt.Printf("Escaped: %s\n", si.String())     // `users`
fmt.Printf("Driver: %v\n", si.Driver())      // mysql
fmt.Printf("Valid: %v\n", si.IsValid())      // true

// Safe identifier lists
columns := []string{"id", "name", "email", "created_at"}
sil, _ := sqlx.NewSafeIdentifierList(sqlx.MySQL, columns...)
fmt.Println(sil.Join(", "))  // `id`, `name`, `email`, `created_at`
```

#### Best Practices

1. **Always validate user input** before using it as an identifier
2. **Use parameterized queries** for values (SQLX does this automatically)
3. **Prefer safe identifier functions** over manual string concatenation
4. **Validate against reserved words** to avoid syntax errors
5. **Use appropriate length limits** for database compatibility

### Advanced Features

#### Multiple Database Connections

```go
configs := sqlx.ConfigMap{
    "primary": sqlx.DefaultConfig().
        WithDSN("write_user:password@tcp(primary.db:3306)/appdb"),
    "replica": sqlx.DefaultConfig().
        WithDSN("read_user:password@tcp(replica.db:3306)/appdb").
        WithMaxOpenConns(50),
    "analytics": sqlx.DefaultConfig().
        WithDSN("analytics_user:password@tcp(analytics.db:3306)/analyticsdb").
        WithQueryTimeout(5 * time.Minute),
}

sqlx.Init(configs)

// Use different connections for different purposes
writeResult, _ := sqlx.Insert(ctx, "primary", "users", data)
readRows, _ := sqlx.Select(ctx, "replica", "users", columns, where)
```

#### Error Handling

```go
result, err := sqlx.Insert(ctx, "default", "users", data)
if err != nil {
    if sqlx.IsDuplicateError(err) {
        fmt.Println("Duplicate entry detected")
    } else if sqlx.IsForeignKeyError(err) {
        fmt.Println("Foreign key constraint violation")
    } else if sqlx.Is(err, sqlx.ErrNoRows) {
        fmt.Println("No rows found")
    } else {
        log.Fatal("Unexpected error:", err)
    }
}
```

#### Connection Statistics

```go
// Get connection pool statistics
stats := sqlx.Stats()
for name, stat := range stats {
    fmt.Printf("Connection %s:\n", name)
    fmt.Printf("  Max Open Connections: %d\n", stat.MaxOpenConnections)
    fmt.Printf("  Open Connections: %d\n", stat.OpenConnections)
    fmt.Printf("  In Use: %d\n", stat.InUse)
    fmt.Printf("  Idle: %d\n", stat.Idle)
    fmt.Printf("  Wait Count: %d\n", stat.WaitCount)
    fmt.Printf("  Wait Duration: %v\n", stat.WaitDuration)
}
```

#### Retry Logic

```go
// Automatically retry failed operations
err = sqlx.WithRetry("default", func(db *sqlx.DBConfig) error {
    // This operation will be retried up to 3 times
    _, err := db.DB.ExecContext(ctx, 
        "UPDATE users SET last_seen = NOW() WHERE id = ?", 
        userID,
    )
    return err
})

if err != nil {
    log.Fatal("Operation failed after retries:", err)
}
```

## Configuration

### Database Configuration

```go
config := sqlx.DefaultConfig().
    WithDSN("user:password@tcp(host:3306)/database").
    WithMaxOpenConns(100).           // Maximum open connections
    WithMaxIdleConns(25).            // Maximum idle connections
    WithConnMaxLifetime(30 * time.Minute).  // Connection lifetime
    WithConnMaxIdleTime(5 * time.Minute).   // Maximum idle time
    WithQueryTimeout(30 * time.Second).     // Query timeout
    WithTransactionTimeout(60 * time.Second). // Transaction timeout
    WithQueryLog().                   // Enable query logging
    WithSlowQueryLog(100 * time.Millisecond). // Log slow queries
    WithRetries(3, 100 * time.Millisecond)   // Retry configuration
```

### Connection Options

```go
options := sqlx.DefaultConnectionOptions().
    WithContext(ctx).
    WithTimeout(30 * time.Second).
    WithRetry(3, 100 * time.Millisecond)

sqlx.Init(configs, 
    sqlx.WithContext(ctx),
    sqlx.WithTimeout(30*time.Second),
    sqlx.WithRetry(3, 100*time.Millisecond),
)
```

## API Reference

### Core Functions

- `Init(configs ConfigMap, opts ...ConnectionOption) error` - Initialize connection pool
- `DB(name string) (*DBConfig, error)` - Get database connection
- `Default() (*DBConfig, error)` - Get default connection
- `Close() error` - Close all connections
- `Ping(ctx context.Context) error` - Ping all connections
- `Stats() map[string]sql.DBStats` - Get connection statistics

### CRUD Operations

- `Insert(ctx Context, name, table string, data Map) (Result, error)`
- `Update(ctx Context, name, table string, data, where Map) (Result, error)`
- `Delete(ctx Context, name, table string, where Map) (Result, error)`
- `Select(ctx Context, name, table string, columns []string, where Map) (*Rows, error)`
- `SelectOne(ctx Context, name, table string, columns []string, where Map) *Row`

### Query Execution

- `Exec(ctx Context, name, query string, args ...any) (Result, error)`
- `Query(ctx Context, name, query string, args ...any) (*Rows, error)`
- `QueryRow(ctx Context, name, query string, args ...any) *Row`
- `Transaction(ctx Context, name string, fn func(*sql.Tx) error, opts *sql.TxOptions) error`

### Helper Functions

- `Builder(table string) *QueryBuilder` - Create query builder
- `ScanRow(row *Row, dest ...any) error` - Scan single row
- `ScanRows(rows *Rows, fn func(*Rows) error) error` - Scan multiple rows
- `WithRetry(name string, fn func(*DBConfig) error) error` - Execute with retry logic
- `IsDuplicateError(err error) bool` - Check for duplicate entry errors
- `IsForeignKeyError(err error) bool` - Check for foreign key errors

### Utility Functions

- `Data(pairs ...any) Map` - Create data map
- `Where(conditions ...any) Map` - Create where conditions
- `NewColumnList(columns ...string) ColumnList` - Create column list

## Best Practices

1. **Always close connections**: Use `defer sqlx.Close()` in your main function
2. **Use context for timeouts**: Always pass context with appropriate timeouts
3. **Handle errors properly**: Check and handle all errors returned by SQLX
4. **Use transactions for multiple operations**: Group related operations in transactions
5. **Monitor connection pools**: Regularly check connection statistics
6. **Use appropriate timeouts**: Set reasonable timeouts for your use case
7. **Enable logging in development**: Use `WithQueryLog()` during development

## Performance Tips

1. **Reuse connections**: SQLX manages connection pooling automatically
2. **Use prepared statements**: For repeated queries, use prepared statements
3. **Batch operations**: Use batch inserts/updates for bulk operations
4. **Limit result sets**: Use `LIMIT` clause to limit large result sets
5. **Index properly**: Ensure your database tables are properly indexed

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For support, please open an issue on the GitHub repository or contact the maintainers.
