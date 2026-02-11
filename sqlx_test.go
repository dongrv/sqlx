package sqlx_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dongrv/sqlx"
)

func TestDefaultConfig(t *testing.T) {
	config := sqlx.DefaultConfig()

	if config.Driver != sqlx.MySQL {
		t.Errorf("Expected driver to be MySQL, got %s", config.Driver)
	}

	if config.MaxOpenConns != 10 {
		t.Errorf("Expected MaxOpenConns to be 10, got %d", config.MaxOpenConns)
	}

	if config.MaxIdleConns != 5 {
		t.Errorf("Expected MaxIdleConns to be 5, got %d", config.MaxIdleConns)
	}

	if config.ConnMaxLifetime != 30*time.Minute {
		t.Errorf("Expected ConnMaxLifetime to be 30m, got %v", config.ConnMaxLifetime)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  sqlx.Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: sqlx.Config{
				Driver:       sqlx.MySQL,
				DSN:          "test:test@tcp(localhost:3306)/test",
				MaxOpenConns: 10,
				MaxIdleConns: 5,
			},
			wantErr: false,
		},
		{
			name: "empty driver",
			config: sqlx.Config{
				Driver: "",
				DSN:    "test:test@tcp(localhost:3306)/test",
			},
			wantErr: true,
		},
		{
			name: "empty DSN",
			config: sqlx.Config{
				Driver: sqlx.MySQL,
				DSN:    "",
			},
			wantErr: true,
		},
		{
			name: "negative MaxOpenConns",
			config: sqlx.Config{
				Driver:       sqlx.MySQL,
				DSN:          "test:test@tcp(localhost:3306)/test",
				MaxOpenConns: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfigWithMethods(t *testing.T) {
	config := sqlx.DefaultConfig().
		WithDSN("custom@tcp(localhost:3306)/db").
		WithMaxOpenConns(50).
		WithMaxIdleConns(25).
		WithConnMaxLifetime(1*time.Hour).
		WithQueryTimeout(10*time.Second).
		WithRetries(5, 200*time.Millisecond)

	if config.DSN != "custom@tcp(localhost:3306)/db" {
		t.Errorf("Expected custom DSN, got %s", config.DSN)
	}

	if config.MaxOpenConns != 50 {
		t.Errorf("Expected MaxOpenConns 50, got %d", config.MaxOpenConns)
	}

	if config.MaxIdleConns != 25 {
		t.Errorf("Expected MaxIdleConns 25, got %d", config.MaxIdleConns)
	}

	if config.ConnMaxLifetime != 1*time.Hour {
		t.Errorf("Expected ConnMaxLifetime 1h, got %v", config.ConnMaxLifetime)
	}

	if config.QueryTimeout != 10*time.Second {
		t.Errorf("Expected QueryTimeout 10s, got %v", config.QueryTimeout)
	}

	if config.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5, got %d", config.MaxRetries)
	}

	if config.RetryDelay != 200*time.Millisecond {
		t.Errorf("Expected RetryDelay 200ms, got %v", config.RetryDelay)
	}
}

func TestDataHelper(t *testing.T) {
	data := sqlx.Data(
		"name", "John",
		"age", 30,
		"active", true,
	)

	if len(data) != 3 {
		t.Errorf("Expected 3 items in data map, got %d", len(data))
	}

	if name, ok := data["name"]; !ok || name != "John" {
		t.Errorf("Expected name to be 'John', got %v", name)
	}

	if age, ok := data["age"]; !ok || age != 30 {
		t.Errorf("Expected age to be 30, got %v", age)
	}
}

func TestWhereHelper(t *testing.T) {
	where := sqlx.Where(
		"id", 1,
		"status", "active",
		"deleted", false,
	)

	if len(where) != 3 {
		t.Errorf("Expected 3 items in where map, got %d", len(where))
	}

	if id, ok := where["id"]; !ok || id != 1 {
		t.Errorf("Expected id to be 1, got %v", id)
	}

	if status, ok := where["status"]; !ok || status != "active" {
		t.Errorf("Expected status to be 'active', got %v", status)
	}
}

func TestMapHelpers(t *testing.T) {
	m := sqlx.NewMap().
		Set("key1", "value1").
		Set("key2", 42).
		Set("key3", true)

	if len(m) != 3 {
		t.Errorf("Expected map length 3, got %d", len(m))
	}

	// Test Get
	if val, ok := m.Get("key1"); !ok || val != "value1" {
		t.Errorf("Expected key1 to be 'value1', got %v", val)
	}

	// Test Keys
	keys := m.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	// Test Values
	values := m.Values()
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	// Test Clone
	clone := m.Clone()
	if len(clone) != 3 {
		t.Errorf("Expected clone length 3, got %d", len(clone))
	}

	// Modify clone and ensure original is unchanged
	clone.Set("key4", "new")
	if len(m) != 3 {
		t.Errorf("Original map should still have 3 items, got %d", len(m))
	}
}

func TestErrorIs(t *testing.T) {
	err1 := sqlx.ErrNoRows
	err2 := sqlx.ErrInvalidQuery

	if !sqlx.Is(err1, sqlx.ErrNoRows) {
		t.Error("Expected Is to return true for same error")
	}

	if sqlx.Is(err1, err2) {
		t.Error("Expected Is to return false for different errors")
	}

	// Test with nil
	if sqlx.Is(nil, err1) {
		t.Error("Expected Is to return false when err is nil")
	}

	if sqlx.Is(err1, nil) {
		t.Error("Expected Is to return false when target is nil")
	}

	if !sqlx.Is(nil, nil) {
		t.Error("Expected Is to return true when both are nil")
	}
}

func TestIsDuplicateError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "duplicate entry error",
			err:      errors.New("Error 1062: Duplicate entry 'test@example.com' for key 'email'"),
			expected: true,
		},
		{
			name:     "unique constraint error",
			err:      errors.New("UNIQUE constraint failed: users.email"),
			expected: true,
		},
		{
			name:     "generic error",
			err:      errors.New("some other error"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sqlx.IsDuplicateError(tt.err)
			if result != tt.expected {
				t.Errorf("IsDuplicateError() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestColumnList(t *testing.T) {
	columns := sqlx.NewColumnList("id", "name", "email")

	if !columns.Contains("id") {
		t.Error("Expected columns to contain 'id'")
	}

	if !columns.Contains("name") {
		t.Error("Expected columns to contain 'name'")
	}

	if !columns.Contains("email") {
		t.Error("Expected columns to contain 'email'")
	}

	if columns.Contains("nonexistent") {
		t.Error("Expected columns not to contain 'nonexistent'")
	}

	// Test String representation
	expected := "`id`, `name`, `email`"
	if columns.String() != expected {
		t.Errorf("Expected String() to return %q, got %q", expected, columns.String())
	}

	// Test empty column list
	empty := sqlx.NewColumnList()
	if empty.String() != "*" {
		t.Errorf("Expected empty String() to return '*', got %q", empty.String())
	}
}

func TestQueryBuilderFunctions(t *testing.T) {
	// Test BuildInsertQuery
	insertData := map[string]interface{}{
		"name":  "John",
		"email": "john@example.com",
		"age":   30,
	}
	insertQuery, insertArgs := sqlx.BuildInsertQuery("users", insertData)
	expectedInsertQuery := "INSERT INTO `users` (`name`, `email`, `age`) VALUES (?, ?, ?)"
	if insertQuery != expectedInsertQuery {
		t.Errorf("Expected insert query %q, got %q", expectedInsertQuery, insertQuery)
	}
	if len(insertArgs) != 3 {
		t.Errorf("Expected 3 insert args, got %d", len(insertArgs))
	}

	// Test BuildUpdateQuery
	updateData := map[string]interface{}{"age": 31}
	updateWhere := map[string]interface{}{"id": 1}
	updateQuery, updateArgs := sqlx.BuildUpdateQuery("users", updateData, updateWhere)
	expectedUpdateQuery := "UPDATE `users` SET `age` = ? WHERE `id` = ?"
	if updateQuery != expectedUpdateQuery {
		t.Errorf("Expected update query %q, got %q", expectedUpdateQuery, updateQuery)
	}
	if len(updateArgs) != 2 {
		t.Errorf("Expected 2 update args, got %d", len(updateArgs))
	}

	// Test BuildDeleteQuery
	deleteWhere := map[string]interface{}{"id": 1}
	deleteQuery, deleteArgs := sqlx.BuildDeleteQuery("users", deleteWhere)
	expectedDeleteQuery := "DELETE FROM `users` WHERE `id` = ?"
	if deleteQuery != expectedDeleteQuery {
		t.Errorf("Expected delete query %q, got %q", expectedDeleteQuery, deleteQuery)
	}
	if len(deleteArgs) != 1 {
		t.Errorf("Expected 1 delete arg, got %d", len(deleteArgs))
	}

	// Test Paginate
	paginatedQuery := sqlx.Paginate("SELECT * FROM users", 2, 10)
	expectedPaginatedQuery := "SELECT * FROM users LIMIT 10 OFFSET 10"
	if paginatedQuery != expectedPaginatedQuery {
		t.Errorf("Expected paginated query %q, got %q", expectedPaginatedQuery, paginatedQuery)
	}

	// Test OrderBy
	orderedQuery := sqlx.OrderBy("SELECT * FROM users", "created_at", true)
	expectedOrderedQuery := "SELECT * FROM users ORDER BY `created_at` DESC"
	if orderedQuery != expectedOrderedQuery {
		t.Errorf("Expected ordered query %q, got %q", expectedOrderedQuery, orderedQuery)
	}
}

func TestQueryResult(t *testing.T) {
	qr := sqlx.NewQueryResult().
		WithError(nil).
		WithDuration(100 * time.Millisecond)

	if !qr.Success() {
		t.Error("Expected Success() to return true for nil error")
	}

	if qr.Failed() {
		t.Error("Expected Failed() to return false for nil error")
	}

	if qr.Duration != 100*time.Millisecond {
		t.Errorf("Expected duration 100ms, got %v", qr.Duration)
	}

	// Test with error
	qr2 := sqlx.NewQueryResult().
		WithError(errors.New("test error"))

	if qr2.Success() {
		t.Error("Expected Success() to return false for error")
	}

	if !qr2.Failed() {
		t.Error("Expected Failed() to return true for error")
	}
}

func TestTransactionOptions(t *testing.T) {
	opts := sqlx.DefaultTransactionOptions()

	if opts.Isolation != sql.LevelDefault {
		t.Errorf("Expected default isolation LevelDefault, got %v", opts.Isolation)
	}

	if opts.ReadOnly != false {
		t.Error("Expected default ReadOnly false")
	}

	if opts.Timeout != 60*time.Second {
		t.Errorf("Expected default timeout 60s, got %v", opts.Timeout)
	}

	// Test WithIsolation
	opts2 := opts.WithIsolation(sql.LevelReadCommitted)
	if opts2.Isolation != sql.LevelReadCommitted {
		t.Errorf("Expected isolation LevelReadCommitted, got %v", opts2.Isolation)
	}

	// Test WithReadOnly
	opts3 := opts.WithReadOnly(true)
	if opts3.ReadOnly != true {
		t.Error("Expected ReadOnly true")
	}

	// Test ToTxOptions
	txOpts := opts.ToTxOptions()
	if txOpts == nil {
		t.Error("Expected non-nil TxOptions")
	}
}

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "connection error",
			err:      errors.New("connection refused"),
			expected: true,
		},
		{
			name:     "timeout error",
			err:      errors.New("operation timed out"),
			expected: true,
		},
		{
			name:     "generic error",
			err:      errors.New("some other error"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sqlx.ShouldRetry(tt.err)
			if result != tt.expected {
				t.Errorf("ShouldRetry() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestCountQuery(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple select",
			input:    "SELECT id, name FROM users WHERE active = 1",
			expected: "SELECT COUNT(*) FROM users WHERE active = 1",
		},
		{
			name:     "select with order by",
			input:    "SELECT * FROM users ORDER BY created_at DESC",
			expected: "SELECT COUNT(*) FROM users",
		},
		{
			name:     "select with limit",
			input:    "SELECT id FROM users LIMIT 10",
			expected: "SELECT COUNT(*) FROM users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sqlx.CountQuery(tt.input)
			if result != tt.expected {
				t.Errorf("CountQuery() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestCaseBuilder(t *testing.T) {
	cb := sqlx.NewCaseBuilder().
		Case("status").
		When("'active'", "'Active'").
		When("'inactive'", "'Inactive'").
		Else("'Unknown'")

	caseExpr := cb.Build()
	expectedCaseExpr := "CASE status WHEN 'active' THEN 'Active' WHEN 'inactive' THEN 'Inactive' ELSE 'Unknown' END"
	if caseExpr != expectedCaseExpr {
		t.Errorf("Expected CASE expression %q, got %q", expectedCaseExpr, caseExpr)
	}
}

func TestConfigMapValidation(t *testing.T) {
	tests := []struct {
		name    string
		configs sqlx.ConfigMap
		wantErr bool
	}{
		{
			name: "valid config map",
			configs: sqlx.ConfigMap{
				"db1": sqlx.DefaultConfig().WithDSN("test1@tcp(localhost:3306)/db1"),
				"db2": sqlx.DefaultConfig().WithDSN("test2@tcp(localhost:3306)/db2"),
			},
			wantErr: false,
		},
		{
			name:    "empty config map",
			configs: sqlx.ConfigMap{},
			wantErr: true,
		},
		{
			name: "config map with empty connection name",
			configs: sqlx.ConfigMap{
				"": sqlx.DefaultConfig().WithDSN("test@tcp(localhost:3306)/db"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.configs.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("ConfigMap.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
