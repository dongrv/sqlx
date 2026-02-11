package sqlx_test

import (
	"context"
	"testing"
	"time"

	"github.com/dongrv/sqlx"
)

func TestDefaultConfig(t *testing.T) {
	config := sqlx.DefaultConfig()

	if config.Driver != sqlx.MySQLDriver {
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
				Driver:       sqlx.MySQLDriver,
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
				Driver: sqlx.MySQLDriver,
				DSN:    "",
			},
			wantErr: true,
		},
		{
			name: "negative MaxOpenConns",
			config: sqlx.Config{
				Driver:       sqlx.MySQLDriver,
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

func TestConfigMapValidation(t *testing.T) {
	configs := sqlx.ConfigMap{
		"db1": sqlx.DefaultConfig().WithDSN("test1@tcp(localhost:3306)/db1"),
		"db2": sqlx.DefaultConfig().WithDSN("test2@tcp(localhost:3306)/db2"),
	}

	if err := configs.Validate(); err != nil {
		t.Errorf("ConfigMap.Validate() returned error: %v", err)
	}

	// Test empty config map
	emptyConfigs := sqlx.ConfigMap{}
	if err := emptyConfigs.Validate(); err == nil {
		t.Error("Expected error for empty config map, got nil")
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

	if active, ok := data["active"]; !ok || active != true {
		t.Errorf("Expected active to be true, got %v", active)
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

	expectedString := "id, name, email"
	if columns.String() != expectedString {
		t.Errorf("Expected String() to return %q, got %q", expectedString, columns.String())
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

func TestConnectionOptions(t *testing.T) {
	ctx := context.Background()
	opts := sqlx.DefaultConnectionOptions().
		WithContext(ctx).
		WithTimeout(30*time.Second).
		WithRetry(5, 200*time.Millisecond)

	if opts.Context != ctx {
		t.Error("Expected context to be set")
	}

	if opts.Timeout != 30*time.Second {
		t.Errorf("Expected timeout to be 30s, got %v", opts.Timeout)
	}

	if opts.RetryCount != 5 {
		t.Errorf("Expected RetryCount to be 5, got %d", opts.RetryCount)
	}

	if opts.RetryDelay != 200*time.Millisecond {
		t.Errorf("Expected RetryDelay to be 200ms, got %v", opts.RetryDelay)
	}
}

func TestErrorHelpers(t *testing.T) {
	// Test error creation
	err := sqlx.NewError("test operation", sqlx.ErrInvalidConfig)

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	// Test error wrapping
	if !sqlx.Is(err, sqlx.ErrInvalidConfig) {
		t.Error("Expected error to wrap ErrInvalidConfig")
	}

	// Test error with query
	errWithQuery := sqlx.NewErrorWithQuery(
		"query operation",
		sqlx.ErrInvalidQuery,
		"SELECT * FROM users",
		[]any{1, "test"},
	)

	if errWithQuery == nil {
		t.Fatal("Expected error with query, got nil")
	}
}
