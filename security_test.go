package sqlx_test

import (
	"testing"

	"github.com/dongrv/sqlx"
)

func TestMySQLIdentifierEscaper(t *testing.T) {
	escaper := &sqlx.MySQLIdentifierEscaper{}

	tests := []struct {
		name        string
		identifier  string
		wantErr     bool
		description string
	}{
		// Valid identifiers
		{"valid simple", "users", false, "simple table name"},
		{"valid with underscore", "user_table", false, "table name with underscore"},
		{"valid with numbers", "user123", false, "table name with numbers"},
		{"valid uppercase", "USERS", false, "uppercase table name"},

		// Invalid identifiers
		{"empty", "", true, "empty identifier"},
		{"starts with number", "123users", true, "starts with number"},
		{"contains dash", "user-table", true, "contains dash"},
		{"contains space", "user table", true, "contains space"},
		{"contains dot", "user.table", true, "contains dot"},
		{"contains special char", "user@table", true, "contains special char"},
		{"too long", "a_very_long_table_name_that_exceeds_the_maximum_length_of_sixty_four_characters", true, "exceeds 64 characters"},

		// Reserved words
		{"reserved SELECT", "SELECT", true, "SQL keyword SELECT"},
		{"reserved FROM", "FROM", true, "SQL keyword FROM"},
		{"reserved WHERE", "WHERE", true, "SQL keyword WHERE"},
		{"reserved INSERT", "INSERT", true, "SQL keyword INSERT"},
		{"reserved UPDATE", "UPDATE", true, "SQL keyword UPDATE"},
		{"reserved DELETE", "DELETE", true, "SQL keyword DELETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := escaper.Validate(tt.identifier)
			if (err != nil) != tt.wantErr {
				t.Errorf("MySQLIdentifierEscaper.Validate(%q) error = %v, wantErr %v", tt.identifier, err, tt.wantErr)
			}

			// Test escape if validation passes
			if err == nil {
				escaped, err := escaper.Escape(tt.identifier)
				if err != nil {
					t.Errorf("MySQLIdentifierEscaper.Escape(%q) unexpected error: %v", tt.identifier, err)
				}

				// Check that escaped string contains backticks
				if len(escaped) < 2 || escaped[0] != '`' || escaped[len(escaped)-1] != '`' {
					t.Errorf("MySQLIdentifierEscaper.Escape(%q) = %q, should be wrapped in backticks", tt.identifier, escaped)
				}

				// Check quote character
				if escaper.QuoteChar() != "`" {
					t.Errorf("MySQLIdentifierEscaper.QuoteChar() = %q, want `", escaper.QuoteChar())
				}
			}
		})
	}
}

func TestMySQLIdentifierEscaperEscape(t *testing.T) {
	escaper := &sqlx.MySQLIdentifierEscaper{}

	tests := []struct {
		name       string
		identifier string
		want       string
	}{
		{"simple", "users", "`users`"},
		{"with underscore", "user_table", "`user_table`"},
		{"with backtick", "user_table", "`user_table`"},
		{"multiple underscores", "user__table", "`user__table`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := escaper.Escape(tt.identifier)
			if err != nil {
				t.Errorf("MySQLIdentifierEscaper.Escape(%q) error = %v", tt.identifier, err)
				return
			}
			if got != tt.want {
				t.Errorf("MySQLIdentifierEscaper.Escape(%q) = %q, want %q", tt.identifier, got, tt.want)
			}
		})
	}
}

func TestPostgreSQLIdentifierEscaper(t *testing.T) {
	escaper := &sqlx.PostgreSQLIdentifierEscaper{}

	tests := []struct {
		name        string
		identifier  string
		wantErr     bool
		description string
	}{
		// Valid identifiers
		{"valid simple", "users", false, "simple table name"},
		{"valid with underscore", "user_table", false, "table name with underscore"},
		{"valid with numbers", "user123", false, "table name with numbers"},

		// Invalid identifiers
		{"empty", "", true, "empty identifier"},
		{"starts with number", "123users", true, "starts with number"},
		{"contains dash", "user-table", true, "contains dash"},
		{"too long", "a_very_long_table_name_that_exceeds_the_postgresql_default_limit_of_sixty_three_characters_yes", true, "exceeds 63 characters"},

		// Reserved words
		{"reserved SELECT", "SELECT", true, "SQL keyword SELECT"},
		{"reserved FROM", "FROM", true, "SQL keyword FROM"},
		{"reserved WHERE", "WHERE", true, "SQL keyword WHERE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := escaper.Validate(tt.identifier)
			if (err != nil) != tt.wantErr {
				t.Errorf("PostgreSQLIdentifierEscaper.Validate(%q) error = %v, wantErr %v", tt.identifier, err, tt.wantErr)
			}

			// Test escape if validation passes
			if err == nil {
				escaped, err := escaper.Escape(tt.identifier)
				if err != nil {
					t.Errorf("PostgreSQLIdentifierEscaper.Escape(%q) unexpected error: %v", tt.identifier, err)
				}

				// Check that escaped string contains double quotes
				if len(escaped) < 2 || escaped[0] != '"' || escaped[len(escaped)-1] != '"' {
					t.Errorf("PostgreSQLIdentifierEscaper.Escape(%q) = %q, should be wrapped in double quotes", tt.identifier, escaped)
				}

				// Check quote character
				if escaper.QuoteChar() != "\"" {
					t.Errorf("PostgreSQLIdentifierEscaper.QuoteChar() = %q, want \"", escaper.QuoteChar())
				}
			}
		})
	}
}

func TestPostgreSQLIdentifierEscaperEscape(t *testing.T) {
	escaper := &sqlx.PostgreSQLIdentifierEscaper{}

	tests := []struct {
		name       string
		identifier string
		want       string
	}{
		{"simple", "users", "\"users\""},
		{"with underscore", "user_table", "\"user_table\""},
		{"with underscore", "user_table", "\"user_table\""},
		{"multiple underscores", "user__table", "\"user__table\""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := escaper.Escape(tt.identifier)
			if err != nil {
				t.Errorf("PostgreSQLIdentifierEscaper.Escape(%q) error = %v", tt.identifier, err)
				return
			}
			if got != tt.want {
				t.Errorf("PostgreSQLIdentifierEscaper.Escape(%q) = %q, want %q", tt.identifier, got, tt.want)
			}
		})
	}
}

func TestSQLiteIdentifierEscaper(t *testing.T) {
	escaper := &sqlx.SQLiteIdentifierEscaper{}

	tests := []struct {
		name        string
		identifier  string
		wantErr     bool
		description string
	}{
		// Valid identifiers
		{"valid simple", "users", false, "simple table name"},
		{"valid with underscore", "user_table", false, "table name with underscore"},
		{"valid with numbers", "user123", false, "table name with numbers"},

		// Invalid identifiers
		{"empty", "", true, "empty identifier"},
		{"starts with number", "123users", true, "starts with number"},
		{"contains dash", "user-table", true, "contains dash"},

		// Reserved words
		{"reserved SELECT", "SELECT", true, "SQL keyword SELECT"},
		{"reserved FROM", "FROM", true, "SQL keyword FROM"},
		{"reserved WHERE", "WHERE", true, "SQL keyword WHERE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := escaper.Validate(tt.identifier)
			if (err != nil) != tt.wantErr {
				t.Errorf("SQLiteIdentifierEscaper.Validate(%q) error = %v, wantErr %v", tt.identifier, err, tt.wantErr)
			}

			// Test escape if validation passes
			if err == nil {
				escaped, err := escaper.Escape(tt.identifier)
				if err != nil {
					t.Errorf("SQLiteIdentifierEscaper.Escape(%q) unexpected error: %v", tt.identifier, err)
				}

				// Check that escaped string contains double quotes
				if len(escaped) < 2 || escaped[0] != '"' || escaped[len(escaped)-1] != '"' {
					t.Errorf("SQLiteIdentifierEscaper.Escape(%q) = %q, should be wrapped in double quotes", tt.identifier, escaped)
				}

				// Check quote character
				if escaper.QuoteChar() != "\"" {
					t.Errorf("SQLiteIdentifierEscaper.QuoteChar() = %q, want \"", escaper.QuoteChar())
				}
			}
		})
	}
}

func TestGetIdentifierEscaper(t *testing.T) {
	tests := []struct {
		name   string
		driver sqlx.Driver
		want   string
	}{
		{"MySQL", sqlx.MySQL, "`"},
		{"PostgreSQL", sqlx.PostgreSQL, "\""},
		{"SQLite", sqlx.SQLite, "\""},
		{"Unknown", sqlx.Driver("unknown"), "`"}, // Defaults to MySQL
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			escaper := sqlx.GetIdentifierEscaper(tt.driver)
			if escaper.QuoteChar() != tt.want {
				t.Errorf("GetIdentifierEscaper(%v).QuoteChar() = %q, want %q", tt.driver, escaper.QuoteChar(), tt.want)
			}
		})
	}
}

func TestSafeIdentifier(t *testing.T) {
	// Test valid identifier
	si, err := sqlx.NewSafeIdentifier(sqlx.MySQL, "users")
	if err != nil {
		t.Fatalf("NewSafeIdentifier failed: %v", err)
	}

	if si.String() != "`users`" {
		t.Errorf("SafeIdentifier.String() = %q, want `users`", si.String())
	}

	if si.Original() != "users" {
		t.Errorf("SafeIdentifier.Original() = %q, want users", si.Original())
	}

	if si.Driver() != sqlx.MySQL {
		t.Errorf("SafeIdentifier.Driver() = %v, want MySQL", si.Driver())
	}

	if !si.IsValid() {
		t.Error("SafeIdentifier.IsValid() = false, want true")
	}

	if si.QuoteChar() != "`" {
		t.Errorf("SafeIdentifier.QuoteChar() = %q, want `", si.QuoteChar())
	}

	// Test invalid identifier
	_, err = sqlx.NewSafeIdentifier(sqlx.MySQL, "123users")
	if err == nil {
		t.Error("NewSafeIdentifier with invalid identifier should have failed")
	}

	// Test MustSafeIdentifier with valid identifier
	si = sqlx.MustSafeIdentifier(sqlx.MySQL, "products")
	if si.String() != "`products`" {
		t.Errorf("MustSafeIdentifier.String() = %q, want `products`", si.String())
	}

	// Test MustSafeIdentifier panics with invalid identifier
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustSafeIdentifier should have panicked with invalid identifier")
		}
	}()
	sqlx.MustSafeIdentifier(sqlx.MySQL, "123invalid")
}

func TestEscapeFunctions(t *testing.T) {
	driver := sqlx.MySQL

	// Test EscapeTableName
	escaped, err := sqlx.EscapeTableName(driver, "users")
	if err != nil {
		t.Fatalf("EscapeTableName failed: %v", err)
	}
	if escaped != "`users`" {
		t.Errorf("EscapeTableName() = %q, want `users`", escaped)
	}

	// Test EscapeColumnName
	escaped, err = sqlx.EscapeColumnName(driver, "user_id")
	if err != nil {
		t.Fatalf("EscapeColumnName failed: %v", err)
	}
	if escaped != "`user_id`" {
		t.Errorf("EscapeColumnName() = %q, want `user_id`", escaped)
	}

	// Test EscapeIdentifier
	escaped, err = sqlx.EscapeIdentifier(driver, "created_at")
	if err != nil {
		t.Fatalf("EscapeIdentifier failed: %v", err)
	}
	if escaped != "`created_at`" {
		t.Errorf("EscapeIdentifier() = %q, want `created_at`", escaped)
	}

	// Test Must functions
	tableName := sqlx.MustEscapeTableName(driver, "orders")
	if tableName != "`orders`" {
		t.Errorf("MustEscapeTableName() = %q, want `orders`", tableName)
	}

	columnName := sqlx.MustEscapeColumnName(driver, "order_date")
	if columnName != "`order_date`" {
		t.Errorf("MustEscapeColumnName() = %q, want `order_date`", columnName)
	}

	identifier := sqlx.MustEscapeIdentifier(driver, "total_amount")
	if identifier != "`total_amount`" {
		t.Errorf("MustEscapeIdentifier() = %q, want `total_amount`", identifier)
	}
}

func TestValidateFunctions(t *testing.T) {
	driver := sqlx.MySQL

	// Test valid identifier
	err := sqlx.ValidateTableName(driver, "users")
	if err != nil {
		t.Errorf("ValidateTableName with valid name failed: %v", err)
	}

	err = sqlx.ValidateColumnName(driver, "user_name")
	if err != nil {
		t.Errorf("ValidateColumnName with valid name failed: %v", err)
	}

	err = sqlx.ValidateIdentifier(driver, "created_at")
	if err != nil {
		t.Errorf("ValidateIdentifier with valid name failed: %v", err)
	}

	// Test invalid identifier
	err = sqlx.ValidateTableName(driver, "123users")
	if err == nil {
		t.Error("ValidateTableName with invalid name should have failed")
	}

	err = sqlx.ValidateColumnName(driver, "user-name")
	if err == nil {
		t.Error("ValidateColumnName with invalid name should have failed")
	}

	err = sqlx.ValidateIdentifier(driver, "SELECT")
	if err == nil {
		t.Error("ValidateIdentifier with reserved word should have failed")
	}
}

func TestSafeIdentifierList(t *testing.T) {
	driver := sqlx.MySQL

	// Test creating list
	list, err := sqlx.NewSafeIdentifierList(driver, "id", "name", "email")
	if err != nil {
		t.Fatalf("NewSafeIdentifierList failed: %v", err)
	}

	if list.Length() != 3 {
		t.Errorf("SafeIdentifierList.Length() = %d, want 3", list.Length())
	}

	// Test Strings method
	strings := list.Strings()
	expected := []string{"`id`", "`name`", "`email`"}
	for i, s := range strings {
		if s != expected[i] {
			t.Errorf("Strings()[%d] = %q, want %q", i, s, expected[i])
		}
	}

	// Test Originals method
	originals := list.Originals()
	expectedOriginals := []string{"id", "name", "email"}
	for i, s := range originals {
		if s != expectedOriginals[i] {
			t.Errorf("Originals()[%d] = %q, want %q", i, s, expectedOriginals[i])
		}
	}

	// Test Join method
	joined := list.Join(", ")
	if joined != "`id`, `name`, `email`" {
		t.Errorf("Join() = %q, want `id`, `name`, `email`", joined)
	}

	// Test Get method
	si := list.Get(1)
	if si == nil {
		t.Fatal("Get(1) returned nil")
	}
	if si.String() != "`name`" {
		t.Errorf("Get(1).String() = %q, want `name`", si.String())
	}

	// Test Contains method
	if !list.Contains("name") {
		t.Error("Contains(\"name\") = false, want true")
	}
	if list.Contains("nonexistent") {
		t.Error("Contains(\"nonexistent\") = true, want false")
	}

	// Test Add method
	err = list.Add("age")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if list.Length() != 4 {
		t.Errorf("After Add, Length() = %d, want 4", list.Length())
	}
	if !list.Contains("age") {
		t.Error("After Add, Contains(\"age\") = false, want true")
	}

	// Test Clear method
	list.Clear()
	if list.Length() != 0 {
		t.Errorf("After Clear, Length() = %d, want 0", list.Length())
	}
}
