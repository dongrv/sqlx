package sqlx

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// IdentifierEscaper defines the interface for escaping SQL identifiers.
type IdentifierEscaper interface {
	// Escape escapes a SQL identifier for safe use in queries.
	Escape(identifier string) (string, error)

	// Validate validates if an identifier is safe to use.
	Validate(identifier string) error

	// QuoteChar returns the quote character used by this escaper.
	QuoteChar() string
}

// MySQLIdentifierEscaper implements IdentifierEscaper for MySQL.
type MySQLIdentifierEscaper struct{}

// Escape escapes a MySQL identifier using backticks.
func (e *MySQLIdentifierEscaper) Escape(identifier string) (string, error) {
	if err := e.Validate(identifier); err != nil {
		return "", err
	}

	// Escape backticks by doubling them
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return "`" + escaped + "`", nil
}

// Validate validates a MySQL identifier.
func (e *MySQLIdentifierEscaper) Validate(identifier string) error {
	if identifier == "" {
		return fmt.Errorf("%w: identifier cannot be empty", ErrInvalidIdentifier)
	}

	// Check length (MySQL max identifier length is 64 bytes for most identifiers)
	if utf8.RuneCountInString(identifier) > 64 {
		return fmt.Errorf("%w: identifier exceeds maximum length of 64 characters", ErrInvalidIdentifier)
	}

	// MySQL allows letters, digits, dollar sign, underscore, and national characters
	// but we'll be more restrictive for safety
	validPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if !validPattern.MatchString(identifier) {
		return fmt.Errorf("%w: identifier %q contains invalid characters", ErrInvalidIdentifier, identifier)
	}

	// Check for SQL keywords
	if isMySQLReservedWord(identifier) {
		return fmt.Errorf("%w: identifier %q is a MySQL reserved word", ErrInvalidIdentifier, identifier)
	}

	return nil
}

// QuoteChar returns the quote character used by MySQL.
func (e *MySQLIdentifierEscaper) QuoteChar() string {
	return "`"
}

// PostgreSQLIdentifierEscaper implements IdentifierEscaper for PostgreSQL.
type PostgreSQLIdentifierEscaper struct{}

// Escape escapes a PostgreSQL identifier using double quotes.
func (e *PostgreSQLIdentifierEscaper) Escape(identifier string) (string, error) {
	if err := e.Validate(identifier); err != nil {
		return "", err
	}

	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return `"` + escaped + `"`, nil
}

// Validate validates a PostgreSQL identifier.
func (e *PostgreSQLIdentifierEscaper) Validate(identifier string) error {
	if identifier == "" {
		return fmt.Errorf("%w: identifier cannot be empty", ErrInvalidIdentifier)
	}

	// PostgreSQL max identifier length is 63 bytes by default
	if utf8.RuneCountInString(identifier) > 63 {
		return fmt.Errorf("%w: identifier exceeds maximum length of 63 characters", ErrInvalidIdentifier)
	}

	// PostgreSQL is more permissive than MySQL, but we'll still validate
	// It allows any characters if quoted, but we'll validate unquoted identifiers
	validPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if !validPattern.MatchString(identifier) {
		return fmt.Errorf("%w: identifier %q contains invalid characters for unquoted identifier", ErrInvalidIdentifier, identifier)
	}

	// Check for PostgreSQL keywords
	if isPostgreSQLReservedWord(identifier) {
		return fmt.Errorf("%w: identifier %q is a PostgreSQL reserved word", ErrInvalidIdentifier, identifier)
	}

	return nil
}

// QuoteChar returns the quote character used by PostgreSQL.
func (e *PostgreSQLIdentifierEscaper) QuoteChar() string {
	return `"`
}

// SQLiteIdentifierEscaper implements IdentifierEscaper for SQLite.
type SQLiteIdentifierEscaper struct{}

// Escape escapes a SQLite identifier using double quotes.
func (e *SQLiteIdentifierEscaper) Escape(identifier string) (string, error) {
	if err := e.Validate(identifier); err != nil {
		return "", err
	}

	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return `"` + escaped + `"`, nil
}

// Validate validates a SQLite identifier.
func (e *SQLiteIdentifierEscaper) Validate(identifier string) error {
	if identifier == "" {
		return fmt.Errorf("%w: identifier cannot be empty", ErrInvalidIdentifier)
	}

	// SQLite doesn't have a strict length limit, but we'll impose a reasonable limit
	if utf8.RuneCountInString(identifier) > 255 {
		return fmt.Errorf("%w: identifier exceeds maximum length of 255 characters", ErrInvalidIdentifier)
	}

	// SQLite allows letters, digits, and underscore for unquoted identifiers
	validPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if !validPattern.MatchString(identifier) {
		return fmt.Errorf("%w: identifier %q contains invalid characters for unquoted identifier", ErrInvalidIdentifier, identifier)
	}

	// Check for SQLite keywords
	if isSQLiteReservedWord(identifier) {
		return fmt.Errorf("%w: identifier %q is a SQLite reserved word", ErrInvalidIdentifier, identifier)
	}

	return nil
}

// QuoteChar returns the quote character used by SQLite.
func (e *SQLiteIdentifierEscaper) QuoteChar() string {
	return `"`
}

// DefaultIdentifierEscaper returns the default identifier escaper for MySQL.
func DefaultIdentifierEscaper() IdentifierEscaper {
	return &MySQLIdentifierEscaper{}
}

// GetIdentifierEscaper returns an appropriate identifier escaper for the given driver.
func GetIdentifierEscaper(driver Driver) IdentifierEscaper {
	switch driver {
	case MySQL:
		return &MySQLIdentifierEscaper{}
	case PostgreSQL:
		return &PostgreSQLIdentifierEscaper{}
	case SQLite:
		return &SQLiteIdentifierEscaper{}
	default:
		// Default to MySQL escaper for unknown drivers
		return &MySQLIdentifierEscaper{}
	}
}

// isMySQLReservedWord checks if a word is a MySQL reserved word.
func isMySQLReservedWord(word string) bool {
	word = strings.ToUpper(word)

	// Common MySQL reserved words (partial list)
	reservedWords := map[string]bool{
		"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
		"FROM": true, "WHERE": true, "AND": true, "OR": true, "NOT": true,
		"INTO": true, "VALUES": true, "SET": true, "CREATE": true, "DROP": true,
		"TABLE": true, "DATABASE": true, "INDEX": true, "VIEW": true,
		"ALTER": true, "TRUNCATE": true, "JOIN": true, "LEFT": true, "RIGHT": true,
		"INNER": true, "OUTER": true, "ON": true, "AS": true, "IS": true,
		"NULL": true, "NOTNULL": true, "LIKE": true, "BETWEEN": true,
		"ORDER": true, "BY": true, "GROUP": true, "HAVING": true,
		"LIMIT": true, "OFFSET": true, "UNION": true, "ALL": true,
		"DISTINCT": true, "COUNT": true, "SUM": true, "AVG": true,
		"MIN": true, "MAX": true, "EXISTS": true, "CASE": true,
		"WHEN": true, "THEN": true, "ELSE": true, "END": true,
		"PRIMARY": true, "KEY": true, "FOREIGN": true, "REFERENCES": true,
		"UNIQUE": true, "CHECK": true, "DEFAULT": true, "AUTO_INCREMENT": true,
		"COMMENT": true, "ENGINE": true, "CHARSET": true, "COLLATE": true,
		"ROW_FORMAT": true, "PARTITION": true, "SUBPARTITION": true,
		"PROCEDURE": true, "FUNCTION": true, "TRIGGER": true, "EVENT": true,
		"BEGIN": true, "DECLARE": true, "IF": true, "WHILE": true, "LOOP": true,
		"REPEAT": true, "UNTIL": true, "LEAVE": true, "ITERATE": true,
		"CALL": true, "RETURN": true, "SIGNAL": true, "RESIGNAL": true,
		"GET": true, "DIAGNOSTICS": true, "CONDITION": true, "SQLSTATE": true,
		"SQLWARNING": true, "SQLEXCEPTION": true, "DEALLOCATE": true,
		"PREPARE": true, "EXECUTE": true, "DESCRIBE": true, "EXPLAIN": true,
		"SHOW": true, "USE": true, "HELP": true, "SOURCE": true,
		"LOAD": true, "DATA": true, "INFILE": true, "OUTFILE": true,
		"DUMPFILE": true, "REPLACE": true, "IGNORE": true, "LOCK": true,
		"TABLES": true, "READ": true, "WRITE": true,
		"LOCAL": true, "GLOBAL": true, "SESSION": true, "SYSTEM": true,
		"VARIABLES": true, "STATUS": true, "PROCESSLIST": true,
		"KILL": true, "FLUSH": true, "RESET": true, "PURGE": true,
		"OPTIMIZE": true, "ANALYZE": true, "CHECKSUM": true, "REPAIR": true,
		"BACKUP": true, "RESTORE": true, "INSTALL": true, "UNINSTALL": true,
		"PLUGIN": true, "USER": true, "ROLE": true, "GRANT": true,
		"REVOKE": true, "PRIVILEGES": true, "WITH": true, "ADMIN": true,
		"OPTION": true, "IDENTIFIED": true, "PASSWORD": true,
		"EXPIRED": true, "ACCOUNT": true,
		"REQUIRE": true, "SSL": true, "X509": true, "CIPHER": true,
		"ISSUER": true, "SUBJECT": true, "SAN": true, "MAX_QUERIES_PER_HOUR": true,
		"MAX_UPDATES_PER_HOUR": true, "MAX_CONNECTIONS_PER_HOUR": true,
		"MAX_USER_CONNECTIONS": true, "PROFILE": true, "PROFILES": true,
		"DEFAULT_ROLE": true, "ROLE_ADMIN": true, "SET_ROLE": true,
		"CURRENT_ROLE": true, "ACTIVE_ROLE": true, "MANDATORY_ROLES": true,
		"OPTIONAL_ROLES": true, "NONE": true, "EXCEPT": true,
	}

	return reservedWords[word]
}

// isPostgreSQLReservedWord checks if a word is a PostgreSQL reserved word.
func isPostgreSQLReservedWord(word string) bool {
	word = strings.ToUpper(word)

	// Common PostgreSQL reserved words (partial list)
	reservedWords := map[string]bool{
		"ALL": true, "ANALYSE": true, "ANALYZE": true, "AND": true,
		"ANY": true, "ARRAY": true, "AS": true, "ASC": true,
		"ASYMMETRIC": true, "AUTHORIZATION": true, "BINARY": true,
		"BOTH": true, "CASE": true, "CAST": true, "CHECK": true,
		"COLLATE": true, "COLUMN": true, "CONSTRAINT": true,
		"CREATE": true, "CROSS": true, "CURRENT_CATALOG": true,
		"CURRENT_DATE": true, "CURRENT_ROLE": true, "CURRENT_SCHEMA": true,
		"CURRENT_TIME": true, "CURRENT_TIMESTAMP": true, "CURRENT_USER": true,
		"DEFAULT": true, "DEFERRABLE": true, "DESC": true,
		"DISTINCT": true, "DO": true, "ELSE": true, "END": true,
		"EXCEPT": true, "FALSE": true, "FOR": true, "FOREIGN": true,
		"FROM": true, "GRANT": true, "GROUP": true, "HAVING": true,
		"IN": true, "INITIALLY": true, "INTERSECT": true, "INTO": true,
		"LATERAL": true, "LEADING": true, "LIMIT": true, "LOCALTIME": true,
		"LOCALTIMESTAMP": true, "NOT": true, "NULL": true, "OFFSET": true,
		"ON": true, "ONLY": true, "OR": true, "ORDER": true,
		"PLACING": true, "PRIMARY": true, "REFERENCES": true,
		"RETURNING": true, "SELECT": true, "SESSION_USER": true,
		"SOME": true, "SYMMETRIC": true, "TABLE": true, "THEN": true,
		"TO": true, "TRAILING": true, "TRUE": true, "UNION": true,
		"UNIQUE": true, "USER": true, "USING": true, "VARIADIC": true,
		"WHEN": true, "WHERE": true, "WINDOW": true, "WITH": true,
	}

	return reservedWords[word]
}

// isSQLiteReservedWord checks if a word is a SQLite reserved word.
func isSQLiteReservedWord(word string) bool {
	word = strings.ToUpper(word)

	// SQLite reserved words
	reservedWords := map[string]bool{
		"ABORT": true, "ACTION": true, "ADD": true, "AFTER": true,
		"ALL": true, "ALTER": true, "ANALYZE": true, "AND": true,
		"AS": true, "ASC": true, "ATTACH": true, "AUTOINCREMENT": true,
		"BEFORE": true, "BEGIN": true, "BETWEEN": true, "BY": true,
		"CASCADE": true, "CASE": true, "CAST": true, "CHECK": true,
		"COLLATE": true, "COLUMN": true, "COMMIT": true, "CONFLICT": true,
		"CONSTRAINT": true, "CREATE": true, "CROSS": true, "CURRENT_DATE": true,
		"CURRENT_TIME": true, "CURRENT_TIMESTAMP": true, "DATABASE": true,
		"DEFAULT": true, "DEFERRABLE": true, "DEFERRED": true, "DELETE": true,
		"DESC": true, "DETACH": true, "DISTINCT": true, "DROP": true,
		"EACH": true, "ELSE": true, "END": true, "ESCAPE": true,
		"EXCEPT": true, "EXCLUSIVE": true, "EXISTS": true, "EXPLAIN": true,
		"FAIL": true, "FOR": true, "FOREIGN": true, "FROM": true,
		"FULL": true, "GLOB": true, "GROUP": true, "HAVING": true,
		"IF": true, "IGNORE": true, "IMMEDIATE": true, "IN": true,
		"INDEX": true, "INDEXED": true, "INITIALLY": true, "INNER": true,
		"INSERT": true, "INSTEAD": true, "INTERSECT": true, "INTO": true,
		"IS": true, "ISNULL": true, "JOIN": true, "KEY": true,
		"LEFT": true, "LIKE": true, "LIMIT": true, "MATCH": true,
		"NATURAL": true, "NO": true, "NOT": true, "NOTNULL": true,
		"NULL": true, "OF": true, "OFFSET": true, "ON": true,
		"OR": true, "ORDER": true, "OUTER": true, "PLAN": true,
		"PRAGMA": true, "PRIMARY": true, "QUERY": true, "RAISE": true,
		"RECURSIVE": true, "REFERENCES": true, "REGEXP": true,
		"REINDEX": true, "RELEASE": true, "RENAME": true, "REPLACE": true,
		"RESTRICT": true, "RIGHT": true, "ROLLBACK": true, "ROW": true,
		"SAVEPOINT": true, "SELECT": true, "SET": true, "TABLE": true,
		"TEMP": true, "TEMPORARY": true, "THEN": true, "TO": true,
		"TRANSACTION": true, "TRIGGER": true, "UNION": true, "UNIQUE": true,
		"UPDATE": true, "USING": true, "VACUUM": true, "VALUES": true,
		"VIEW": true, "VIRTUAL": true, "WHEN": true, "WHERE": true,
		"WITH": true, "WITHOUT": true,
	}

	return reservedWords[word]
}

// SafeIdentifier represents a validated and escaped SQL identifier.
type SafeIdentifier struct {
	// original is the original identifier string
	original string

	// escaped is the escaped identifier string
	escaped string

	// escaper is the identifier escaper used
	escaper IdentifierEscaper

	// driver is the database driver
	driver Driver
}

// NewSafeIdentifier creates a new SafeIdentifier for the given driver.
func NewSafeIdentifier(driver Driver, identifier string) (*SafeIdentifier, error) {
	escaper := GetIdentifierEscaper(driver)
	escaped, err := escaper.Escape(identifier)
	if err != nil {
		return nil, err
	}

	return &SafeIdentifier{
		original: identifier,
		escaped:  escaped,
		escaper:  escaper,
		driver:   driver,
	}, nil
}

// MustSafeIdentifier creates a new SafeIdentifier, panicking on error.
// Use only when you are certain the identifier is valid.
func MustSafeIdentifier(driver Driver, identifier string) *SafeIdentifier {
	si, err := NewSafeIdentifier(driver, identifier)
	if err != nil {
		panic(err)
	}
	return si
}

// String returns the escaped identifier string.
func (si *SafeIdentifier) String() string {
	return si.escaped
}

// Original returns the original identifier string.
func (si *SafeIdentifier) Original() string {
	return si.original
}

// Driver returns the database driver.
func (si *SafeIdentifier) Driver() Driver {
	return si.driver
}

// Validate validates the identifier.
func (si *SafeIdentifier) Validate() error {
	return si.escaper.Validate(si.original)
}

// QuoteChar returns the quote character used.
func (si *SafeIdentifier) QuoteChar() string {
	return si.escaper.QuoteChar()
}

// IsValid returns true if the identifier is valid.
func (si *SafeIdentifier) IsValid() bool {
	return si.escaper.Validate(si.original) == nil
}

// EscapeTableName creates a safe table identifier.
func EscapeTableName(driver Driver, tableName string) (string, error) {
	si, err := NewSafeIdentifier(driver, tableName)
	if err != nil {
		return "", err
	}
	return si.String(), nil
}

// EscapeColumnName creates a safe column identifier.
func EscapeColumnName(driver Driver, columnName string) (string, error) {
	si, err := NewSafeIdentifier(driver, columnName)
	if err != nil {
		return "", err
	}
	return si.String(), nil
}

// EscapeIdentifier creates a safe SQL identifier.
func EscapeIdentifier(driver Driver, identifier string) (string, error) {
	si, err := NewSafeIdentifier(driver, identifier)
	if err != nil {
		return "", err
	}
	return si.String(), nil
}

// MustEscapeTableName creates a safe table identifier, panicking on error.
func MustEscapeTableName(driver Driver, tableName string) string {
	return MustSafeIdentifier(driver, tableName).String()
}

// MustEscapeColumnName creates a safe column identifier, panicking on error.
func MustEscapeColumnName(driver Driver, columnName string) string {
	return MustSafeIdentifier(driver, columnName).String()
}

// MustEscapeIdentifier creates a safe SQL identifier, panicking on error.
func MustEscapeIdentifier(driver Driver, identifier string) string {
	return MustSafeIdentifier(driver, identifier).String()
}

// ValidateTableName validates a table name.
func ValidateTableName(driver Driver, tableName string) error {
	escaper := GetIdentifierEscaper(driver)
	return escaper.Validate(tableName)
}

// ValidateColumnName validates a column name.
func ValidateColumnName(driver Driver, columnName string) error {
	escaper := GetIdentifierEscaper(driver)
	return escaper.Validate(columnName)
}

// ValidateIdentifier validates a SQL identifier.
func ValidateIdentifier(driver Driver, identifier string) error {
	escaper := GetIdentifierEscaper(driver)
	return escaper.Validate(identifier)
}

// SafeIdentifierList represents a list of safe identifiers.
type SafeIdentifierList struct {
	identifiers []*SafeIdentifier
	driver      Driver
}

// NewSafeIdentifierList creates a new SafeIdentifierList.
func NewSafeIdentifierList(driver Driver, identifiers ...string) (*SafeIdentifierList, error) {
	list := &SafeIdentifierList{
		driver: driver,
	}

	for _, id := range identifiers {
		si, err := NewSafeIdentifier(driver, id)
		if err != nil {
			return nil, err
		}
		list.identifiers = append(list.identifiers, si)
	}

	return list, nil
}

// Add adds an identifier to the list.
func (sil *SafeIdentifierList) Add(identifier string) error {
	si, err := NewSafeIdentifier(sil.driver, identifier)
	if err != nil {
		return err
	}
	sil.identifiers = append(sil.identifiers, si)
	return nil
}

// Strings returns the escaped identifier strings.
func (sil *SafeIdentifierList) Strings() []string {
	result := make([]string, len(sil.identifiers))
	for i, si := range sil.identifiers {
		result[i] = si.String()
	}
	return result
}

// Originals returns the original identifier strings.
func (sil *SafeIdentifierList) Originals() []string {
	result := make([]string, len(sil.identifiers))
	for i, si := range sil.identifiers {
		result[i] = si.Original()
	}
	return result
}

// Join joins the escaped identifiers with a separator.
func (sil *SafeIdentifierList) Join(sep string) string {
	escaped := sil.Strings()
	return strings.Join(escaped, sep)
}

// Length returns the number of identifiers in the list.
func (sil *SafeIdentifierList) Length() int {
	return len(sil.identifiers)
}

// Get returns the SafeIdentifier at the specified index.
func (sil *SafeIdentifierList) Get(index int) *SafeIdentifier {
	if index < 0 || index >= len(sil.identifiers) {
		return nil
	}
	return sil.identifiers[index]
}

// Contains checks if the list contains an identifier.
func (sil *SafeIdentifierList) Contains(identifier string) bool {
	for _, si := range sil.identifiers {
		if si.Original() == identifier {
			return true
		}
	}
	return false
}

// Clear clears all identifiers from the list.
func (sil *SafeIdentifierList) Clear() {
	sil.identifiers = nil
}
