package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// RowScanner defines the interface for scanning a single row.
type RowScanner interface {
	Scan(dest ...interface{}) error
}

// RowsScanner defines the interface for scanning multiple rows.
type RowsScanner interface {
	Scan(dest ...interface{}) error
	Next() bool
	Err() error
	Close() error
}

// ColumnList is a slice of column names with helper methods.
type ColumnList []string

// NewColumnList creates a new ColumnList.
func NewColumnList(columns ...string) ColumnList {
	return columns
}

// Add adds columns to the list.
func (c *ColumnList) Add(columns ...string) {
	*c = append(*c, columns...)
}

// Remove removes a column from the list.
func (c *ColumnList) Remove(column string) {
	for i, col := range *c {
		if col == column {
			*c = append((*c)[:i], (*c)[i+1:]...)
			return
		}
	}
}

// Contains checks if the list contains a column.
func (c ColumnList) Contains(column string) bool {
	for _, col := range c {
		if col == column {
			return true
		}
	}
	return false
}

// String returns the columns as a comma-separated string.
func (c ColumnList) String() string {
	if len(c) == 0 {
		return "*"
	}
	escaped := make([]string, len(c))
	for i, col := range c {
		escaped[i] = escapeIdentifier(col)
	}
	return strings.Join(escaped, ", ")
}

// QueryOptions represents options for query execution.
type QueryOptions struct {
	// Timeout for the query execution.
	Timeout time.Duration

	// MaxRows limits the number of rows returned.
	MaxRows int

	// SkipValidation skips parameter validation.
	SkipValidation bool
}

// DefaultQueryOptions returns default query options.
func DefaultQueryOptions() QueryOptions {
	return QueryOptions{
		Timeout: 30 * time.Second,
		MaxRows: 0, // No limit
	}
}

// WithTimeout returns a copy with the given timeout.
func (o QueryOptions) WithTimeout(timeout time.Duration) QueryOptions {
	o.Timeout = timeout
	return o
}

// WithMaxRows returns a copy with the given max rows limit.
func (o QueryOptions) WithMaxRows(maxRows int) QueryOptions {
	o.MaxRows = maxRows
	return o
}

// TransactionOptions represents options for transaction execution.
type TransactionOptions struct {
	// Isolation level for the transaction.
	Isolation sql.IsolationLevel

	// ReadOnly specifies if the transaction is read-only.
	ReadOnly bool

	// Timeout for the transaction execution.
	Timeout time.Duration
}

// DefaultTransactionOptions returns default transaction options.
func DefaultTransactionOptions() TransactionOptions {
	return TransactionOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
		Timeout:   60 * time.Second,
	}
}

// WithIsolation returns a copy with the given isolation level.
func (o TransactionOptions) WithIsolation(level sql.IsolationLevel) TransactionOptions {
	o.Isolation = level
	return o
}

// WithReadOnly returns a copy with the given read-only setting.
func (o TransactionOptions) WithReadOnly(readOnly bool) TransactionOptions {
	o.ReadOnly = readOnly
	return o
}

// WithTimeout returns a copy with the given timeout.
func (o TransactionOptions) WithTimeout(timeout time.Duration) TransactionOptions {
	o.Timeout = timeout
	return o
}

// ToTxOptions converts TransactionOptions to sql.TxOptions.
func (o TransactionOptions) ToTxOptions() *sql.TxOptions {
	return &sql.TxOptions{
		Isolation: o.Isolation,
		ReadOnly:  o.ReadOnly,
	}
}

// QueryResult represents the result of a query execution.
type QueryResult struct {
	// Result is the sql.Result for write operations.
	Result sql.Result

	// Rows is the sql.Rows for read operations.
	Rows *sql.Rows

	// Row is the sql.Row for single row read operations.
	Row *sql.Row

	// Error contains any error that occurred.
	Error error

	// Duration of the query execution.
	Duration time.Duration

	// RowsAffected is the number of rows affected (for write operations).
	RowsAffected int64

	// LastInsertID is the last insert ID (for insert operations).
	LastInsertID int64
}

// NewQueryResult creates a new QueryResult.
func NewQueryResult() *QueryResult {
	return &QueryResult{}
}

// WithResult sets the result and returns the QueryResult for chaining.
func (qr *QueryResult) WithResult(result sql.Result) *QueryResult {
	qr.Result = result
	return qr
}

// WithRows sets the rows and returns the QueryResult for chaining.
func (qr *QueryResult) WithRows(rows *sql.Rows) *QueryResult {
	qr.Rows = rows
	return qr
}

// WithRow sets the row and returns the QueryResult for chaining.
func (qr *QueryResult) WithRow(row *sql.Row) *QueryResult {
	qr.Row = row
	return qr
}

// WithError sets the error and returns the QueryResult for chaining.
func (qr *QueryResult) WithError(err error) *QueryResult {
	qr.Error = err
	return qr
}

// WithDuration sets the duration and returns the QueryResult for chaining.
func (qr *QueryResult) WithDuration(duration time.Duration) *QueryResult {
	qr.Duration = duration
	return qr
}

// Success returns true if there was no error.
func (qr *QueryResult) Success() bool {
	return qr.Error == nil
}

// Failed returns true if there was an error.
func (qr *QueryResult) Failed() bool {
	return qr.Error != nil
}

// GetRowsAffected returns the number of rows affected.
func (qr *QueryResult) GetRowsAffected() (int64, error) {
	if qr.Result == nil {
		return 0, ErrInvalidOperation
	}
	return qr.Result.RowsAffected()
}

// GetLastInsertID returns the last insert ID.
func (qr *QueryResult) GetLastInsertID() (int64, error) {
	if qr.Result == nil {
		return 0, ErrInvalidOperation
	}
	return qr.Result.LastInsertId()
}

// ScanRow scans the row into the provided destinations.
func (qr *QueryResult) ScanRow(dest ...interface{}) error {
	if qr.Row == nil {
		return ErrInvalidOperation
	}
	return ScanRow(qr.Row, dest...)
}

// ScanRows scans all rows using the provided function.
func (qr *QueryResult) ScanRows(fn func(*sql.Rows) error) error {
	if qr.Rows == nil {
		return ErrInvalidOperation
	}
	return ScanRows(qr.Rows, fn)
}

// CloseRows closes the rows if they exist.
func (qr *QueryResult) CloseRows() error {
	if qr.Rows == nil {
		return nil
	}
	return qr.Rows.Close()
}

// Bind binds query results to a struct or slice of structs.
type Binder struct {
	// TagName specifies the struct tag name to use (default: "db").
	TagName string

	// UseFieldNames specifies whether to use field names when tag is not present.
	UseFieldNames bool
}

// NewBinder creates a new Binder with default settings.
func NewBinder() *Binder {
	return &Binder{
		TagName:       "db",
		UseFieldNames: true,
	}
}

// WithTagName sets the tag name and returns the Binder for chaining.
func (b *Binder) WithTagName(tagName string) *Binder {
	b.TagName = tagName
	return b
}

// WithUseFieldNames sets whether to use field names and returns the Binder for chaining.
func (b *Binder) WithUseFieldNames(use bool) *Binder {
	b.UseFieldNames = use
	return b
}

// BindRow binds a single row to a struct.
func (b *Binder) BindRow(rows RowScanner, dest interface{}) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer")
	}

	elem := destVal.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("dest must point to a struct")
	}

	columns, err := b.getColumns(elem)
	if err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	return b.setValues(elem, columns, values)
}

// BindRows binds multiple rows to a slice of structs.
func (b *Binder) BindRows(rows RowsScanner, dest interface{}) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer")
	}

	sliceVal := destVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("dest must point to a slice")
	}

	// Get element type
	elemType := sliceVal.Type().Elem()
	if elemType.Kind() != reflect.Struct && elemType.Kind() != reflect.Ptr {
		return fmt.Errorf("slice elements must be structs or pointers to structs")
	}

	// Create a sample element to get columns
	var sampleElem reflect.Value
	if elemType.Kind() == reflect.Ptr {
		sampleElem = reflect.New(elemType.Elem())
	} else {
		sampleElem = reflect.New(elemType)
	}

	columns, err := b.getColumns(sampleElem.Elem())
	if err != nil {
		return err
	}

	// Process rows
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			return err
		}

		// Create new element
		var newElem reflect.Value
		if elemType.Kind() == reflect.Ptr {
			newElem = reflect.New(elemType.Elem())
			if err := b.setValues(newElem.Elem(), columns, values); err != nil {
				return err
			}
			sliceVal.Set(reflect.Append(sliceVal, newElem))
		} else {
			newElem = reflect.New(elemType).Elem()
			if err := b.setValues(newElem, columns, values); err != nil {
				return err
			}
			sliceVal.Set(reflect.Append(sliceVal, newElem))
		}
	}

	return rows.Err()
}

// getColumns extracts column information from a struct.
func (b *Binder) getColumns(structVal reflect.Value) ([]structField, error) {
	structType := structVal.Type()
	fields := make([]structField, 0, structType.NumField())

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get tag value
		tag := field.Tag.Get(b.TagName)
		if tag == "-" {
			continue
		}

		columnName := tag
		if columnName == "" && b.UseFieldNames {
			columnName = field.Name
		}

		if columnName != "" {
			fields = append(fields, structField{
				Index:      i,
				Name:       columnName,
				FieldType:  field.Type,
				FieldValue: structVal.Field(i),
			})
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no fields found with tag %q", b.TagName)
	}

	return fields, nil
}

// setValues sets values from the database into struct fields.
func (b *Binder) setValues(structVal reflect.Value, fields []structField, values []interface{}) error {
	for i, field := range fields {
		rawValue := *(values[i].(*interface{}))
		if rawValue == nil {
			continue
		}

		fieldVal := structVal.Field(field.Index)
		if !fieldVal.CanSet() {
			continue
		}

		// Convert the value to the field type
		converted, err := convertValue(rawValue, field.FieldType)
		if err != nil {
			return fmt.Errorf("field %s: %w", field.Name, err)
		}

		fieldVal.Set(converted)
	}

	return nil
}

// structField represents a struct field with its database mapping.
type structField struct {
	Index      int
	Name       string
	FieldType  reflect.Type
	FieldValue reflect.Value
}

// convertValue converts a value from the database to the target type.
func convertValue(src interface{}, targetType reflect.Type) (reflect.Value, error) {
	srcVal := reflect.ValueOf(src)
	srcType := srcVal.Type()

	// If types match, return directly
	if srcType.AssignableTo(targetType) {
		return srcVal, nil
	}

	// Handle nil values
	if src == nil {
		return reflect.Zero(targetType), nil
	}

	// Handle common conversions
	switch targetType.Kind() {
	case reflect.String:
		return reflect.ValueOf(fmt.Sprint(src)), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := src.(type) {
		case int64:
			return reflect.ValueOf(v).Convert(targetType), nil
		case int32:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case int:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case float64:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %T to %v", src, targetType)
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := src.(type) {
		case int64:
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case uint64:
			return reflect.ValueOf(v).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %T to %v", src, targetType)
		}

	case reflect.Float32, reflect.Float64:
		switch v := src.(type) {
		case float64:
			return reflect.ValueOf(v).Convert(targetType), nil
		case int64:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %T to %v", src, targetType)
		}

	case reflect.Bool:
		switch v := src.(type) {
		case bool:
			return reflect.ValueOf(v), nil
		case int64:
			return reflect.ValueOf(v != 0), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %T to bool", src)
		}

	case reflect.Struct:
		// Handle time.Time
		if targetType == reflect.TypeOf(time.Time{}) {
			switch v := src.(type) {
			case time.Time:
				return reflect.ValueOf(v), nil
			case []byte:
				t, err := time.Parse(time.RFC3339, string(v))
				if err != nil {
					return reflect.Value{}, fmt.Errorf("cannot parse time: %w", err)
				}
				return reflect.ValueOf(t), nil
			case string:
				t, err := time.Parse(time.RFC3339, v)
				if err != nil {
					return reflect.Value{}, fmt.Errorf("cannot parse time: %w", err)
				}
				return reflect.ValueOf(t), nil
			}
		}
	}

	return reflect.Value{}, fmt.Errorf("cannot convert %T to %v", src, targetType)
}

// ContextWithTimeout creates a context with timeout from options.
func ContextWithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}

// IsContextError checks if an error is a context error (deadline or cancellation).
func IsContextError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// ShouldRetry checks if an operation should be retried based on the error.
func ShouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Retry on context errors (except cancellation)
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Retry on connection errors
	errStr := strings.ToLower(err.Error())
	retryKeywords := []string{
		"connection",
		"timeout",
		"deadline",
		"busy",
		"locked",
		"temporary",
		"retry",
	}

	for _, keyword := range retryKeywords {
		if strings.Contains(errStr, keyword) {
			return true
		}
	}

	return false
}

// BuildInsertQuery builds an INSERT query string.
func BuildInsertQuery(table string, data map[string]interface{}) (string, []interface{}) {
	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data))

	for column, value := range data {
		columns = append(columns, escapeIdentifier(column))
		placeholders = append(placeholders, "?")
		args = append(args, value)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		escapeIdentifier(table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	return query, args
}

// BuildInsertQueryWithDriver builds an INSERT query string with driver-specific escaping.
func BuildInsertQueryWithDriver(driver Driver, table string, data map[string]interface{}) (string, []interface{}, error) {
	escapedTable, err := EscapeTableName(driver, table)
	if err != nil {
		return "", nil, fmt.Errorf("escape table name %q: %w", table, err)
	}

	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data))

	for column, value := range data {
		escapedCol, err := EscapeColumnName(driver, column)
		if err != nil {
			return "", nil, fmt.Errorf("escape column %q: %w", column, err)
		}
		columns = append(columns, escapedCol)
		placeholders = append(placeholders, "?")
		args = append(args, value)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		escapedTable,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	return query, args, nil
}

// BuildUpdateQuery builds an UPDATE query string.
func BuildUpdateQuery(table string, data, where map[string]interface{}) (string, []interface{}) {
	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data))

	for column, value := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", escapeIdentifier(column)))
		args = append(args, value)
	}

	whereClauses := make([]string, 0, len(where))
	for column, value := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", escapeIdentifier(column)))
		args = append(args, value)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		escapeIdentifier(table),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	return query, args
}

// BuildUpdateQueryWithDriver builds an UPDATE query string with driver-specific escaping.
func BuildUpdateQueryWithDriver(driver Driver, table string, data, where map[string]interface{}) (string, []interface{}, error) {
	escapedTable, err := EscapeTableName(driver, table)
	if err != nil {
		return "", nil, fmt.Errorf("escape table name %q: %w", table, err)
	}

	setClauses := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data))

	for column, value := range data {
		escapedCol, err := EscapeColumnName(driver, column)
		if err != nil {
			return "", nil, fmt.Errorf("escape column %q: %w", column, err)
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", escapedCol))
		args = append(args, value)
	}

	whereClauses := make([]string, 0, len(where))
	for column, value := range where {
		escapedCol, err := EscapeColumnName(driver, column)
		if err != nil {
			return "", nil, fmt.Errorf("escape column %q: %w", column, err)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", escapedCol))
		args = append(args, value)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		escapedTable,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)

	return query, args, nil
}

// BuildDeleteQuery builds a DELETE query string.
func BuildDeleteQuery(table string, where map[string]interface{}) (string, []interface{}) {
	whereClauses := make([]string, 0, len(where))
	args := make([]interface{}, 0, len(where))

	for column, value := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", escapeIdentifier(column)))
		args = append(args, value)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		escapeIdentifier(table),
		strings.Join(whereClauses, " AND "),
	)

	return query, args
}

// BuildDeleteQueryWithDriver builds a DELETE query string with driver-specific escaping.
func BuildDeleteQueryWithDriver(driver Driver, table string, where map[string]interface{}) (string, []interface{}, error) {
	escapedTable, err := EscapeTableName(driver, table)
	if err != nil {
		return "", nil, fmt.Errorf("escape table name %q: %w", table, err)
	}

	whereClauses := make([]string, 0, len(where))
	args := make([]interface{}, 0, len(where))

	for column, value := range where {
		escapedCol, err := EscapeColumnName(driver, column)
		if err != nil {
			return "", nil, fmt.Errorf("escape column %q: %w", column, err)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", escapedCol))
		args = append(args, value)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		escapedTable,
		strings.Join(whereClauses, " AND "),
	)

	return query, args, nil
}

// BuildSelectQuery builds a SELECT query string.
func BuildSelectQuery(table string, columns []string, where map[string]interface{}) (string, []interface{}) {
	columnList := "*"
	if len(columns) > 0 {
		escaped := make([]string, len(columns))
		for i, col := range columns {
			escaped[i] = escapeIdentifier(col)
		}
		columnList = strings.Join(escaped, ", ")
	}

	whereClauses := make([]string, 0, len(where))
	args := make([]interface{}, 0, len(where))

	for column, value := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", escapeIdentifier(column)))
		args = append(args, value)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, escapeIdentifier(table))
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	return query, args
}

// BuildSelectQueryWithDriver builds a SELECT query string with driver-specific escaping.
func BuildSelectQueryWithDriver(driver Driver, table string, columns []string, where map[string]interface{}) (string, []interface{}, error) {
	escapedTable, err := EscapeTableName(driver, table)
	if err != nil {
		return "", nil, fmt.Errorf("escape table name %q: %w", table, err)
	}

	columnList := "*"
	if len(columns) > 0 {
		escaped := make([]string, len(columns))
		for i, col := range columns {
			escapedCol, err := EscapeColumnName(driver, col)
			if err != nil {
				return "", nil, fmt.Errorf("escape column %q: %w", col, err)
			}
			escaped[i] = escapedCol
		}
		columnList = strings.Join(escaped, ", ")
	}

	whereClauses := make([]string, 0, len(where))
	args := make([]interface{}, 0, len(where))

	for column, value := range where {
		escapedCol, err := EscapeColumnName(driver, column)
		if err != nil {
			return "", nil, fmt.Errorf("escape column %q: %w", column, err)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", escapedCol))
		args = append(args, value)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, escapedTable)
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	return query, args, nil
}

// Paginate adds pagination to a query.
func Paginate(query string, page, pageSize int) string {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	offset := (page - 1) * pageSize
	return fmt.Sprintf("%s LIMIT %d OFFSET %d", query, pageSize, offset)
}

// PaginateWithDriver adds pagination to a query with driver-specific syntax.
func PaginateWithDriver(driver Driver, query string, page, pageSize int) (string, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}

	offset := (page - 1) * pageSize

	switch driver {
	case MySQL:
		return fmt.Sprintf("%s LIMIT %d OFFSET %d", query, pageSize, offset), nil
	case PostgreSQL:
		return fmt.Sprintf("%s LIMIT %d OFFSET %d", query, pageSize, offset), nil
	case SQLite:
		return fmt.Sprintf("%s LIMIT %d OFFSET %d", query, pageSize, offset), nil
	default:
		return "", fmt.Errorf("%w: %s", ErrDriverNotSupported, driver)
	}
}

// OrderBy adds ORDER BY clause to a query.
func OrderBy(query string, orderBy string, desc ...bool) string {
	direction := "ASC"
	if len(desc) > 0 && desc[0] {
		direction = "DESC"
	}
	return fmt.Sprintf("%s ORDER BY %s %s", query, escapeIdentifier(orderBy), direction)
}

// GroupBy adds GROUP BY clause to a query.
func GroupBy(query string, groupBy ...string) string {
	if len(groupBy) == 0 {
		return query
	}
	escaped := make([]string, len(groupBy))
	for i, col := range groupBy {
		escaped[i] = escapeIdentifier(col)
	}
	return fmt.Sprintf("%s GROUP BY %s", query, strings.Join(escaped, ", "))
}

// Having adds HAVING clause to a query.
func Having(query string, having string) string {
	return fmt.Sprintf("%s HAVING %s", query, having)
}

// Distinct adds DISTINCT keyword to a query.
func Distinct(query string) string {
	// Simple implementation - assumes query starts with SELECT
	return strings.Replace(query, "SELECT", "SELECT DISTINCT", 1)
}

// CountQuery converts a SELECT query to a COUNT query.
func CountQuery(query string) string {
	// Find the SELECT ... FROM pattern
	fromIndex := strings.Index(strings.ToUpper(query), "FROM")
	if fromIndex == -1 {
		return query
	}

	// Keep everything from FROM onward
	fromPart := query[fromIndex:]

	// Remove ORDER BY, LIMIT, OFFSET for count query
	fromPart = strings.Split(fromPart, "ORDER BY")[0]
	fromPart = strings.Split(fromPart, "LIMIT")[0]
	fromPart = strings.Split(fromPart, "OFFSET")[0]

	return fmt.Sprintf("SELECT COUNT(*) %s", fromPart)
}

// Placeholders generates a string of SQL placeholders.
func Placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ", ")
}

// InClause builds an IN clause with placeholders.
func InClause(column string, count int) string {
	if count <= 0 {
		return fmt.Sprintf("%s IN ()", escapeIdentifier(column))
	}
	return fmt.Sprintf("%s IN (%s)", escapeIdentifier(column), Placeholders(count))
}

// BetweenClause builds a BETWEEN clause.
func BetweenClause(column string) string {
	return fmt.Sprintf("%s BETWEEN ? AND ?", escapeIdentifier(column))
}

// LikeClause builds a LIKE clause.
func LikeClause(column string) string {
	return fmt.Sprintf("%s LIKE ?", escapeIdentifier(column))
}

// NullSafeEqual builds a null-safe equality comparison.
func NullSafeEqual(column string) string {
	return fmt.Sprintf("%s <=> ?", escapeIdentifier(column))
}

// IsNull builds an IS NULL check.
func IsNull(column string) string {
	return fmt.Sprintf("%s IS NULL", escapeIdentifier(column))
}

// IsNotNull builds an IS NOT NULL check.
func IsNotNull(column string) string {
	return fmt.Sprintf("%s IS NOT NULL", escapeIdentifier(column))
}

// Alias adds an alias to a column or expression.
func Alias(expression, alias string) string {
	return fmt.Sprintf("%s AS %s", expression, escapeIdentifier(alias))
}

// Coalesce builds a COALESCE expression.
func Coalesce(columns ...string) string {
	if len(columns) == 0 {
		return "COALESCE()"
	}
	escaped := make([]string, len(columns))
	for i, col := range columns {
		escaped[i] = escapeIdentifier(col)
	}
	return fmt.Sprintf("COALESCE(%s)", strings.Join(escaped, ", "))
}

// Concat builds a CONCAT expression.
func Concat(columns ...string) string {
	if len(columns) == 0 {
		return "CONCAT()"
	}
	escaped := make([]string, len(columns))
	for i, col := range columns {
		escaped[i] = escapeIdentifier(col)
	}
	return fmt.Sprintf("CONCAT(%s)", strings.Join(escaped, ", "))
}

// CaseBuilder helps build CASE expressions.
type CaseBuilder struct {
	caseExpr string
	when     []string
	elseExpr string
}

// NewCaseBuilder creates a new CaseBuilder.
func NewCaseBuilder() *CaseBuilder {
	return &CaseBuilder{
		when: make([]string, 0),
	}
}

// Case starts a CASE expression.
func (cb *CaseBuilder) Case(expression string) *CaseBuilder {
	cb.caseExpr = expression
	return cb
}

// When adds a WHEN clause.
func (cb *CaseBuilder) When(condition, result string) *CaseBuilder {
	cb.when = append(cb.when, fmt.Sprintf("WHEN %s THEN %s", condition, result))
	return cb
}

// Else adds an ELSE clause.
func (cb *CaseBuilder) Else(expression string) *CaseBuilder {
	cb.elseExpr = expression
	return cb
}

// Build builds the CASE expression.
func (cb *CaseBuilder) Build() string {
	var builder strings.Builder

	builder.WriteString("CASE")
	if cb.caseExpr != "" {
		builder.WriteString(" ")
		builder.WriteString(cb.caseExpr)
	}

	for _, when := range cb.when {
		builder.WriteString(" ")
		builder.WriteString(when)
	}

	if cb.elseExpr != "" {
		builder.WriteString(" ELSE ")
		builder.WriteString(cb.elseExpr)
	}

	builder.WriteString(" END")
	return builder.String()
}

// QueryLogger defines the interface for logging queries.
type QueryLogger interface {
	// LogQuery logs a query execution.
	LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error)
}

// DefaultQueryLogger is a simple query logger that does nothing.
type DefaultQueryLogger struct{}

// LogQuery implements QueryLogger interface.
func (l *DefaultQueryLogger) LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error) {
	// Default implementation does nothing
}

// NullLogger returns a QueryLogger that does nothing.
func NullLogger() QueryLogger {
	return &DefaultQueryLogger{}
}
