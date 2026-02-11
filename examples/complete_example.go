package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/dongrv/sqlx"
)

func main() {
	fmt.Println("📚 SQLx 完整示例程序")
	fmt.Println("====================\n")

	fmt.Println("1. 基础示例演示:")
	fmt.Println("----------------")
	basicExample()

	fmt.Println("\n2. 优雅API示例演示:")
	fmt.Println("-------------------")
	elegantExample()

	fmt.Println("\n3. 安全功能演示:")
	fmt.Println("---------------")
	demoSQLSecurity()
}

// ============================================================================
// 基础示例
// ============================================================================

func basicExample() {
	// Configure database connections
	configs := sqlx.ConfigMap{
		"game": sqlx.DefaultConfig().
			WithDSN("root:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4&parseTime=True&loc=Local").
			WithMaxOpenConns(100).
			WithMaxIdleConns(20).
			WithConnMaxLifetime(100 * time.Second).
			WithConnMaxIdleTime(3600 * time.Second),
		"log": sqlx.DefaultConfig().
			WithDSN("root:123456@tcp(127.0.0.1:3306)/unity?charset=utf8mb4&parseTime=True&loc=Local").
			WithMaxOpenConns(100).
			WithMaxIdleConns(20).
			WithConnMaxLifetime(100 * time.Second).
			WithConnMaxIdleTime(3600 * time.Second),
	}

	// Initialize the connection pool
	err := sqlx.Init(configs)
	if err != nil {
		log.Printf("⚠️  数据库初始化失败（预期中，因为无真实数据库）: %v", err)
		fmt.Println("（继续演示 API 使用）\n")
		return
	}
	defer sqlx.Close()

	// Ping all connections
	ctx := context.Background()
	if err := sqlx.Ping(ctx); err != nil {
		log.Printf("Warning: some connections failed ping: %v", err)
	}

	// Create table if not exists
	_, err = sqlx.Exec(ctx, "game", `
		CREATE TABLE IF NOT EXISTS profile (
			id INT AUTO_INCREMENT PRIMARY KEY,
			first_name VARCHAR(100),
			last_name VARCHAR(100),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Printf("Failed to create table: %v", err)
	}

	// Run examples
	fmt.Println("✅ 数据库初始化完成")
	fmt.Println("✅ 开始演示 CRUD 操作...")

	// 演示各种操作
	demoBasicCRUD()
	demoTransactions()
	demoConnectionStats()
}

// BasicProfile represents a user profile for basic example
type BasicProfile struct {
	ID        int64
	FirstName string
	LastName  string
	CreatedAt time.Time
}

func demoBasicCRUD() {
	fmt.Println("\n📝 基础 CRUD 操作演示:")

	ctx := context.Background()

	// 1. 创建数据
	fmt.Println("1. 创建数据:")
	data := sqlx.Data(
		"first_name", "John",
		"last_name", "Doe",
	)
	result, err := sqlx.Insert(ctx, "game", "profile", data)
	if err != nil {
		log.Printf("❌ 插入失败: %v", err)
	} else {
		id, _ := result.LastInsertId()
		fmt.Printf("✅ 插入成功，ID: %d\n", id)
	}

	// 2. 查询单条数据
	fmt.Println("\n2. 查询单条数据:")
	row := sqlx.SelectOne(ctx, "game", "profile",
		[]string{"id", "first_name", "last_name", "created_at"},
		sqlx.Where("first_name", "John"),
	)

	var profile BasicProfile
	if err := sqlx.ScanRow(row, &profile.ID, &profile.FirstName, &profile.LastName, &profile.CreatedAt); err != nil {
		if sqlx.Is(err, sqlx.ErrNoRows) {
			fmt.Println("✅ 未找到数据（预期中）")
		} else {
			log.Printf("❌ 查询失败: %v", err)
		}
	} else {
		fmt.Printf("✅ 查询成功: ID=%d, Name=%s %s\n", profile.ID, profile.FirstName, profile.LastName)
	}

	// 3. 更新数据
	fmt.Println("\n3. 更新数据:")
	updateData := sqlx.Data("last_name", "Doe-Smith")
	where := sqlx.Where("first_name", "John")
	result, err = sqlx.Update(ctx, "game", "profile", updateData, where)
	if err != nil {
		log.Printf("❌ 更新失败: %v", err)
	} else {
		rows, _ := result.RowsAffected()
		fmt.Printf("✅ 更新成功，影响行数: %d\n", rows)
	}

	// 4. 删除数据
	fmt.Println("\n4. 删除数据:")
	where = sqlx.Where("id", ">", 100) // 清理旧数据
	result, err = sqlx.Delete(ctx, "game", "profile", where)
	if err != nil {
		log.Printf("❌ 删除失败: %v", err)
	} else {
		rows, _ := result.RowsAffected()
		fmt.Printf("✅ 删除成功，影响行数: %d\n", rows)
	}
}

func demoTransactions() {
	fmt.Println("\n💳 事务操作演示:")

	ctx := context.Background()

	err := sqlx.Transaction(ctx, "game", func(tx *sql.Tx) error {
		// 在事务中插入数据
		result, err := tx.ExecContext(ctx,
			"INSERT INTO profile (first_name, last_name) VALUES (?, ?)",
			"Transaction", "Test",
		)
		if err != nil {
			return fmt.Errorf("insert in transaction: %w", err)
		}

		id, _ := result.LastInsertId()
		fmt.Printf("✅ 事务中插入成功，ID: %d\n", id)

		// 在事务中更新数据
		_, err = tx.ExecContext(ctx,
			"UPDATE profile SET last_name = ? WHERE id = ?",
			"Updated", id,
		)
		if err != nil {
			return fmt.Errorf("update in transaction: %w", err)
		}

		fmt.Println("✅ 事务中更新成功")
		return nil
	}, nil)

	if err != nil {
		log.Printf("❌ 事务执行失败: %v", err)
	} else {
		fmt.Println("✅ 事务提交成功")
	}
}

func demoConnectionStats() {
	fmt.Println("\n📊 连接统计信息演示:")

	stats := sqlx.Stats()
	if len(stats) == 0 {
		fmt.Println("✅ 无活跃连接（预期中）")
		return
	}

	for name, stat := range stats {
		fmt.Printf("连接 %s:\n", name)
		fmt.Printf("  打开连接数: %d\n", stat.OpenConnections)
		fmt.Printf("  使用中连接: %d\n", stat.InUse)
		fmt.Printf("  空闲连接数: %d\n", stat.Idle)
		fmt.Printf("  等待连接数: %d\n", stat.WaitCount)
		fmt.Printf("  等待时间: %v\n", stat.WaitDuration)
	}
}

// ============================================================================
// 优雅 API 示例
// ============================================================================

// User represents a user in the database
type User struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	Age       int       `db:"age"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// UserProfile represents a user profile
type UserProfile struct {
	UserID    int64  `db:"user_id"`
	Bio       string `db:"bio"`
	AvatarURL string `db:"avatar_url"`
}

func elegantExample() {
	fmt.Println("🚀 优雅 API 功能演示")
	fmt.Println("==================\n")

	// 演示流畅配置
	demoFluentConfiguration()

	// 演示高级查询
	demoAdvancedQueries()

	// 演示事务选项
	demoTransactionOptions()

	// 演示错误处理
	demoErrorHandling()
}

func demoFluentConfiguration() {
	fmt.Println("1. 流畅配置接口:")

	configs := sqlx.ConfigMap{
		"primary": sqlx.DefaultConfig().
			WithDriver(sqlx.MySQL).
			WithDSN("root:password@tcp(localhost:3306)/appdb?charset=utf8mb4&parseTime=true&loc=Local").
			WithMaxOpenConns(25).
			WithMaxIdleConns(10).
			WithQueryTimeout(10*time.Second).
			WithTransactionTimeout(30*time.Second).
			WithRetries(3, 100*time.Millisecond),

		"replica": sqlx.DefaultConfig().
			WithDriver(sqlx.MySQL).
			WithDSN("readonly:password@tcp(replica.localhost:3306)/appdb").
			WithMaxOpenConns(50).
			WithMaxIdleConns(20).
			WithQueryTimeout(5 * time.Second),
	}

	fmt.Printf("✅ 主数据库配置:\n")
	fmt.Printf("  驱动: %v\n", configs["primary"].Driver)
	fmt.Printf("  最大打开连接: %d\n", configs["primary"].MaxOpenConns)
	fmt.Printf("  查询超时: %v\n", configs["primary"].QueryTimeout)
	fmt.Printf("  重试次数: %d\n", configs["primary"].MaxRetries)

	fmt.Printf("\n✅ 从数据库配置:\n")
	fmt.Printf("  最大打开连接: %d\n", configs["replica"].MaxOpenConns)
	fmt.Printf("  查询超时: %v\n", configs["replica"].QueryTimeout)
}

func demoAdvancedQueries() {
	fmt.Println("\n2. 高级查询构建:")

	// 构建 INSERT 查询
	insertData := map[string]interface{}{
		"name":  "John",
		"email": "john@example.com",
		"age":   30,
	}
	insertQuery, insertArgs := sqlx.BuildInsertQuery("users", insertData)
	fmt.Printf("✅ INSERT 查询:\n")
	fmt.Printf("  SQL: %s\n", insertQuery)
	fmt.Printf("  参数: %v\n", insertArgs)

	// 构建 UPDATE 查询
	updateData := map[string]interface{}{"age": 31}
	updateWhere := map[string]interface{}{"id": 1}
	updateQuery, updateArgs := sqlx.BuildUpdateQuery("users", updateData, updateWhere)
	fmt.Printf("\n✅ UPDATE 查询:\n")
	fmt.Printf("  SQL: %s\n", updateQuery)
	fmt.Printf("  参数: %v\n", updateArgs)

	// 构建 SELECT 查询
	selectColumns := []string{"id", "name", "email"}
	selectWhere := map[string]interface{}{"active": true}
	selectQuery, selectArgs := sqlx.BuildSelectQuery("users", selectColumns, selectWhere)
	fmt.Printf("\n✅ SELECT 查询:\n")
	fmt.Printf("  SQL: %s\n", selectQuery)
	fmt.Printf("  参数: %v\n", selectArgs)

	// 分页查询
	paginatedQuery := sqlx.Paginate(selectQuery, 2, 10)
	fmt.Printf("\n✅ 分页查询:\n")
	fmt.Printf("  SQL: %s\n", paginatedQuery)
}

func demoTransactionOptions() {
	fmt.Println("\n3. 事务选项配置:")

	txOpts := sqlx.TransactionOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
		Timeout:   30 * time.Second,
	}

	fmt.Printf("✅ 事务选项:\n")
	fmt.Printf("  隔离级别: %v\n", txOpts.Isolation)
	fmt.Printf("  只读: %v\n", txOpts.ReadOnly)
	fmt.Printf("  超时: %v\n", txOpts.Timeout)

	// 转换为标准库选项
	sqlOpts := txOpts.ToTxOptions()
	fmt.Printf("\n✅ 标准库事务选项:\n")
	fmt.Printf("  隔离级别: %v\n", sqlOpts.Isolation)
	fmt.Printf("  只读: %v\n", sqlOpts.ReadOnly)
}

func demoErrorHandling() {
	fmt.Println("\n4. 错误处理演示:")

	// 模拟各种错误
	errorsToTest := []struct {
		name string
		err  error
	}{
		{"重复条目错误", sqlx.ErrDuplicateEntry},
		{"无结果错误", sqlx.ErrNoRows},
		{"超时错误", sqlx.ErrTimeout},
		{"连接关闭错误", sqlx.ErrConnectionClosed},
	}

	for _, test := range errorsToTest {
		fmt.Printf("\n测试错误: %s\n", test.name)

		if sqlx.IsDuplicateError(test.err) {
			fmt.Println("  ✅ 检测到重复条目错误")
		}

		if sqlx.Is(test.err, sqlx.ErrNoRows) {
			fmt.Println("  ✅ 检测到无结果错误")
		}

		if sqlx.Is(test.err, sqlx.ErrTimeout) {
			fmt.Println("  ✅ 检测到超时错误")
		}

		// 检查是否应该重试
		if sqlx.ShouldRetry(test.err) {
			fmt.Println("  ⚠️  建议重试此操作")
		}
	}
}

// ============================================================================
// 安全功能演示
// ============================================================================

func demoSQLSecurity() {
	fmt.Println("🔒 SQLx 安全功能演示")
	fmt.Println("===================\n")

	// 1. 演示标识符验证
	demoIdentifierValidation()

	// 2. 演示安全标识符使用
	demoSafeIdentifiers()

	// 3. 演示驱动特定的转义
	demoDriverSpecificEscaping()

	// 4. 演示 SQL 注入防护
	demoSQLInjectionProtection()

	// 5. 演示生产环境使用
	demoProductionUsage()
}

// demoIdentifierValidation 演示标识符验证
func demoIdentifierValidation() {
	fmt.Println("1. 标识符验证演示")
	fmt.Println("----------------")

	driver := sqlx.MySQL

	// 有效标识符
	validIdentifiers := []string{"users", "user_table", "order123", "created_at"}
	for _, id := range validIdentifiers {
		if err := sqlx.ValidateIdentifier(driver, id); err != nil {
			log.Printf("❌ 意外错误: %v", err)
		} else {
			fmt.Printf("✅ 有效标识符: %q\n", id)
		}
	}

	// 无效标识符
	invalidIdentifiers := []struct {
		id  string
		msg string
	}{
		{"", "空标识符"},
		{"123users", "以数字开头"},
		{"user-table", "包含连字符"},
		{"user table", "包含空格"},
		{"user.table", "包含点号"},
		{"SELECT", "SQL 关键字"},
		{"FROM", "SQL 关键字"},
		{"a_very_long_identifier_that_exceeds_the_maximum_length_of_sixty_four_characters", "超过长度限制"},
	}

	for _, test := range invalidIdentifiers {
		if err := sqlx.ValidateIdentifier(driver, test.id); err != nil {
			fmt.Printf("✅ 正确拒绝无效标识符 %q (%s): %v\n", test.id, test.msg, err)
		} else {
			log.Printf("❌ 应该拒绝但接受了标识符: %q (%s)", test.id, test.msg)
		}
	}

	fmt.Println()
}

// demoSafeIdentifiers 演示安全标识符使用
func demoSafeIdentifiers() {
	fmt.Println("2. 安全标识符使用演示")
	fmt.Println("-------------------")

	driver := sqlx.MySQL

	// 创建安全标识符
	tableName := "users"
	si, err := sqlx.NewSafeIdentifier(driver, tableName)
	if err != nil {
		log.Printf("❌ 创建安全标识符失败: %v", err)
		return
	}

	fmt.Printf("✅ 原始标识符: %q\n", si.Original())
	fmt.Printf("✅ 转义后标识符: %q\n", si.String())
	fmt.Printf("✅ 数据库驱动: %v\n", si.Driver())
	fmt.Printf("✅ 引号字符: %q\n", si.QuoteChar())
	fmt.Printf("✅ 是否有效: %v\n", si.IsValid())

	// 使用 Must 函数（确保标识符有效时使用）
	safeColumn := sqlx.MustEscapeColumnName(driver, "user_id")
	fmt.Printf("✅ 安全列名: %q\n", safeColumn)

	// 安全标识符列表
	columns := []string{"id", "name", "email", "created_at"}
	sil, err := sqlx.NewSafeIdentifierList(driver, columns...)
	if err != nil {
		log.Printf("❌ 创建安全标识符列表失败: %v", err)
		return
	}

	fmt.Printf("✅ 安全列列表: %s\n", sil.Join(", "))

	fmt.Println()
}

// demoDriverSpecificEscaping 演示驱动特定的转义
func demoDriverSpecificEscaping() {
	fmt.Println("3. 驱动特定的转义演示")
	fmt.Println("-------------------")

	identifiers := []string{"users", "order_items", "created_at"}

	drivers := []sqlx.Driver{
		sqlx.MySQL,
		sqlx.PostgreSQL,
		sqlx.SQLite,
	}

	for _, driver := range drivers {
		fmt.Printf("\n驱动: %v\n", driver)
		fmt.Printf("引号字符: %q\n", sqlx.GetIdentifierEscaper(driver).QuoteChar())

		for _, id := range identifiers {
			escaped, err := sqlx.EscapeIdentifier(driver, id)
			if err != nil {
				log.Printf("❌ 转义失败: %v", err)
				continue
			}
			fmt.Printf("  %q → %q\n", id, escaped)
		}
	}

	fmt.Println()
}

// demoSQLInjectionProtection 演示 SQL 注入防护
func demoSQLInjectionProtection() {
	fmt.Println("4. SQL 注入防护演示")
	fmt.Println("------------------")

	driver := sqlx.MySQL

	// 模拟恶意输入
	maliciousInputs := []struct {
		name     string
		input    string
		expected string
	}{
		{"简单注入", "users; DROP TABLE users", "会被拒绝"},
		{"注释注入", "users -- comment", "会被拒绝"},
		{"联合查询", "users UNION SELECT * FROM passwords", "会被拒绝"},
		{"引号注入", "user`table", "会被拒绝"},
	}

	for _, test := range maliciousInputs {
		fmt.Printf("\n测试: %s\n", test.name)
		fmt.Printf("输入: %q\n", test.input)

		// 尝试创建安全标识符
		_, err := sqlx.NewSafeIdentifier(driver, test.input)
		if err != nil {
			fmt.Printf("✅ 成功阻止: %v\n", err)
		} else {
			log.Printf("❌ 危险！应该阻止但接受了输入: %q", test.input)
		}
	}

	// 演示安全查询构建
	fmt.Printf("\n✅ 安全查询构建示例:\n")

	table := "users"
	data := map[string]interface{}{
		"name":  "John Doe",
		"email": "john@example.com",
		"age":   30,
	}

	query, args, err := sqlx.BuildInsertQueryWithDriver(driver, table, data)
	if err != nil {
		log.Printf("❌ 构建查询失败: %v", err)
	} else {
		fmt.Printf("安全 INSERT 查询: %s\n", query)
		fmt.Printf("参数: %v\n", args)
	}

	fmt.Println()
}

// demoProductionUsage 演示生产环境使用
func demoProductionUsage() {
	fmt.Println("5. 生产环境使用演示")
	fmt.Println("-----------------")

	// 初始化数据库配置
	configs := sqlx.ConfigMap{
		"primary": sqlx.DefaultConfig().
			WithDriver(sqlx.MySQL).
			WithDSN("user:pass@tcp(localhost:3306)/appdb?charset=utf8mb4&parseTime=true&loc=Local").
			WithMaxOpenConns(25).
			WithMaxIdleConns(10),
	}

	// 初始化数据库连接
	err := sqlx.Init(configs)
	if err != nil {
		log.Printf("⚠️  数据库初始化失败（预期中，因为无真实数据库）: %v", err)
		fmt.Println("（继续演示 API 使用）\n")
	}

	// 演示安全的 CRUD 操作
	fmt.Println("安全 CRUD 操作示例:")

	// 1. 安全插入
	insertData := sqlx.Data(
		"username", "johndoe",
		"email", "john@example.com",
		"full_name", "John Doe",
		"age", 30,
		"is_active", true,
	)

	fmt.Printf("1. 插入数据: %v\n", insertData)

	// 2. 安全查询
	where := sqlx.Where("is_active", true)
	fmt.Printf("2. 查询条件: %v\n", where)

	// 3. 安全更新
	updateData := sqlx.Data("age", 31)
	updateWhere := sqlx.Where("username", "johndoe")
	fmt.Printf("3. 更新数据: %v (条件: %v)\n", updateData, updateWhere)

	// 4. 使用安全标识符列表
	columns := []string{"id", "username", "email", "age", "created_at"}
	sil, err := sqlx.NewSafeIdentifierList(sqlx.MySQL, columns...)
	if err != nil {
		log.Printf("❌ 创建列列表失败: %v", err)
	} else {
		fmt.Printf("4. 安全列列表: %s\n", sil.Join(", "))
	}

	// 5. 验证生产环境标识符
	productionTables := []string{
		"users",
		"orders",
		"order_items",
		"products",
		"categories",
		"payments",
	}

	fmt.Println("\n生产环境表名验证:")
	for _, table := range productionTables {
		if err := sqlx.ValidateTableName(sqlx.MySQL, table); err != nil {
			log.Printf("❌ 表名 %q 无效: %v", table, err)
		} else {
			fmt.Printf("✅ 表名 %q 有效\n", table)
		}
	}

	fmt.Println("\n🎯 安全功能总结:")
	fmt.Println("1. ✅ 所有标识符都经过严格验证")
	fmt.Println("2. ✅ 防止 SQL 注入攻击")
	fmt.Println("3. ✅ 支持多数据库驱动")
	fmt.Println("4. ✅ 提供类型安全的 API")
	fmt.Println("5. ✅ 生产环境就绪")
}
