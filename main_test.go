package dbratelimit

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/time/rate"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// setupTestDB 创建一个测试用的 SQLite 数据库
func setupTestDB(t *testing.T) *sql.DB {
	// 使用唯一的内存数据库名称，避免测试间干扰
	dbName := "file:" + t.Name() + "?mode=memory&cache=shared"
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// 设置连接池参数以支持并发
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	// 创建测试表
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 插入测试数据
	_, err = db.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "Alice", "alice@example.com")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	return db
}

// TestWrap 测试 Wrap 函数
func TestWrap(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	if rateLimitedDB == nil {
		t.Fatal("Wrap returned nil")
	}

	if rateLimitedDB.db != db {
		t.Error("Wrapped DB does not match original DB")
	}

	if rateLimitedDB.limiter == nil {
		t.Error("Limiter is nil")
	}
}

// TestQueryContext 测试 QueryContext 方法
func TestQueryContext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	ctx := context.Background()
	rows, err := rateLimitedDB.QueryContext(ctx, "SELECT id, name, email FROM users")
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
		t.Logf("Row: id=%d, name=%s, email=%s", id, name, email)
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}
}

// TestQueryRowContext 测试 QueryRowContext 方法
func TestQueryRowContext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	ctx := context.Background()
	row := rateLimitedDB.QueryRowContext(ctx, "SELECT name, email FROM users WHERE id = ?", 1)

	var name, email string
	if err := row.Scan(&name, &email); err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}

	if name != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", name)
	}
	if email != "alice@example.com" {
		t.Errorf("Expected email 'alice@example.com', got '%s'", email)
	}
}

// TestExecContext 测试 ExecContext 方法
func TestExecContext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	ctx := context.Background()
	result, err := rateLimitedDB.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Bob", "bob@example.com")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}

	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	// 验证插入成功
	row := rateLimitedDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM users")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

// TestPrepareContext 测试 PrepareContext 方法
func TestPrepareContext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	ctx := context.Background()
	stmt, err := rateLimitedDB.PrepareContext(ctx, "SELECT name FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("PrepareContext failed: %v", err)
	}
	defer stmt.Close()

	var name string
	if err := stmt.QueryRowContext(ctx, 1).Scan(&name); err != nil {
		t.Fatalf("Failed to query with prepared statement: %v", err)
	}

	if name != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", name)
	}
}

// TestRateLimit 测试速率限制功能
func TestRateLimit(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 设置每秒 2 个请求的限制
	rateLimitedDB := Wrap(db, rate.Limit(2), 1)
	defer rateLimitedDB.Close()

	ctx := context.Background()
	start := time.Now()

	// 执行 5 次查询
	for i := 0; i < 5; i++ {
		rows, err := rateLimitedDB.QueryContext(ctx, "SELECT * FROM users")
		if err != nil {
			t.Fatalf("Query %d failed: %v", i, err)
		}
		rows.Close()
	}

	elapsed := time.Since(start)

	// 5 个请求，速率为 2/秒，应该至少需要 2 秒
	// (第1个立即执行，第2个立即执行(burst=1)，第3-5个需要等待)
	expectedMinDuration := 1500 * time.Millisecond
	if elapsed < expectedMinDuration {
		t.Errorf("Rate limiting not working properly. Expected at least %v, got %v", expectedMinDuration, elapsed)
	}

	t.Logf("5 queries with rate limit 2/s took %v", elapsed)
}

// TestContextCancellation 测试上下文取消
func TestContextCancellation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// 设置非常低的速率限制
	rateLimitedDB := Wrap(db, rate.Limit(0.1), 1)
	defer rateLimitedDB.Close()

	// 创建一个会很快取消的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// 第一个查询应该成功（使用 burst）
	_, err := rateLimitedDB.QueryContext(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("First query should succeed: %v", err)
	}

	// 第二个查询应该因为上下文超时而失败
	_, err = rateLimitedDB.QueryContext(ctx, "SELECT * FROM users")
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Logf("Got error (expected): %v", err)
	}
}

// TestPing 测试 Ping 方法
func TestPing(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	if err := rateLimitedDB.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

// TestConn 测试 Conn 方法
func TestConn(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	ctx := context.Background()
	conn, err := rateLimitedDB.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn failed: %v", err)
	}
	defer conn.Close()

	// 使用连接执行查询
	var count int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("Query on conn failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 user, got %d", count)
	}
}

// TestRaw 测试 Raw 方法
func TestRaw(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	rawDB := rateLimitedDB.Raw()
	if rawDB != db {
		t.Error("Raw() did not return the original database")
	}

	// 验证可以直接使用原始 DB（绕过速率限制）
	ctx := context.Background()
	var count int
	err := rawDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("Query on raw DB failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 user, got %d", count)
	}
}

// TestConcurrentAccess 测试并发访问
func TestConcurrentAccess(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	rateLimitedDB := Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	ctx := context.Background()
	concurrency := 10
	done := make(chan bool, concurrency)

	start := time.Now()

	// 启动多个并发查询
	for i := 0; i < concurrency; i++ {
		go func(id int) {
			rows, err := rateLimitedDB.QueryContext(ctx, "SELECT * FROM users")
			if err != nil {
				t.Errorf("Concurrent query %d failed: %v", id, err)
			} else {
				rows.Close()
			}
			done <- true
		}(i)
	}

	// 等待所有查询完成
	for i := 0; i < concurrency; i++ {
		<-done
	}

	elapsed := time.Since(start)
	t.Logf("%d concurrent queries took %v", concurrency, elapsed)

	// 验证速率限制在并发情况下也能工作
	// 10 个请求，速率 10/s，burst 5，应该需要至少 0.5 秒
	expectedMinDuration := 400 * time.Millisecond
	if elapsed < expectedMinDuration {
		t.Errorf("Concurrent rate limiting not working properly. Expected at least %v, got %v", expectedMinDuration, elapsed)
	}
}

// User 模型用于 GORM 测试
type User struct {
	ID    uint   `gorm:"primaryKey"`
	Name  string `gorm:"not null"`
	Email string `gorm:"not null"`
}

// TestGormIntegration 测试与 GORM 的集成
func TestGormIntegration(t *testing.T) {
	// 创建标准 SQL 数据库
	sqlDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	// 使用 RateLimitedDB 包装
	rateLimitedDB := Wrap(sqlDB, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	// 使用 GORM 连接，将 RateLimitedDB 作为 ConnPool
	gormDB, err := gorm.Open(sqlite.Dialector{
		Conn: rateLimitedDB,
	}, &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to initialize GORM: %v", err)
	}

	// 自动迁移
	if err := gormDB.AutoMigrate(&User{}); err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	// 创建记录
	user := User{Name: "Alice", Email: "alice@example.com"}
	result := gormDB.Create(&user)
	if result.Error != nil {
		t.Fatalf("Failed to create user: %v", result.Error)
	}
	if user.ID == 0 {
		t.Error("User ID should be set after creation")
	}
	t.Logf("Created user with ID: %d", user.ID)

	// 查询记录
	var foundUser User
	result = gormDB.First(&foundUser, user.ID)
	if result.Error != nil {
		t.Fatalf("Failed to find user: %v", result.Error)
	}
	if foundUser.Name != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", foundUser.Name)
	}
	if foundUser.Email != "alice@example.com" {
		t.Errorf("Expected email 'alice@example.com', got '%s'", foundUser.Email)
	}

	// 更新记录
	result = gormDB.Model(&foundUser).Update("Email", "alice.new@example.com")
	if result.Error != nil {
		t.Fatalf("Failed to update user: %v", result.Error)
	}

	// 验证更新
	var updatedUser User
	gormDB.First(&updatedUser, user.ID)
	if updatedUser.Email != "alice.new@example.com" {
		t.Errorf("Expected updated email 'alice.new@example.com', got '%s'", updatedUser.Email)
	}

	// 查询所有记录
	var users []User
	result = gormDB.Find(&users)
	if result.Error != nil {
		t.Fatalf("Failed to find users: %v", result.Error)
	}
	if len(users) != 1 {
		t.Errorf("Expected 1 user, got %d", len(users))
	}

	// 删除记录
	result = gormDB.Delete(&foundUser)
	if result.Error != nil {
		t.Fatalf("Failed to delete user: %v", result.Error)
	}

	// 验证删除
	var count int64
	gormDB.Model(&User{}).Count(&count)
	if count != 0 {
		t.Errorf("Expected 0 users after deletion, got %d", count)
	}

	t.Log("GORM integration test passed successfully!")
}

// TestGormWithRateLimit 测试 GORM 操作的速率限制
func TestGormWithRateLimit(t *testing.T) {
	// 创建标准 SQL 数据库
	sqlDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	// 使用较低的速率限制
	rateLimitedDB := Wrap(sqlDB, rate.Limit(5), 2)
	defer rateLimitedDB.Close()

	// 使用 GORM 连接
	gormDB, err := gorm.Open(sqlite.Dialector{
		Conn: rateLimitedDB,
	}, &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to initialize GORM: %v", err)
	}

	// 自动迁移
	if err := gormDB.AutoMigrate(&User{}); err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	start := time.Now()

	// 执行多次插入操作
	for i := 0; i < 10; i++ {
		user := User{
			Name:  "User" + string(rune('A'+i)),
			Email: "user" + string(rune('a'+i)) + "@example.com",
		}
		if err := gormDB.Create(&user).Error; err != nil {
			t.Fatalf("Failed to create user %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)

	// 10 个操作，速率 5/s，burst 2，应该至少需要 1.6 秒
	expectedMinDuration := 1500 * time.Millisecond
	if elapsed < expectedMinDuration {
		t.Errorf("Rate limiting not working with GORM. Expected at least %v, got %v", expectedMinDuration, elapsed)
	}

	t.Logf("10 GORM operations with rate limit 5/s took %v", elapsed)

	// 验证所有记录都已创建
	var count int64
	gormDB.Model(&User{}).Count(&count)
	if count != 10 {
		t.Errorf("Expected 10 users, got %d", count)
	}
}

// TestGormComplexQueries 测试 GORM 复杂查询
func TestGormComplexQueries(t *testing.T) {
	// 创建标准 SQL 数据库
	sqlDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	// 使用 RateLimitedDB 包装
	rateLimitedDB := Wrap(sqlDB, rate.Limit(20), 10)
	defer rateLimitedDB.Close()

	// 使用 GORM 连接
	gormDB, err := gorm.Open(sqlite.Dialector{
		Conn: rateLimitedDB,
	}, &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to initialize GORM: %v", err)
	}

	// 自动迁移
	if err := gormDB.AutoMigrate(&User{}); err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}

	// 批量创建测试数据
	users := []User{
		{Name: "Alice", Email: "alice@example.com"},
		{Name: "Bob", Email: "bob@example.com"},
		{Name: "Charlie", Email: "charlie@example.com"},
		{Name: "David", Email: "david@example.com"},
		{Name: "Eve", Email: "eve@example.com"},
	}
	if err := gormDB.Create(&users).Error; err != nil {
		t.Fatalf("Failed to create users: %v", err)
	}

	// 测试 Where 查询
	var foundUsers []User
	result := gormDB.Where("name LIKE ?", "%e%").Find(&foundUsers)
	if result.Error != nil {
		t.Fatalf("Failed to query users: %v", result.Error)
	}
	if len(foundUsers) != 3 { // Alice, Charlie, Eve
		t.Errorf("Expected 3 users with 'e' in name, got %d", len(foundUsers))
	}

	// 测试排序
	var sortedUsers []User
	result = gormDB.Order("name DESC").Find(&sortedUsers)
	if result.Error != nil {
		t.Fatalf("Failed to query sorted users: %v", result.Error)
	}
	if sortedUsers[0].Name != "Eve" {
		t.Errorf("Expected first user to be 'Eve', got '%s'", sortedUsers[0].Name)
	}

	// 测试限制和偏移
	var limitedUsers []User
	result = gormDB.Limit(2).Offset(1).Find(&limitedUsers)
	if result.Error != nil {
		t.Fatalf("Failed to query limited users: %v", result.Error)
	}
	if len(limitedUsers) != 2 {
		t.Errorf("Expected 2 users, got %d", len(limitedUsers))
	}

	// 测试计数
	var count int64
	result = gormDB.Model(&User{}).Where("name LIKE ?", "A%").Count(&count)
	if result.Error != nil {
		t.Fatalf("Failed to count users: %v", result.Error)
	}
	if count != 1 { // Alice
		t.Errorf("Expected 1 user starting with 'A', got %d", count)
	}

	// 测试批量更新
	result = gormDB.Model(&User{}).Where("name LIKE ?", "%e%").Update("email", "updated@example.com")
	if result.Error != nil {
		t.Fatalf("Failed to update users: %v", result.Error)
	}
	if result.RowsAffected != 3 {
		t.Errorf("Expected 3 rows affected, got %d", result.RowsAffected)
	}

	t.Log("GORM complex queries test passed successfully!")
}
