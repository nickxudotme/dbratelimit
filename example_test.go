package dbratelimit_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/time/rate"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"dbratelimit"
)

// ExampleWrap_standardSQL 展示如何使用标准 database/sql
func ExampleWrap_standardSQL() {
	// 1. 创建标准的 SQL 数据库连接
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 2. 使用 RateLimitedDB 包装数据库
	// 参数：每秒允许 10 个请求，突发容量为 5
	rateLimitedDB := dbratelimit.Wrap(db, rate.Limit(10), 5)
	defer rateLimitedDB.Close()

	// 3. 创建表
	_, err = rateLimitedDB.ExecContext(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	// 4. 插入数据（会受到速率限制）
	_, err = rateLimitedDB.ExecContext(context.Background(),
		"INSERT INTO users (name) VALUES (?)", "Alice")
	if err != nil {
		log.Fatal(err)
	}

	// 5. 查询数据（会受到速率限制）
	rows, err := rateLimitedDB.QueryContext(context.Background(),
		"SELECT id, name FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("User: id=%d, name=%s\n", id, name)
	}

	// Output:
	// User: id=1, name=Alice
}

// ExampleWrap_gorm 展示如何与 GORM 集成使用
func ExampleWrap_gorm() {
	// 定义模型
	type User struct {
		ID    uint   `gorm:"primaryKey"`
		Name  string `gorm:"not null"`
		Email string `gorm:"not null"`
	}

	// 1. 创建标准的 SQL 数据库连接
	sqlDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	// 2. 使用 RateLimitedDB 包装数据库
	// 参数：每秒允许 20 个请求，突发容量为 10
	rateLimitedDB := dbratelimit.Wrap(sqlDB, rate.Limit(20), 10)
	defer rateLimitedDB.Close()

	// 3. 使用 GORM 连接，将 RateLimitedDB 作为 ConnPool
	gormDB, err := gorm.Open(sqlite.Dialector{
		Conn: rateLimitedDB,
	}, &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// 4. 自动迁移（会受到速率限制）
	if err := gormDB.AutoMigrate(&User{}); err != nil {
		log.Fatal(err)
	}

	// 5. 创建记录（会受到速率限制）
	user := User{Name: "Bob", Email: "bob@example.com"}
	if err := gormDB.Create(&user).Error; err != nil {
		log.Fatal(err)
	}

	// 6. 查询记录（会受到速率限制）
	var foundUser User
	if err := gormDB.First(&foundUser, user.ID).Error; err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found user: %s (%s)\n", foundUser.Name, foundUser.Email)

	// 7. 更新记录（会受到速率限制）
	if err := gormDB.Model(&foundUser).Update("Email", "bob.new@example.com").Error; err != nil {
		log.Fatal(err)
	}

	// 8. 删除记录（会受到速率限制）
	if err := gormDB.Delete(&foundUser).Error; err != nil {
		log.Fatal(err)
	}

	fmt.Println("GORM operations completed successfully!")

	// Output:
	// Found user: Bob (bob@example.com)
	// GORM operations completed successfully!
}

// ExampleWrap_contextCancellation 展示如何使用上下文取消
func ExampleWrap_contextCancellation() {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 设置较低的速率限制
	rateLimitedDB := dbratelimit.Wrap(db, rate.Limit(10), 2)
	defer rateLimitedDB.Close()

	// 创建表
	_, err = rateLimitedDB.ExecContext(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	// 使用带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// 前两个查询会成功（使用 burst）
	for i := 1; i <= 2; i++ {
		rows, err := rateLimitedDB.QueryContext(ctx, "SELECT * FROM users")
		if err != nil {
			fmt.Printf("Query %d failed: %v\n", i, err)
		} else {
			rows.Close()
			fmt.Printf("Query %d succeeded\n", i)
		}
	}

	fmt.Println("Context cancellation example completed")

	// Output:
	// Query 1 succeeded
	// Query 2 succeeded
	// Context cancellation example completed
}

// ExampleWrap_rawAccess 展示如何访问原始数据库（绕过速率限制）
func ExampleWrap_rawAccess() {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rateLimitedDB := dbratelimit.Wrap(db, rate.Limit(1), 1)
	defer rateLimitedDB.Close()

	// 通过 RateLimitedDB 访问（受速率限制）
	fmt.Println("Using rate-limited DB...")
	_, err = rateLimitedDB.ExecContext(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		log.Fatal(err)
	}

	// 通过 Raw() 访问原始数据库（绕过速率限制）
	fmt.Println("Using raw DB (bypassing rate limit)...")
	rawDB := rateLimitedDB.Raw()
	_, err = rawDB.ExecContext(context.Background(),
		"INSERT INTO users (name) VALUES (?)", "Alice")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Operations completed!")

	// Output:
	// Using rate-limited DB...
	// Using raw DB (bypassing rate limit)...
	// Operations completed!
}
