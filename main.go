package dbratelimit

import (
	"context"
	"database/sql"

	"golang.org/x/time/rate"
	"gorm.io/gorm"
)

var _ gorm.ConnPool = (*RateLimitedDB)(nil)

type RateLimitedDB struct {
	db      *sql.DB
	limiter *rate.Limiter
}

func Wrap(db *sql.DB, limit rate.Limit, burst int) *RateLimitedDB {
	return &RateLimitedDB{
		db:      db,
		limiter: rate.NewLimiter(limit, burst),
	}
}

// wait blocks until limiter allows or ctx cancels
func (r *RateLimitedDB) wait(ctx context.Context) error {
	return r.limiter.Wait(ctx)
}

func (r *RateLimitedDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := r.wait(ctx); err != nil {
		return nil, err
	}
	return r.db.QueryContext(ctx, query, args...)
}

func (r *RateLimitedDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	// Note: QueryRowContext doesn't return error, so we can't check wait() error here
	// The error will be returned when Scan() is called on the Row
	_ = r.wait(ctx)
	return r.db.QueryRowContext(ctx, query, args...)
}

func (r *RateLimitedDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := r.wait(ctx); err != nil {
		return nil, err
	}
	return r.db.ExecContext(ctx, query, args...)
}

func (r *RateLimitedDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if err := r.wait(ctx); err != nil {
		return nil, err
	}
	return r.db.PrepareContext(ctx, query)
}

func (r *RateLimitedDB) Close() error {
	return r.db.Close()
}

func (r *RateLimitedDB) Ping() error {
	return r.db.Ping()
}

func (r *RateLimitedDB) Conn(ctx context.Context) (*sql.Conn, error) {
	return r.db.Conn(ctx)
}

func (r *RateLimitedDB) Raw() *sql.DB {
	return r.db
}
