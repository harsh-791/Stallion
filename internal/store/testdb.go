package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
)

func TestDatabaseName() string {
	return fmt.Sprintf("audit_test_%d", os.Getpid())
}

// PrepareTestDatabase recreates the audit_test database from db/init.sql.
// It does not modify the audit database used by the running service.
func PrepareTestDatabase(ctx context.Context, databaseURL string) (*Store, error) {
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer admin.Close(ctx)

	name := TestDatabaseName()
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
			return nil, fmt.Errorf("create %s: %w", name, err)
		}
	}

	testURL, err := withDatabase(databaseURL, name)
	if err != nil {
		return nil, err
	}
	conn, err := pgx.Connect(ctx, testURL)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `DROP SCHEMA IF EXISTS public CASCADE`); err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `CREATE SCHEMA public`); err != nil {
		return nil, err
	}
	schemaPath, err := findSchema()
	if err != nil {
		return nil, err
	}
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}
	result := conn.PgConn().Exec(ctx, string(schema))
	if _, err := result.ReadAll(); err != nil {
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return Connect(ctx, testURL)
}

// DropTestDatabase removes this process's test database. Safe to call when tests are finished.
func DropTestDatabase(ctx context.Context, databaseURL string) error {
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer admin.Close(ctx)
	name := TestDatabaseName()
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, name)
	_, err = admin.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, name))
	return err
}

func withDatabase(raw, name string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	return u.String(), nil
}

func findSchema() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(dir, "db", "init.sql")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("db/init.sql not found")
		}
		dir = parent
	}
}
