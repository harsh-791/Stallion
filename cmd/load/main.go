package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"auditlog/internal/audit"
	"auditlog/internal/store"
)

const (
	expectedSHA = "e968244bc33aaf0bc13c98d253f49819da1236620eae8cf51de31b7fb34d4dc1"
	chunkSize   = 50000
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if len(os.Args) != 2 {
		log.Error("usage: load <audit_events.ndjson.gz>")
		os.Exit(2)
	}
	if err := run(context.Background(), log, os.Args[1], os.Getenv("DATABASE_URL")); err != nil {
		log.Error("load failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, path, databaseURL string) error {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	sum, err := fileSHA256(path)
	if err != nil {
		return err
	}
	if !strings.EqualFold(sum, expectedSHA) {
		return fmt.Errorf("checksum mismatch: got %s want %s", sum, expectedSHA)
	}
	log.Info("checksum ok", "sha256", sum)

	loader, err := store.NewLoader(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer loader.Close(ctx)

	if err := loader.PrepareLoad(ctx); err != nil {
		return err
	}

	start := time.Now()
	lines, inserted, streamErr := stream(ctx, log, loader, path)
	// FinishLoad rebuilds the list indexes even when the stream fails,
	// and its time is part of the load duration.
	finishErr := loader.FinishLoad(context.Background())
	if streamErr != nil {
		if finishErr != nil {
			return fmt.Errorf("%w; restoring indexes: %v", streamErr, finishErr)
		}
		return streamErr
	}
	if finishErr != nil {
		return finishErr
	}
	log.Info("load complete",
		"lines", lines,
		"inserted", inserted,
		"duplicates", lines-inserted,
		"seconds", time.Since(start).Seconds(),
	)
	return nil
}

func stream(ctx context.Context, log *slog.Logger, loader *store.Loader, path string) (int64, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return 0, 0, fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	batch := make([]audit.Event, 0, chunkSize)
	var lines, inserted, seq int64
	start := time.Now()
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := loader.LoadChunk(ctx, batch, seq-int64(len(batch))+1)
		if err != nil {
			return err
		}
		inserted += n
		log.Info("chunk", "lines", lines, "inserted", inserted, "seconds", time.Since(start).Seconds())
		batch = batch[:0]
		return nil
	}

	var lineNo int
	for sc.Scan() {
		lineNo++
		raw := bytes.TrimSpace(sc.Bytes())
		if len(raw) == 0 {
			continue
		}
		var ev audit.Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			return lines, inserted, fmt.Errorf("line %d: %w", lineNo, err)
		}
		if err := ev.Validate(); err != nil {
			return lines, inserted, fmt.Errorf("line %d: %w", lineNo, err)
		}
		seq++
		lines++
		batch = append(batch, ev)
		if len(batch) >= chunkSize {
			if err := flush(); err != nil {
				return lines, inserted, err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return lines, inserted, err
	}
	if err := flush(); err != nil {
		return lines, inserted, err
	}
	return lines, inserted, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
