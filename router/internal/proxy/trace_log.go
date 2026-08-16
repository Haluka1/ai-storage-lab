package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type TraceLogger struct {
	mu   sync.Mutex
	file *os.File
}

type traceContext struct {
	TraceID      string
	ParentSpanID string
}

func NewTraceLogger(path string) (*TraceLogger, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &TraceLogger{file: file}, nil
}

func (l *TraceLogger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	return l.file.Close()
}

func (l *TraceLogger) WriteSpan(runID string, ctx traceContext, name string, parentSpanID string, started time.Time, result string, attrs map[string]any) string {
	spanID := spanIDFor(runID, ctx.TraceID, name, started)
	if l == nil || l.file == nil {
		return spanID
	}
	if parentSpanID == "" {
		parentSpanID = ctx.ParentSpanID
	}
	duration := time.Since(started)
	if duration < 0 {
		duration = 0
	}
	record := map[string]any{
		"timestamp":      time.Now().UTC().Format(time.RFC3339Nano),
		"level":          "info",
		"component":      "router",
		"run_id":         runID,
		"trace_id":       ctx.TraceID,
		"span_id":        spanID,
		"parent_span_id": parentSpanID,
		"name":           name,
		"event":          name,
		"duration_ms":    float64(duration.Microseconds()) / 1000.0,
		"result":         result,
		"attributes":     attrs,
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return spanID
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.file.Write(append(raw, '\n')); err != nil {
		return spanID
	}
	_ = l.file.Sync()
	return spanID
}

func traceContextFromRequest(traceparent string, fallbackNonce string) traceContext {
	if traceID := traceIDFromTraceparent(traceparent); traceID != "" {
		return traceContext{TraceID: traceID, ParentSpanID: spanIDFromTraceparent(traceparent)}
	}
	sum := sha256.Sum256([]byte("router-trace:" + fallbackNonce))
	return traceContext{TraceID: hex.EncodeToString(sum[:])[:32]}
}

func spanIDFromTraceparent(value string) string {
	parts := splitTraceparent(value)
	if len(parts) < 4 {
		return ""
	}
	spanID := parts[2]
	if len(spanID) != 16 || spanID == "0000000000000000" {
		return ""
	}
	for _, ch := range spanID {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return ""
		}
	}
	return spanID
}

func spanIDFor(runID string, traceID string, name string, started time.Time) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + traceID + "\x00" + name + "\x00" + started.Format(time.RFC3339Nano)))
	return hex.EncodeToString(sum[:])[:16]
}

func splitTraceparent(value string) []string {
	return strings.Split(strings.TrimSpace(value), "-")
}
