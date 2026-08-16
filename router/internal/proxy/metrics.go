package proxy

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	metricWindowCapacity  = 4096
	maxTenantMetricSeries = 4096
)

type intSampleWindow struct {
	values []int64
	next   int
	count  uint64
	sum    int64
}

func (w *intSampleWindow) observe(value int64) {
	w.count++
	w.sum += value
	if len(w.values) < metricWindowCapacity {
		w.values = append(w.values, value)
		return
	}
	w.values[w.next] = value
	w.next = (w.next + 1) % metricWindowCapacity
}

type floatSampleWindow struct {
	values []float64
	next   int
	count  uint64
	sum    float64
}

func (w *floatSampleWindow) observe(value float64) {
	w.count++
	w.sum += value
	if len(w.values) < metricWindowCapacity {
		w.values = append(w.values, value)
		return
	}
	w.values[w.next] = value
	w.next = (w.next + 1) % metricWindowCapacity
}

type Metrics struct {
	mu             sync.Mutex
	requests       map[string]int64
	selected       map[string]int64
	tenantRequests map[string]int64
	routeLatencyUS intSampleWindow
	proxyTTFTMS    intSampleWindow
	proxyTotalMS   intSampleWindow
	cachePrecision floatSampleWindow
	cacheRecall    floatSampleWindow
}

func NewMetrics() *Metrics {
	return &Metrics{
		requests:       make(map[string]int64),
		selected:       make(map[string]int64),
		tenantRequests: make(map[string]int64),
	}
}

func (m *Metrics) ObserveRoute(strategy string, result string, workerHash string, routeLatency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[strategy+"|"+result]++
	if workerHash != "" {
		m.selected[strategy+"|"+workerHash]++
	}
	m.routeLatencyUS.observe(routeLatency.Microseconds())
}

func (m *Metrics) ObserveTenantRoute(strategy string, tenantHash string, result string) {
	if tenantHash == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := strategy + "|" + tenantHash + "|" + result
	if _, exists := m.tenantRequests[key]; !exists && len(m.tenantRequests) >= maxTenantMetricSeries {
		key = strategy + "|overflow|" + result
	}
	m.tenantRequests[key]++
}

func (m *Metrics) ObserveProxy(ttft time.Duration, total time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ttft >= 0 {
		m.proxyTTFTMS.observe(ttft.Milliseconds())
	}
	m.proxyTotalMS.observe(total.Milliseconds())
}

func (m *Metrics) ObserveCachePrediction(predictedOverlap int, actualHit int, actualMiss int) {
	if predictedOverlap < 0 || actualHit < 0 || actualMiss < 0 {
		return
	}
	precision := floatRatio(actualHit, predictedOverlap, 1.0)
	recall := floatRatio(actualHit, actualHit+actualMiss, 1.0)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cachePrecision.observe(precision)
	m.cacheRecall.observe(recall)
}

func (m *Metrics) PrometheusText() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	b.WriteString("# TYPE router_requests_total counter\n")
	writeCounterMap(&b, "router_requests_total", []string{"strategy", "result"}, m.requests)
	b.WriteString("# TYPE router_selected_worker_total counter\n")
	writeCounterMap(&b, "router_selected_worker_total", []string{"strategy", "worker_hash"}, m.selected)
	b.WriteString("# TYPE router_tenant_requests_total counter\n")
	writeCounterMap(&b, "router_tenant_requests_total", []string{"strategy", "tenant_hash", "result"}, m.tenantRequests)
	b.WriteString("# TYPE router_route_latency_microseconds summary\n")
	writeIntSummary(&b, "router_route_latency_microseconds", &m.routeLatencyUS)
	b.WriteString("# TYPE router_proxy_ttft_milliseconds summary\n")
	writeIntSummary(&b, "router_proxy_ttft_milliseconds", &m.proxyTTFTMS)
	b.WriteString("# TYPE router_proxy_total_milliseconds summary\n")
	writeIntSummary(&b, "router_proxy_total_milliseconds", &m.proxyTotalMS)
	b.WriteString("# TYPE router_cache_prediction_precision summary\n")
	writeFloatSummary(&b, "router_cache_prediction_precision", &m.cachePrecision)
	b.WriteString("# TYPE router_cache_prediction_recall summary\n")
	writeFloatSummary(&b, "router_cache_prediction_recall", &m.cacheRecall)
	return b.String()
}

func writeCounterMap(b *strings.Builder, name string, labelNames []string, values map[string]int64) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.Split(key, "|")
		labels := make([]string, 0, len(labelNames))
		for i, label := range labelNames {
			value := ""
			if i < len(parts) {
				value = parts[i]
			}
			labels = append(labels, fmt.Sprintf("%s=%q", label, value))
		}
		fmt.Fprintf(b, "%s{%s} %d\n", name, strings.Join(labels, ","), values[key])
	}
}

func writeIntSummary(b *strings.Builder, name string, window *intSampleWindow) {
	fmt.Fprintf(b, "%s_count %d\n", name, window.count)
	fmt.Fprintf(b, "%s_sum %d\n", name, window.sum)
	if len(window.values) == 0 {
		return
	}
	ordered := append([]int64(nil), window.values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	fmt.Fprintf(b, "%s{quantile=\"0.50\"} %d\n", name, quantile(ordered, 0.50))
	fmt.Fprintf(b, "%s{quantile=\"0.95\"} %d\n", name, quantile(ordered, 0.95))
	fmt.Fprintf(b, "%s{quantile=\"0.99\"} %d\n", name, quantile(ordered, 0.99))
}

func writeFloatSummary(b *strings.Builder, name string, window *floatSampleWindow) {
	fmt.Fprintf(b, "%s_count %d\n", name, window.count)
	fmt.Fprintf(b, "%s_sum %.6f\n", name, window.sum)
	if len(window.values) == 0 {
		return
	}
	ordered := append([]float64(nil), window.values...)
	sort.Float64s(ordered)
	fmt.Fprintf(b, "%s{quantile=\"0.50\"} %.6f\n", name, floatQuantile(ordered, 0.50))
	fmt.Fprintf(b, "%s{quantile=\"0.95\"} %.6f\n", name, floatQuantile(ordered, 0.95))
	fmt.Fprintf(b, "%s{quantile=\"0.99\"} %.6f\n", name, floatQuantile(ordered, 0.99))
}

func quantile(values []int64, q float64) int64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func floatQuantile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func floatRatio(numerator int, denominator int, defaultValue float64) float64 {
	if denominator <= 0 {
		return defaultValue
	}
	return float64(numerator) / float64(denominator)
}
