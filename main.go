package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/proxy"
)

type Config struct {
	BaseURL           string
	Method            string
	RequestBody       string
	Headers           []string
	Cookies           []string
	Mode              string
	Length            int
	Prefix            string
	Suffix            string
	Seed              int64
	TotalChecks       int
	ThreadCount       int
	OutputFile        string
	OutputAllFile     string
	OutputFormat      string
	RequestTimeout    time.Duration
	OverallTimeout    time.Duration
	UAFile            string
	Delay             time.Duration
	DelayJitter       time.Duration
	Retries           int
	MaxBackoff        time.Duration
	Proxy             string
	ProxyFile         string
	ProxyType         string
	Quiet             bool
	Verbose           bool
	LogLevel          string
	LogFile           string
	LogFormat         string
	CheckFile         string
	ResumeFile        string
	ResumeFlushEvery  int
	StatsInterval     time.Duration
	RateLimit         float64
	RateLimitBurst    int
	PerProxyRateLimit float64
	HeaderFile        string
	AvailableStatuses []int
	TakenStatuses     []int
	RetryStatuses     []int
	AvailableBody     string
	AvailableRegex    string
	DryRun            bool
	TLSInsecure       bool
	BasicAuth         string
	BearerToken       string
	MaxIdleConns      int
	IdleConnTimeout   time.Duration
	MaxBodyBytes      int64
	MaxRedirects      int
	FollowRedirects   bool
	StopOnAvailable   int64
	Color             bool
	Version           bool
	CleanOnStart      bool
	Stealth           bool
	MinDelay          time.Duration
	MaxDelay          time.Duration
	Jitter            float64
	RotateHeaders     bool
	RandomOrder       bool
	DisableHTTP2      bool
	CustomDNS         string
	UserAgentRot      bool
	ConnPoolSize      int
	RetryOnNetwork    bool
	Fingerprint       string
	SmartRetry        bool
	CircuitBreaker    bool
	AdaptiveRate      bool
	RequestJitter     bool
	HeaderShuffle     bool
	SocketTimeout     time.Duration
	KeepAlive         time.Duration
	MaxConnsPerHost   int
	ReadBufferSize    int
	WriteBufferSize   int
}

var defaultConfig = Config{
	BaseURL:           "https://github.com/%s",
	Method:            "GET",
	Mode:              "letters",
	Length:            4,
	TotalChecks:       100,
	ThreadCount:       10,
	OutputFile:        "available.txt",
	OutputAllFile:     "all_results.txt",
	OutputFormat:      "plain",
	RequestTimeout:    5 * time.Second,
	Retries:           2,
	MaxBackoff:        5 * time.Second,
	ProxyType:         "http",
	LogLevel:          "info",
	LogFormat:         "text",
	StatsInterval:     10 * time.Second,
	AvailableStatuses: []int{404},
	TakenStatuses:     []int{200},
	RetryStatuses:     []int{429, 500, 502, 503, 504},
	MaxIdleConns:      100,
	IdleConnTimeout:   90 * time.Second,
	MaxBodyBytes:      2 << 20,
	MaxRedirects:      10,
	FollowRedirects:   true,
	ResumeFlushEvery:  100,
	Color:             true,
	Stealth:           true,
	MinDelay:          0,
	MaxDelay:          0,
	Jitter:            0.2,
	RotateHeaders:     true,
	RandomOrder:       true,
	DisableHTTP2:      false,
	UserAgentRot:      true,
	ConnPoolSize:      200,
	RetryOnNetwork:    true,
	SmartRetry:        true,
	CircuitBreaker:    true,
	AdaptiveRate:      true,
	RequestJitter:     true,
	HeaderShuffle:     true,
	SocketTimeout:     30 * time.Second,
	KeepAlive:         30 * time.Second,
	MaxConnsPerHost:   50,
	ReadBufferSize:    64 * 1024,
	WriteBufferSize:   64 * 1024,
}

type LeveledLogger struct {
	*log.Logger
	level int
	json  bool
	mu    sync.Mutex
	color bool
}

const (
	LogDebug = iota
	LogInfo
	LogWarn
	LogError
)

const (
	ansiReset  = "\033[0m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
	ansiGray   = "\033[90m"
)

func parseLevel(s string) int {
	switch strings.ToLower(s) {
	case "debug":
		return LogDebug
	case "warn", "warning":
		return LogWarn
	case "error":
		return LogError
	default:
		return LogInfo
	}
}

func NewLeveledLogger(w io.Writer, levelStr, format string, color bool) *LeveledLogger {
	return &LeveledLogger{
		Logger: log.New(w, "", log.LstdFlags),
		level:  parseLevel(levelStr),
		json:   strings.EqualFold(format, "json"),
		color:  color,
	}
}

func (l *LeveledLogger) log(level int, tag, color string, msg string, fields map[string]interface{}) {
	if l.level > level {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.json {
		rec := map[string]interface{}{
			"time":  time.Now().Format(time.RFC3339Nano),
			"level": tag,
			"msg":   msg,
		}
		for k, v := range fields {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		l.Logger.Println(string(b))
		return
	}
	if l.color && color != "" {
		l.Logger.Printf("%s[%s]%s %s", color, tag, ansiReset, msg)
	} else {
		l.Logger.Printf("[%s] %s", tag, msg)
	}
}

func (l *LeveledLogger) Debug(v ...interface{}) {
	l.log(LogDebug, "DEBUG", ansiGray, fmt.Sprint(v...), nil)
}
func (l *LeveledLogger) Debugf(format string, v ...interface{}) {
	l.log(LogDebug, "DEBUG", ansiGray, fmt.Sprintf(format, v...), nil)
}
func (l *LeveledLogger) Info(v ...interface{}) {
	l.log(LogInfo, "INFO", ansiCyan, fmt.Sprint(v...), nil)
}
func (l *LeveledLogger) Infof(format string, v ...interface{}) {
	l.log(LogInfo, "INFO", ansiCyan, fmt.Sprintf(format, v...), nil)
}
func (l *LeveledLogger) Warn(v ...interface{}) {
	l.log(LogWarn, "WARN", ansiYellow, fmt.Sprint(v...), nil)
}
func (l *LeveledLogger) Warnf(format string, v ...interface{}) {
	l.log(LogWarn, "WARN", ansiYellow, fmt.Sprintf(format, v...), nil)
}
func (l *LeveledLogger) Error(v ...interface{}) {
	l.log(LogError, "ERROR", ansiRed, fmt.Sprint(v...), nil)
}
func (l *LeveledLogger) Errorf(format string, v ...interface{}) {
	l.log(LogError, "ERROR", ansiRed, fmt.Sprintf(format, v...), nil)
}
func (l *LeveledLogger) Available(name string) {
	l.log(LogInfo, "+", ansiGreen, fmt.Sprintf("[+] %s", name), nil)
}
func (l *LeveledLogger) Taken(name string) {
	l.log(LogInfo, "-", ansiGray, fmt.Sprintf("[-] %s", name), nil)
}
func (l *LeveledLogger) Fatal(v ...interface{}) {
	l.Error(v...)
	os.Exit(1)
}
func (l *LeveledLogger) Fatalf(format string, v ...interface{}) {
	l.Errorf(format, v...)
	os.Exit(1)
}

func loadLines(filename string) ([]string, error) {
	if filename == "" {
		return nil, nil
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

func loadUserAgents(filename string) ([]string, error) {
	if filename == "" {
		return []string{
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
			"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 14.6; rv:130.0) Gecko/20100101 Firefox/130.0",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
			"Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
			"Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Mobile Safari/537.36",
		}, nil
	}
	agents, err := loadLines(filename)
	if err != nil {
		return nil, err
	}
	if len(agents) == 0 {
		return nil, fmt.Errorf("no user agents found in %s", filename)
	}
	return agents, nil
}

func parseStatusList(s string) ([]int, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	var statuses []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		code, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid status code %q: %w", p, err)
		}
		if code < 100 || code > 599 {
			return nil, fmt.Errorf("status code out of range: %d", code)
		}
		statuses = append(statuses, code)
	}
	return statuses, nil
}

func containsInt(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

type RateLimiter struct {
	rate   float64
	burst  float64
	tokens float64
	mu     sync.Mutex
	last   time.Time
}

func NewRateLimiter(rate float64, burst int) *RateLimiter {
	if rate <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = int(rate)
		if burst < 1 {
			burst = 1
		}
	}
	return &RateLimiter{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   time.Now(),
	}
}

func (rl *RateLimiter) Wait(ctx context.Context) error {
	if rl == nil {
		return nil
	}
	for {
		rl.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(rl.last).Seconds()
		if elapsed > 0 {
			rl.tokens += elapsed * rl.rate
			if rl.tokens > rl.burst {
				rl.tokens = rl.burst
			}
			rl.last = now
		}
		if rl.tokens >= 1 {
			rl.tokens--
			rl.mu.Unlock()
			return nil
		}
		need := (1 - rl.tokens) / rl.rate * float64(time.Second)
		rl.mu.Unlock()
		timer := time.NewTimer(time.Duration(need))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

type ProxyManager struct {
	proxies       []string
	index         uint32
	dead          map[string]int
	failThreshold int
	mu            sync.Mutex
	perProxyRL    map[string]*RateLimiter
}

func NewProxyManager(proxies []string, perProxyRate float64, failThreshold int) *ProxyManager {
	pm := &ProxyManager{
		proxies:       proxies,
		dead:          make(map[string]int),
		failThreshold: failThreshold,
		perProxyRL:    make(map[string]*RateLimiter),
	}
	if perProxyRate > 0 && len(proxies) > 0 {
		burst := int(perProxyRate)
		if burst < 1 {
			burst = 1
		}
		for _, p := range proxies {
			pm.perProxyRL[p] = NewRateLimiter(perProxyRate, burst)
		}
	}
	return pm
}

func (pm *ProxyManager) Empty() bool { return len(pm.proxies) == 0 }

func (pm *ProxyManager) Next(ctx context.Context) (string, *RateLimiter, error) {
	if len(pm.proxies) == 0 {
		return "", nil, nil
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	n := len(pm.proxies)
	for i := 0; i < n; i++ {
		idx := atomic.AddUint32(&pm.index, 1) - 1
		proxy := pm.proxies[idx%uint32(n)]
		if pm.dead[proxy] >= pm.failThreshold {
			continue
		}
		return proxy, pm.perProxyRL[proxy], nil
	}
	for p := range pm.dead {
		delete(pm.dead, p)
	}
	proxy := pm.proxies[0]
	return proxy, pm.perProxyRL[proxy], nil
}

func (pm *ProxyManager) MarkDead(proxy string) {
	if proxy == "" {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.dead[proxy]++
}

func (pm *ProxyManager) MarkAlive(proxy string) {
	if proxy == "" {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.dead, proxy)
}

type Result struct {
	Username     string        `json:"username"`
	Available    bool          `json:"available"`
	StatusCode   int           `json:"status_code,omitempty"`
	ResponseTime time.Duration `json:"response_time_ns,omitempty"`
	Error        string        `json:"error,omitempty"`
	Attempts     int           `json:"attempts,omitempty"`
	Proxy        string        `json:"proxy,omitempty"`
	Timestamp    time.Time     `json:"timestamp"`
}

type proxyCtxKey struct{}

type transportPool struct {
	base    *http.Transport
	mu      sync.Mutex
	byProxy map[string]*http.Transport
	cfg     *Config
}

func newTransportPool(cfg *Config) *transportPool {
	base := &http.Transport{
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxConnsPerHost,
		IdleConnTimeout:     cfg.IdleConnTimeout,
		ForceAttemptHTTP2:   !cfg.DisableHTTP2,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.TLSInsecure,
			MinVersion:         tls.VersionTLS12,
			MaxVersion:         tls.VersionTLS13,
		},
		DialContext: (&net.Dialer{
			Timeout:   cfg.SocketTimeout,
			KeepAlive: cfg.KeepAlive,
		}).DialContext,
		ReadBufferSize:  cfg.ReadBufferSize,
		WriteBufferSize: cfg.WriteBufferSize,
		MaxConnsPerHost: cfg.MaxConnsPerHost,
	}
	if cfg.Proxy == "" && cfg.ProxyFile == "" {
	}
	return &transportPool{
		base:    base,
		byProxy: make(map[string]*http.Transport),
		cfg:     cfg,
	}
}

func (tp *transportPool) get(proxyURL string) (http.RoundTripper, error) {
	if proxyURL == "" {
		return tp.base, nil
	}
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if t, ok := tp.byProxy[proxyURL]; ok {
		return t, nil
	}
	t := tp.base.Clone()
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy url %q: %w", proxyURL, err)
	}
	switch strings.ToLower(tp.cfg.ProxyType) {
	case "socks5", "socks", "socks5h":
		dialer, err := proxy.SOCKS5("tcp", u.Host, nil, proxy.Direct)
		if err != nil {
			return nil, fmt.Errorf("socks5 dialer: %w", err)
		}
		t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			type ctxDialer interface {
				DialContext(ctx context.Context, network, addr string) (net.Conn, error)
			}
			if cd, ok := dialer.(ctxDialer); ok {
				return cd.DialContext(ctx, network, addr)
			}
			return dialer.Dial(network, addr)
		}
		t.Proxy = nil
	default:
		t.Proxy = http.ProxyURL(u)
	}
	tp.byProxy[proxyURL] = t
	return t, nil
}

type customTransport struct {
	pool *transportPool
}

func (ct *customTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	proxyURL, _ := req.Context().Value(proxyCtxKey{}).(string)
	rt, err := ct.pool.get(proxyURL)
	if err != nil {
		return nil, err
	}
	return rt.RoundTrip(req)
}

type Checker struct {
	config     *Config
	client     *http.Client
	userAgents []string
	headers    []string
	logger     *LeveledLogger
	rlGlobal   *RateLimiter
	pm         *ProxyManager
	re         *regexp.Regexp
	uaIdx      uint32
}

func NewChecker(cfg *Config, logger *LeveledLogger) (*Checker, error) {
	uas, err := loadUserAgents(cfg.UAFile)
	if err != nil {
		return nil, fmt.Errorf("load user agents: %w", err)
	}
	headers, err := loadLines(cfg.HeaderFile)
	if err != nil {
		return nil, fmt.Errorf("load headers: %w", err)
	}
	headers = append(headers, cfg.Headers...)
	proxies, err := loadProxies(cfg.Proxy, cfg.ProxyFile)
	if err != nil {
		return nil, fmt.Errorf("load proxies: %w", err)
	}
	pm := NewProxyManager(proxies, cfg.PerProxyRateLimit, 3)
	rl := NewRateLimiter(cfg.RateLimit, cfg.RateLimitBurst)
	pool := newTransportPool(cfg)
	client := &http.Client{
		Timeout:   cfg.RequestTimeout,
		Transport: &customTransport{pool: pool},
	}
	if !cfg.FollowRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	} else if cfg.MaxRedirects > 0 {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= cfg.MaxRedirects {
				return fmt.Errorf("stopped after %d redirects", cfg.MaxRedirects)
			}
			return nil
		}
	}
	var re *regexp.Regexp
	if cfg.AvailableRegex != "" {
		re, err = regexp.Compile(cfg.AvailableRegex)
		if err != nil {
			return nil, fmt.Errorf("invalid regex: %w", err)
		}
	}

	return &Checker{
		config:     cfg,
		client:     client,
		userAgents: uas,
		headers:    headers,
		logger:     logger,
		rlGlobal:   rl,
		pm:         pm,
		re:         re,
	}, nil
}

func loadProxies(proxy, proxyFile string) ([]string, error) {
	var proxies []string
	if proxy != "" {
		proxies = append(proxies, proxy)
	}
	if proxyFile != "" {
		fileProxies, err := loadLines(proxyFile)
		if err != nil {
			return nil, err
		}
		proxies = append(proxies, fileProxies...)
	}
	return proxies, nil
}

func (c *Checker) nextUA() string {
	if len(c.userAgents) == 0 {
		return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
	}
	if len(c.userAgents) == 1 {
		return c.userAgents[0]
	}
	idx := atomic.AddUint32(&c.uaIdx, 1) - 1
	return c.userAgents[idx%uint32(len(c.userAgents))]
}

func (c *Checker) buildRequest(ctx context.Context, username string) (*http.Request, error) {
	urlStr := fmt.Sprintf(c.config.BaseURL, username)
	var bodyReader io.Reader
	if c.config.Method == "POST" || c.config.Method == "PUT" || c.config.Method == "PATCH" {
		body := fmt.Sprintf(c.config.RequestBody, username)
		bodyReader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, c.config.Method, urlStr, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.nextUA())
	profiles := []struct {
		accept, lang, enc string
	}{
		{"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8", "en-US,en;q=0.9", "gzip, deflate, br"},
		{"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8", "en-GB,en;q=0.8", "gzip, deflate, br, zstd"},
		{"*/*", "en-US,en;q=0.5", "gzip, deflate"},
	}
	p := profiles[rand.Intn(len(profiles))]
	req.Header.Set("Accept", p.accept)
	req.Header.Set("Accept-Language", p.lang)
	req.Header.Set("Accept-Encoding", p.enc)
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	if bodyReader != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for _, h := range c.headers {
		parts := strings.SplitN(h, ":", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			if k != "" {
				req.Header.Set(k, v)
			}
		}
	}
	if c.config.BasicAuth != "" {
		if parts := strings.SplitN(c.config.BasicAuth, ":", 2); len(parts) == 2 {
			req.SetBasicAuth(parts[0], parts[1])
		}
	}
	if c.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.config.BearerToken)
	}
	for _, cookieStr := range c.config.Cookies {
		if parts := strings.SplitN(cookieStr, "=", 2); len(parts) == 2 {
			req.AddCookie(&http.Cookie{Name: parts[0], Value: parts[1]})
		}
	}
	return req, nil
}

func (c *Checker) classify(statusCode int, bodyStr string) (available, taken bool) {
	if containsInt(c.config.AvailableStatuses, statusCode) {
		available = true
	}
	if containsInt(c.config.TakenStatuses, statusCode) {
		taken = true
	}
	if c.config.AvailableBody != "" && strings.Contains(bodyStr, c.config.AvailableBody) {
		available = true
	}
	if c.re != nil && c.re.MatchString(bodyStr) {
		available = true
	}
	if available {
		taken = false
	}
	return
}

func (c *Checker) doOnce(ctx context.Context, username string) (Result, error) {
	res := Result{Username: username, Timestamp: time.Now()}
	req, err := c.buildRequest(ctx, username)
	if err != nil {
		res.Error = fmt.Sprintf("build request: %v", err)
		return res, err
	}
	proxyURL, proxyRL, perr := c.pm.Next(ctx)
	if perr != nil {
		res.Error = fmt.Sprintf("proxy: %v", perr)
		return res, perr
	}
	if proxyURL != "" {
		req = req.WithContext(context.WithValue(req.Context(), proxyCtxKey{}, proxyURL))
		res.Proxy = proxyURL
		if proxyRL != nil {
			if err := proxyRL.Wait(ctx); err != nil {
				res.Error = fmt.Sprintf("proxy ratelimit: %v", err)
				return res, err
			}
		}
	}
	start := time.Now()
	resp, err := c.client.Do(req)
	res.ResponseTime = time.Since(start)
	if err != nil {
		res.Error = fmt.Sprintf("request failed: %v", err)
		if proxyURL != "" {
			c.pm.MarkDead(proxyURL)
		}
		return res, err
	}
	defer resp.Body.Close()
	res.StatusCode = resp.StatusCode
	body, rerr := io.ReadAll(io.LimitReader(resp.Body, c.config.MaxBodyBytes))
	if rerr != nil {
		res.Error = fmt.Sprintf("read body: %v", rerr)
		if proxyURL != "" {
			c.pm.MarkDead(proxyURL)
		}
		return res, rerr
	}
	if proxyURL != "" {
		c.pm.MarkAlive(proxyURL)
	}
	available, taken := c.classify(resp.StatusCode, string(body))
	res.Available = available
	if !available && !taken {
		if resp.StatusCode >= 400 {
			res.Error = fmt.Sprintf("unexpected status %d", resp.StatusCode)
		} else {
			res.Error = fmt.Sprintf("undetermined status %d", resp.StatusCode)
		}
	}
	return res, nil
}

func (c *Checker) Check(ctx context.Context, username string) (Result, error) {
	if c.rlGlobal != nil {
		if err := c.rlGlobal.Wait(ctx); err != nil {
			return Result{Username: username, Error: err.Error(), Timestamp: time.Now()}, err
		}
	}
	if c.config.RequestJitter {
		j := time.Duration(rand.Int63n(int64(150 * time.Millisecond)))
		time.Sleep(j)
	}
	attempts := 0
	var last Result
	for attempts <= c.config.Retries {
		attempts++
		if ctx.Err() != nil {
			return last, ctx.Err()
		}
		res, err := c.doOnce(ctx, username)
		res.Attempts = attempts
		last = res
		if err == nil && !containsInt(c.config.RetryStatuses, res.StatusCode) && res.Error == "" {
			return res, nil
		}
		if err == nil && !containsInt(c.config.RetryStatuses, res.StatusCode) {
			return res, nil
		}
		if attempts <= c.config.Retries {
			backoff := time.Duration(float64(200*time.Millisecond) * float64(uint64(1)<<uint(attempts-1)))
			if c.config.MaxBackoff > 0 && backoff > c.config.MaxBackoff {
				backoff = c.config.MaxBackoff
			}
			jitter := time.Duration(rand.Int63n(int64(backoff/2 + 1)))
			select {
			case <-time.After(backoff/2 + jitter):
			case <-ctx.Done():
				return last, ctx.Err()
			}
		}
	}
	return last, nil
}

type Generator struct {
	config *Config
	logger *LeveledLogger
	seen   *sync.Map
	rng    *rand.Rand
}

func NewGenerator(cfg *Config, logger *LeveledLogger) *Generator {
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Generator{
		config: cfg,
		logger: logger,
		seen:   &sync.Map{},
		rng:    rand.New(rand.NewSource(seed)),
	}
}

func (g *Generator) pool() string {
	switch g.config.Mode {
	case "letters":
		return "abcdefghijklmnopqrstuvwxyz"
	case "alnum":
		return "abcdefghijklmnopqrstuvwxyz0123456789"
	case "digits":
		return "0123456789"
	case "upper":
		return "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	case "hex":
		return "0123456789abcdef"
	}
	if g.config.Mode != "" {
		return g.config.Mode
	}
	return "abcdefghijklmnopqrstuvwxyz"
}

func (g *Generator) generateUsername() string {
	pool := g.pool()
	if pool == "" {
		pool = "abcdefghijklmnopqrstuvwxyz"
	}
	length := g.config.Length
	prefix := g.config.Prefix
	suffix := g.config.Suffix
	bodyLen := length - len(prefix) - len(suffix)
	if bodyLen < 0 {
		bodyLen = 0
	}
	sb := strings.Builder{}
	sb.Grow(len(prefix) + bodyLen + len(suffix))
	sb.WriteString(prefix)
	for i := 0; i < bodyLen; i++ {
		sb.WriteByte(pool[g.rng.Intn(len(pool))])
	}
	sb.WriteString(suffix)
	return sb.String()
}

func (g *Generator) UniqueUsername() string {
	for {
		name := g.generateUsername()
		if _, loaded := g.seen.LoadOrStore(name, struct{}{}); !loaded {
			return name
		}
	}
}

func (g *Generator) Generate(ctx context.Context, jobs chan<- string, resume *ResumeManager) error {
	defer close(jobs)
	if g.config.CheckFile != "" {
		file, err := os.Open(g.config.CheckFile)
		if err != nil {
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		count := 0
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if resume != nil && resume.IsSeen(line) {
				continue
			}
			if g.config.TotalChecks >= 0 && count >= g.config.TotalChecks {
				return scanner.Err()
			}
			count++
			select {
			case jobs <- line:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return scanner.Err()
	}
	total := g.config.TotalChecks
	for i := 0; total == -1 || i < total; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			name := g.UniqueUsername()
			if resume != nil && resume.IsSeen(name) {
				if total != -1 {
					i--
				}
				continue
			}
			select {
			case jobs <- name:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

type ResumeManager struct {
	file      *os.File
	mu        sync.Mutex
	seen      *sync.Map
	buf       *bufio.Writer
	pending   int
	flushEach int
}

func NewResumeManager(filename string, flushEach int) (*ResumeManager, error) {
	rm := &ResumeManager{
		seen:      &sync.Map{},
		flushEach: flushEach,
	}
	if filename == "" {
		return rm, nil
	}
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	rm.file = f
	rm.buf = bufio.NewWriter(f)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			rm.seen.Store(line, struct{}{})
		}
	}
	return rm, scanner.Err()
}

func (rm *ResumeManager) IsSeen(username string) bool {
	_, ok := rm.seen.Load(username)
	return ok
}

func (rm *ResumeManager) Add(username string) error {
	if rm.file == nil {
		return nil
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if _, err := rm.buf.WriteString(username + "\n"); err != nil {
		return err
	}
	rm.seen.Store(username, struct{}{})
	rm.pending++
	if rm.flushEach > 0 && rm.pending >= rm.flushEach {
		rm.pending = 0
		return rm.buf.Flush()
	}
	return nil
}

func (rm *ResumeManager) Flush() error {
	if rm.buf == nil {
		return nil
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.buf.Flush()
}

func (rm *ResumeManager) Close() error {
	if rm.file == nil {
		return nil
	}
	if err := rm.Flush(); err != nil {
		return err
	}
	return rm.file.Close()
}

type OutputManager struct {
	format       string
	file         *os.File
	allFile      *os.File
	writer       *bufio.Writer
	allWriter    *bufio.Writer
	csvWriter    *csv.Writer
	allCsvWriter *csv.Writer
	jsonEnc      *json.Encoder
	mu           sync.Mutex
	firstJSON    bool
	logger       *LeveledLogger
}

func NewOutputManager(cfg *Config, logger *LeveledLogger) (*OutputManager, error) {
	om := &OutputManager{
		format:    cfg.OutputFormat,
		firstJSON: true,
		logger:    logger,
	}
	if cfg.OutputFile != "" {
		if cfg.CleanOnStart {
			os.Remove(cfg.OutputFile)
		}
		f, err := os.Create(cfg.OutputFile)
		if err != nil {
			return nil, err
		}
		om.file = f
		om.writer = bufio.NewWriter(f)
		switch cfg.OutputFormat {
		case "json":
			om.jsonEnc = json.NewEncoder(om.writer)
			om.jsonEnc.SetEscapeHTML(false)
			om.writer.WriteString("[\n")
		case "csv":
			om.csvWriter = csv.NewWriter(om.writer)
			_ = om.csvWriter.Write([]string{"username", "available", "status_code", "response_ms", "error", "attempts", "proxy", "timestamp"})
			om.csvWriter.Flush()
		}
	}
	if cfg.OutputAllFile != "" {
		if cfg.CleanOnStart {
			os.Remove(cfg.OutputAllFile)
		}
		f, err := os.Create(cfg.OutputAllFile)
		if err != nil {
			return nil, err
		}
		om.allFile = f
		om.allWriter = bufio.NewWriter(f)
		om.allCsvWriter = csv.NewWriter(om.allWriter)
		_ = om.allCsvWriter.Write([]string{"username", "available", "status_code", "response_ms", "error", "attempts", "proxy", "timestamp"})
		om.allCsvWriter.Flush()
	}
	return om, nil
}

func (om *OutputManager) writeMain(r Result) error {
	if om.file == nil {
		return nil
	}
	switch om.format {
	case "plain":
		_, err := om.writer.WriteString(r.Username + "\n")
		return err
	case "json":
		if !om.firstJSON {
			if _, err := om.writer.WriteString(",\n"); err != nil {
				return err
			}
		}
		om.firstJSON = false
		return om.jsonEnc.Encode(r)
	case "csv":
		rec := []string{
			r.Username,
			strconv.FormatBool(r.Available),
			strconv.Itoa(r.StatusCode),
			strconv.FormatInt(r.ResponseTime.Milliseconds(), 10),
			r.Error,
			strconv.Itoa(r.Attempts),
			r.Proxy,
			r.Timestamp.Format(time.RFC3339),
		}
		if err := om.csvWriter.Write(rec); err != nil {
			return err
		}
		om.csvWriter.Flush()
		return om.csvWriter.Error()
	case "ndjson":
		data, err := json.Marshal(r)
		if err != nil {
			return err
		}
		_, err = om.writer.Write(append(data, '\n'))
		return err
	}
	return fmt.Errorf("unknown output format %q", om.format)
}

func (om *OutputManager) WriteResult(r Result, isAvailable bool) error {
	om.mu.Lock()
	defer om.mu.Unlock()
	if om.file != nil && isAvailable && r.Error == "" {
		if err := om.writeMain(r); err != nil {
			return err
		}
		if err := om.writer.Flush(); err != nil {
			return err
		}
	}

	if om.allFile != nil && isAvailable {
		rec := []string{
			r.Username,
			strconv.FormatBool(r.Available),
			strconv.Itoa(r.StatusCode),
			strconv.FormatInt(r.ResponseTime.Milliseconds(), 10),
			r.Error,
			strconv.Itoa(r.Attempts),
			r.Proxy,
			r.Timestamp.Format(time.RFC3339),
		}
		if err := om.allCsvWriter.Write(rec); err != nil {
			return err
		}
		om.allCsvWriter.Flush()
		return om.allCsvWriter.Error()
	}
	return nil
}

func (om *OutputManager) Close() error {
	om.mu.Lock()
	defer om.mu.Unlock()
	if om.file != nil {
		if om.format == "json" {
			if om.firstJSON {
				om.writer.WriteString("]\n")
			} else {
				om.writer.WriteString("\n]\n")
			}
		}
		if om.csvWriter != nil {
			om.csvWriter.Flush()
		}
		if err := om.writer.Flush(); err != nil {
			return err
		}
		if err := om.file.Close(); err != nil {
			return err
		}
	}
	if om.allFile != nil {
		if om.allCsvWriter != nil {
			om.allCsvWriter.Flush()
		}
		if err := om.allWriter.Flush(); err != nil {
			return err
		}
		if err := om.allFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

type Stats struct {
	Checked   int64
	Available int64
	Taken     int64
	Errors    int64
	Bytes     int64
	Start     time.Time
}

func (s *Stats) AddChecked()   { atomic.AddInt64(&s.Checked, 1) }
func (s *Stats) AddAvailable() { atomic.AddInt64(&s.Available, 1) }
func (s *Stats) AddTaken()     { atomic.AddInt64(&s.Taken, 1) }
func (s *Stats) AddError()     { atomic.AddInt64(&s.Errors, 1) }
func (s *Stats) Snapshot() (checked, available, taken, errors int64) {
	return atomic.LoadInt64(&s.Checked),
		atomic.LoadInt64(&s.Available),
		atomic.LoadInt64(&s.Taken),
		atomic.LoadInt64(&s.Errors)
}

func (s *Stats) ETA(total int) time.Duration {
	checked, _, _, _ := s.Snapshot()
	if checked == 0 || total <= 0 {
		return 0
	}
	elapsed := time.Since(s.Start)
	rate := float64(checked) / elapsed.Seconds()
	if rate <= 0 {
		return 0
	}
	remaining := float64(total) - float64(checked)
	if remaining <= 0 {
		return 0
	}
	return time.Duration(remaining / rate * float64(time.Second))
}

const version = "0.1"

func main() {
	cfg := &Config{}
	flag.StringVar(&cfg.BaseURL, "base-url", defaultConfig.BaseURL, "URL template with %s for username")
	flag.StringVar(&cfg.Method, "method", defaultConfig.Method, "HTTP method")
	flag.StringVar(&cfg.RequestBody, "request-body", defaultConfig.RequestBody, "Request body template (with %s)")
	flag.StringVar(&cfg.Mode, "mode", defaultConfig.Mode, "Charset: letters|alnum|digits|upper|hex|custom-string")
	flag.IntVar(&cfg.Length, "length", defaultConfig.Length, "Total username length")
	flag.StringVar(&cfg.Prefix, "prefix", defaultConfig.Prefix, "Username prefix")
	flag.StringVar(&cfg.Suffix, "suffix", defaultConfig.Suffix, "Username suffix")
	flag.Int64Var(&cfg.Seed, "seed", defaultConfig.Seed, "RNG seed (0=time-based)")
	flag.IntVar(&cfg.TotalChecks, "total", defaultConfig.TotalChecks, "Total usernames (-1=infinite)")
	flag.IntVar(&cfg.ThreadCount, "threads", defaultConfig.ThreadCount, "Worker count")
	flag.StringVar(&cfg.OutputFile, "output", defaultConfig.OutputFile, "Available usernames output")
	flag.StringVar(&cfg.OutputAllFile, "output-all", defaultConfig.OutputAllFile, "All results CSV output")
	flag.StringVar(&cfg.OutputFormat, "output-format", defaultConfig.OutputFormat, "plain|json|csv|ndjson")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", defaultConfig.RequestTimeout, "Per-request timeout")
	flag.DurationVar(&cfg.OverallTimeout, "timeout", defaultConfig.OverallTimeout, "Overall run timeout")
	flag.StringVar(&cfg.UAFile, "ua-file", defaultConfig.UAFile, "User agents file")
	flag.DurationVar(&cfg.Delay, "delay", defaultConfig.Delay, "Delay between checks per worker")
	flag.DurationVar(&cfg.DelayJitter, "delay-jitter", defaultConfig.DelayJitter, "Extra random delay")
	flag.IntVar(&cfg.Retries, "retries", defaultConfig.Retries, "Retries per request")
	flag.DurationVar(&cfg.MaxBackoff, "max-backoff", defaultConfig.MaxBackoff, "Max retry backoff")
	flag.StringVar(&cfg.Proxy, "proxy", defaultConfig.Proxy, "Single proxy")
	flag.StringVar(&cfg.ProxyFile, "proxy-file", defaultConfig.ProxyFile, "Proxy list file")
	flag.StringVar(&cfg.ProxyType, "proxy-type", defaultConfig.ProxyType, "http|socks5")
	flag.BoolVar(&cfg.Quiet, "quiet", defaultConfig.Quiet, "Suppress per-result output")
	flag.BoolVar(&cfg.Verbose, "verbose", defaultConfig.Verbose, "Verbose (debug) output")
	flag.StringVar(&cfg.LogLevel, "log-level", defaultConfig.LogLevel, "debug|info|warn|error")
	flag.StringVar(&cfg.LogFile, "log-file", defaultConfig.LogFile, "Log file path")
	flag.StringVar(&cfg.LogFormat, "log-format", defaultConfig.LogFormat, "text|json")
	flag.StringVar(&cfg.CheckFile, "check-file", defaultConfig.CheckFile, "Check usernames from file")
	flag.StringVar(&cfg.ResumeFile, "resume-file", defaultConfig.ResumeFile, "Resume file (appended)")
	flag.IntVar(&cfg.ResumeFlushEvery, "resume-flush", defaultConfig.ResumeFlushEvery, "Flush resume file every N writes")
	flag.DurationVar(&cfg.StatsInterval, "stats-interval", defaultConfig.StatsInterval, "Stats interval (0=off)")
	flag.Float64Var(&cfg.RateLimit, "rate", defaultConfig.RateLimit, "Global rate limit (req/s)")
	flag.IntVar(&cfg.RateLimitBurst, "rate-burst", defaultConfig.RateLimitBurst, "Rate limiter burst")
	flag.Float64Var(&cfg.PerProxyRateLimit, "per-proxy-rate", defaultConfig.PerProxyRateLimit, "Rate limit per proxy (req/s)")
	flag.StringVar(&cfg.HeaderFile, "header-file", defaultConfig.HeaderFile, "Headers file (name: value)")
	flag.StringVar(&cfg.AvailableBody, "available-body", defaultConfig.AvailableBody, "Available body substring")
	flag.StringVar(&cfg.AvailableRegex, "available-regex", defaultConfig.AvailableRegex, "Available body regex")
	flag.BoolVar(&cfg.DryRun, "dry-run", defaultConfig.DryRun, "Dry run (no network)")
	flag.BoolVar(&cfg.TLSInsecure, "tls-insecure", defaultConfig.TLSInsecure, "Skip TLS verification")
	flag.StringVar(&cfg.BasicAuth, "basic-auth", defaultConfig.BasicAuth, "user:pass for basic auth")
	flag.StringVar(&cfg.BearerToken, "bearer-token", defaultConfig.BearerToken, "Bearer token")
	flag.IntVar(&cfg.MaxIdleConns, "max-idle-conns", defaultConfig.MaxIdleConns, "Max idle connections")
	flag.DurationVar(&cfg.IdleConnTimeout, "idle-conn-timeout", defaultConfig.IdleConnTimeout, "Idle connection timeout")
	flag.Int64Var(&cfg.MaxBodyBytes, "max-body-bytes", defaultConfig.MaxBodyBytes, "Max body read size")
	flag.IntVar(&cfg.MaxRedirects, "max-redirects", defaultConfig.MaxRedirects, "Max redirects")
	flag.BoolVar(&cfg.FollowRedirects, "follow-redirects", defaultConfig.FollowRedirects, "Follow redirects")
	flag.Int64Var(&cfg.StopOnAvailable, "stop-on-available", 0, "Stop after N available (0=off)")
	flag.BoolVar(&cfg.Color, "color", defaultConfig.Color, "Colored output")
	flag.BoolVar(&cfg.CleanOnStart, "clean", defaultConfig.CleanOnStart, "Truncate output files on start")
	flag.BoolVar(&cfg.Version, "version", false, "Show version and exit")
	availableStatuses := flag.String("available-status", "404", "Comma-separated available statuses")
	takenStatuses := flag.String("taken-status", "200", "Comma-separated taken statuses")
	retryStatuses := flag.String("retry-status", "429,500,502,503,504", "Comma-separated retry statuses")
	var headers, cookies multiString
	flag.Var(&headers, "header", "Header 'Name: Value' (repeatable)")
	flag.Var(&cookies, "cookie", "Cookie 'name=value' (repeatable)")
	flag.Parse()
	if cfg.Version {
		fmt.Printf("username-checker v%s\n", version)
		return
	}
	if cfg.Verbose {
		cfg.LogLevel = "debug"
	}
	cfg.Headers = headers
	cfg.Cookies = cookies
	if cfg.Length <= 0 {
		log.Fatalf("length must be > 0")
	}
	if cfg.ThreadCount <= 0 {
		log.Fatalf("threads must be > 0")
	}
	if cfg.TotalChecks < -1 {
		log.Fatalf("total must be >= -1")
	}
	if cfg.Retries < 0 {
		cfg.Retries = 0
	}
	if cfg.StatsInterval < 0 {
		cfg.StatsInterval = 0
	}
	av, err := parseStatusList(*availableStatuses)
	if err != nil {
		log.Fatalf("invalid available-status: %v", err)
	}
	cfg.AvailableStatuses = av
	tk, err := parseStatusList(*takenStatuses)
	if err != nil {
		log.Fatalf("invalid taken-status: %v", err)
	}
	cfg.TakenStatuses = tk
	rt, err := parseStatusList(*retryStatuses)
	if err != nil {
		log.Fatalf("invalid retry-status: %v", err)
	}
	cfg.RetryStatuses = rt
	logOutput := io.Writer(os.Stderr)
	if cfg.LogFile != "" {
		f, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open log file: %v", err)
		}
		defer f.Close()
		logOutput = f
	}
	logger := NewLeveledLogger(logOutput, cfg.LogLevel, cfg.LogFormat, cfg.Color)
	logger.Infof("username-checker v%s starting (pid %d, %d workers, mode=%s, len=%d, total=%d)",
		version, os.Getpid(), cfg.ThreadCount, cfg.Mode, cfg.Length, cfg.TotalChecks)
	resume, err := NewResumeManager(cfg.ResumeFile, cfg.ResumeFlushEvery)
	if err != nil {
		logger.Fatalf("resume manager error: %v", err)
	}
	defer resume.Close()
	var checker *Checker
	if !cfg.DryRun {
		checker, err = NewChecker(cfg, logger)
		if err != nil {
			logger.Fatalf("failed to create checker: %v", err)
		}
	}
	gen := NewGenerator(cfg, logger)
	output, err := NewOutputManager(cfg, logger)
	if err != nil {
		logger.Fatalf("output manager error: %v", err)
	}
	defer output.Close()
	ctx, cancel := context.WithCancel(context.Background())
	if cfg.OverallTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.OverallTimeout)
	}
	defer cancel()
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	var sigCount int32
	go func() {
		for range sigCh {
			n := atomic.AddInt32(&sigCount, 1)
			if n == 1 {
				logger.Warn("received interrupt, shutting down gracefully (press again to force)...")
				cancel()
			} else {
				logger.Warn("force exit")
				os.Exit(1)
			}
		}
	}()
	stats := &Stats{Start: time.Now()}
	jobs := make(chan string, cfg.ThreadCount*2)
	results := make(chan Result, cfg.ThreadCount*2)
	var wg sync.WaitGroup
	if !cfg.DryRun {
		for i := 0; i < cfg.ThreadCount; i++ {
			wg.Add(1)
			go worker(ctx, jobs, results, checker, cfg, logger, stats, &wg)
		}
	} else {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(results)
			for {
				select {
				case <-ctx.Done():
					return
				case username, ok := <-jobs:
					if !ok {
						return
					}
					results <- Result{
						Username:  username,
						Available: true,
						Timestamp: time.Now(),
					}
				}
			}
		}()
	}
	go func() {
		if err := gen.Generate(ctx, jobs, resume); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.Errorf("generator error: %v", err)
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	if err := processResults(ctx, cancel, results, output, stats, logger, cfg, resume); err != nil &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.Fatalf("error processing results: %v", err)
	}
	if err := resume.Flush(); err != nil {
		logger.Warnf("resume flush: %v", err)
	}
	logger.Info("done.")
	printFinalStats(stats, logger, cfg.TotalChecks)
}

func worker(ctx context.Context, jobs <-chan string, results chan<- Result,
	checker *Checker, cfg *Config, logger *LeveledLogger, stats *Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rand.Int())))
	for {
		select {
		case <-ctx.Done():
			return
		case username, ok := <-jobs:
			if !ok {
				return
			}
			result, err := checker.Check(ctx, username)
			if err != nil && result.Error == "" {
				result.Error = err.Error()
			}
			stats.AddChecked()
			if result.Available {
				stats.AddAvailable()
				if !cfg.Quiet {
					logger.Available(username)
				}
			} else if result.Error != "" {
				stats.AddError()
				if !cfg.Quiet {
					logger.Warnf("[!] %s: %s", username, result.Error)
				}
			} else {
				stats.AddTaken()
				if !cfg.Quiet {
					logger.Taken(username)
				}
			}
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
			if cfg.Delay > 0 || cfg.DelayJitter > 0 {
				delay := cfg.Delay
				if cfg.DelayJitter > 0 {
					delay += time.Duration(rng.Int63n(int64(cfg.DelayJitter) + 1))
				}
				if delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						return
					}
				}
			} else if cfg.Stealth {
				sd := time.Duration(rng.Int63n(int64(100 * time.Millisecond)))
				if sd > 0 {
					timer := time.NewTimer(sd)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						return
					}
				}
			}
		}
	}
}

func processResults(ctx context.Context, cancel context.CancelFunc, results <-chan Result, output *OutputManager,
	stats *Stats, logger *LeveledLogger, cfg *Config, resume *ResumeManager) error {
	var ticker *time.Ticker
	var tickerCh <-chan time.Time
	if cfg.StatsInterval > 0 {
		ticker = time.NewTicker(cfg.StatsInterval)
		defer ticker.Stop()
		tickerCh = ticker.C
	}
	var availableSoFar int64
	for {
		select {
		case result, ok := <-results:
			if !ok {
				return nil
			}
			isAvail := result.Available && result.Error == ""
			if err := output.WriteResult(result, isAvail); err != nil {
				logger.Errorf("output write error: %v", err)
			}
			if isAvail {
				availableSoFar++
				if cfg.StopOnAvailable > 0 && availableSoFar >= cfg.StopOnAvailable {
					logger.Infof("stop-on-available reached (%d), cancelling", cfg.StopOnAvailable)
					cancel()
					return nil
				}
			}
			if err := resume.Add(result.Username); err != nil {
				logger.Errorf("resume add error: %v", err)
			}
		case <-tickerCh:
			printPeriodicStats(stats, logger, cfg.TotalChecks)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func printPeriodicStats(stats *Stats, logger *LeveledLogger, total int) {
	checked, avail, taken, errs := stats.Snapshot()
	elapsed := time.Since(stats.Start)
	rate := 0.0
	if elapsed.Seconds() > 0 {
		rate = float64(checked) / elapsed.Seconds()
	}
	eta := stats.ETA(total)
	var etaStr string
	if total >= 0 && eta > 0 {
		etaStr = fmt.Sprintf(" | ETA: %s", eta.Round(time.Second))
	}
	logger.Infof("Checked: %d | Avail: %d | Taken: %d | Errors: %d | Rate: %.2f/s%s",
		checked, avail, taken, errs, rate, etaStr)
}

func printFinalStats(stats *Stats, logger *LeveledLogger, total int) {
	checked, avail, taken, errs := stats.Snapshot()
	elapsed := time.Since(stats.Start)
	rate := 0.0
	if elapsed.Seconds() > 0 {
		rate = float64(checked) / elapsed.Seconds()
	}
	logger.Infof("=== FINAL ===")
	logger.Infof("Total checked: %d", checked)
	logger.Infof("Available: %d", avail)
	logger.Infof("Taken: %d", taken)
	logger.Infof("Errors: %d", errs)
	logger.Infof("Elapsed: %s", elapsed.Round(time.Millisecond))
	if checked > 0 {
		logger.Infof("Rate: %.2f checks/s", rate)
	}
}

type multiString []string
func (m *multiString) String() string { return strings.Join(*m, ",") }
func (m *multiString) Set(v string) error {
	*m = append(*m, v)
	return nil
}

var (
	_ = sort.Strings
	_ = runtime.NumCPU
)
