package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

type contextKey string

const userIDKey contextKey = "userID"

type Limiter interface {
	Allow() bool
}

type LimiterSet interface {
	Get(key string) Limiter
}

type MemLimiterSet struct {
	mu         sync.Mutex
	limiters   map[string]Limiter
	newLimiter func() Limiter
}

func NewMemLimiterSet(newLimiter func() Limiter) *MemLimiterSet {
	limiters := make(map[string]Limiter)
	return &MemLimiterSet{limiters: limiters, newLimiter: newLimiter}
}

// drawbacks to using map for limiter set: append only, no purging of old limiters

func (m *MemLimiterSet) Get(key string) Limiter {
	m.mu.Lock()
	defer m.mu.Unlock()

	limiter, exists := m.limiters[key]
	if !exists {
		limiter = m.newLimiter()
		m.limiters[key] = limiter
	}
	return limiter
}

type FixedWindowLimiter struct {
	mu        sync.Mutex
	count     int
	limit     int
	windowEnd time.Time
	duration  time.Duration
}

func NewFixedWindowLimiter(limit int, duration time.Duration) *FixedWindowLimiter {
	return &FixedWindowLimiter{
		count:    0,
		limit:    limit,
		duration: duration,
		windowEnd: time.Now().
			Add(duration),
		// initialize window end to now + duration
	}
}

func (l *FixedWindowLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// each call to the limiter we check if the window needs resetting before
	// incrementing count
	if time.Now().After(l.windowEnd) {
		l.count = 0
		l.windowEnd = time.Now().Add(l.duration)
	}
	if l.count >= l.limit {
		return false
	}
	l.count++
	return true
}

type SlidingWindowLimiter struct {
	mu      sync.Mutex
	times   []time.Time
	count   int // we "evict" by setting the new insertion index to overwrite outdated entries and track if more requests allowed via count and limit, not by inspecting if slice is full
	limit   int // max requests allowed in the window
	window  time.Duration
	headIdx int // head always points to the oldest valid entry
}

func NewSlidingWindowLimiter(
	limit int,
	duration time.Duration,
) *SlidingWindowLimiter {
	times := make([]time.Time, limit)
	return &SlidingWindowLimiter{
		times:   times,
		count:   0,
		limit:   limit,
		window:  duration,
		headIdx: 0, // ring buffer head
	}
}

func (l *SlidingWindowLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-l.window)

	// step 1: trim window of old entries. If evictions occur, update headIdx.
	for l.count > 0 {
		oldest := l.times[l.headIdx]
		if oldest.After(cutoff) {
			// if the oldest entry we know about is a timestamp younger than cutoff, no further evictions required
			break
		}
		// if we have reached here, we need to evict the oldest entry and update headIdx and count
		l.headIdx = (l.headIdx + 1) % l.limit
		l.count-- // we do not overwrite buffer with zero values, but rather update the view of the buffer (count and head)
	}

	// step 2: check if we have capacity remaining in the trimmed window
	if l.count >= l.limit {
		// window has already reached capacity, reject
		return false
	}

	// if we have capacity, add at appropriate index and increment count
	insertionIdx := (l.headIdx + l.count) % l.limit // wrap around the ring buffer, determined by head and count
	l.times[insertionIdx] = now
	l.count++

	return true
}

// token bucket adds tokens at rate count/duration and allows requests to consume tokens, if no tokens available, reject request.

type TokenBucketLimiter struct {
	mu       sync.Mutex
	limit    float64 // max tokens in bucket
	tokens   float64 // number of tokens available to requests
	refill   int     // refill/duration is the rate at which tokens are added to the bucket
	duration time.Duration
	lastFill time.Time
}

func NewTokenBucketLimiter(
	limit int,
	refill int,
	duration time.Duration,
) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		limit:    float64(limit),
		tokens:   float64(limit), // start with a full bucket
		refill:   refill,
		duration: duration,
		lastFill: time.Now(),
	}
}

func (l *TokenBucketLimiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsedSeconds := now.Sub(l.lastFill).Seconds()

	refillPerSecond := float64(l.refill) / l.duration.Seconds()
	// add accumulated tokens (rate*elapsed), cap at limit.
	l.tokens = min(l.limit, l.tokens+elapsedSeconds*refillPerSecond)
	l.lastFill = now

	if l.tokens < 1 {
		// no token for u
		return false
	}
	// consume a token from the bucket
	l.tokens -= 1
	return true
}

func NewRateLimitMiddleware(
	limiters LimiterSet,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID, ok := r.Context().Value(userIDKey).(string)
			if !ok || userID == "" {
				w.WriteHeader(http.StatusInternalServerError)
				slog.Error("error getting user id from context")
				return
			}

			limiter := limiters.Get(userID)
			if !limiter.Allow() {
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				slog.Info("rate limit exceeded", "userID", userID)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type Claims struct {
	UserID string `json:"user_id"`
}

type JWTValidator interface {
	Validate(tokenString string) (claims Claims, err error)
}

func NewAuthMiddleware(validator JWTValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenString := r.Header.Get("Authorization")
			if tokenString == "" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				slog.Error("missing auth token")
				return
			}

			claims, err := validator.Validate(tokenString)
			if err != nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				slog.Info("validating token", "error", err)
				return
			}

			slog.Debug("token validated", "claims", claims)

			authCtx := context.WithValue(r.Context(), userIDKey, claims.UserID)
			next.ServeHTTP(w, r.WithContext(authCtx))
		})
	}
}

func NewMiddleware(
	validator JWTValidator,
	multiLimiter LimiterSet,
) func(http.Handler) http.Handler {
	authMiddleware := NewAuthMiddleware(validator)
	rateLimitMiddleware := NewRateLimitMiddleware(multiLimiter)

	return func(next http.Handler) http.Handler {
		// wrap outside->in for call order
		return authMiddleware(rateLimitMiddleware(next))
	}
}

func appHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := r.Context().Value(userIDKey).(string)
	if !ok || userID == "" {
		w.WriteHeader(http.StatusInternalServerError)
		slog.Error("error getting user id from context")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, err := w.Write([]byte(`{"userId": "` + userID + `"}`))
	if err != nil {
		slog.Error("error writing response", "error", err)
	}
	slog.Debug("request processed", "userId", userID)
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func routes(middleware func(next http.Handler) http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /app", middleware(http.HandlerFunc(appHandler)).ServeHTTP)
	// no middleware for healthz
	mux.HandleFunc("GET /healthz", healthzHandler)
	return mux
}

const user1Token = "token1"
const user1Id = "user1"

const user2Token = "token2"
const user2Id = "user2"

type FakeValidator struct{}

func (v FakeValidator) Validate(tokenString string) (Claims, error) {
	switch tokenString {
	case user1Token:
		return Claims{UserID: user1Id}, nil
	case user2Token:
		return Claims{UserID: user2Id}, nil
	default:
		return Claims{}, errors.New("invalid Token")
	}
}

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	// establish any configuration
	// construct dependencies
	// start services if necessary
	// start server

	// configure the settings for a token bucket limiter
	newTokenBucketLimiter := func() Limiter {
		limit := 5
		refill := 10
		duration := 1 * time.Minute
		return NewTokenBucketLimiter(limit, refill, duration)
	}
	limiterSet := NewMemLimiterSet(newTokenBucketLimiter)

	middleware := NewMiddleware(&FakeValidator{}, limiterSet)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: routes(middleware),
	}

	if err := srv.ListenAndServe(); err != nil {
		fmt.Println(fmt.Sprintf("Server error: %v", err))
		os.Exit(1)
	}
}
