package main

import (
	"net/http"
	"sync"
	"time"
)

type Limiter interface {
	Allow() bool
}

type LimiterSet interface {
	Get(key string) Limiter
}

type MemLimiterSet struct {
	mu         sync.Mutex
	limiters   map[string]Limiter
	newLimiter func(string) Limiter
}

func NewMemLimiterSet(newLimiter func(key string) Limiter) *MemLimiterSet {
	limiters := make(map[string]Limiter)
	return &MemLimiterSet{limiters: limiters, newLimiter: newLimiter}
}

// drawbacks to using map for limiter set: append only, no purging of old limiters

func (m *MemLimiterSet) Get(key string) Limiter {
	m.mu.Lock()
	defer m.mu.Unlock()

	limiter, exists := m.limiters[key]
	if !exists {
		limiter = m.newLimiter(key)
		m.limiters[key] = limiter
	}
	return limiter
}

// --- implementations of limiters

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
				return
			}

			limiter := limiters.Get(userID)
			if !limiter.Allow() {
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
