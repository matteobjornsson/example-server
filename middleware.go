package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

type Limiter interface {
	Allow() bool
}

type LimiterSet interface {
	GetOrInsert(key string, allowedRate int) (Limiter, error)
}

type MemLimiterSet struct {
	limiters sync.Map
}

func NewMemLimiterSet() *MemLimiterSet {
	return &MemLimiterSet{
		limiters: sync.Map{},
	}
}

func (m *MemLimiterSet) GetOrInsert(key string, rate int) (Limiter, error) {
	newLim := NewSlidingMinuteLimiter(rate)
	// we do not care if loaded, discard bool
	limiter, _ := m.limiters.LoadOrStore(key, newLim)
	l, ok := limiter.(Limiter)
	if !ok {
		return nil, errors.New("object is not a valid limiter")
	}
	return l, nil
}

type SlidingWindowLimiter struct {
	mu      sync.Mutex
	times   []time.Time
	count   int // we "evict" by setting the new insertion index to overwrite outdated entries and track if more requests allowed via count and limit, not by inspecting if slice is full
	limit   int // max requests allowed in the window
	window  time.Duration
	headIdx int // head always points to the oldest valid entry
}

func NewSlidingMinuteLimiter(limit int) *SlidingWindowLimiter {
	return NewSlidingWindowLimiter(limit, time.Minute)
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

func NewRateLimitMiddleware(
	limiters LimiterSet,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			scopedToken, ok := r.Context().Value(tokenKey).(*Token)
			if !ok || scopedToken == nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			limiter, err := limiters.GetOrInsert(
				scopedToken.Secret,
				scopedToken.LimiterRatePerMinute,
			)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if !limiter.Allow() {
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func NewAuthMiddleware(validator TokenValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenString := r.Header.Get("Authorization")
			if tokenString == "" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			token, err := validator.Validate(tokenString)
			if err != nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			if !token.Allowed(r) {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}

			authCtx := context.WithValue(r.Context(), tokenKey, token)
			next.ServeHTTP(w, r.WithContext(authCtx))
		})
	}
}
