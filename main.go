package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type contextKey string

const userIDKey contextKey = "userID"

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

			authCtx := context.WithValue(r.Context(), userIDKey, strconv.Itoa(token.ID))
			next.ServeHTTP(w, r.WithContext(authCtx))
		})
	}
}

func NewMiddleware(
	validator TokenValidator,
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

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		slog.Error("POSTGRES_DSN not set")
		os.Exit(1)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		slog.Error("failed to connect to db", "error", err)
		os.Exit(1)
	}

	err = db.AutoMigrate(&ScopedToken{})
	if err != nil {
		slog.Error("failed to migrate", "error", err)
		os.Exit(1)
	}

	newTokenBucketLimiterByTokenID := func(userID string) Limiter {
		// ai slop... will get back to this
		id, _ := strconv.Atoi(userID)
		var token ScopedToken
		result := db.First(&token, id)
		if result.Error != nil {
			return NewTokenBucketLimiter(1, 1, 1*time.Second)
		}
		rate := token.LimiterRatePerSecond
		return NewTokenBucketLimiter(rate, rate, 1*time.Second)
	}

	limiterSet := NewMemLimiterSet(newTokenBucketLimiterByTokenID)
	tokenValidator := NewDBTokenValidator(db)

	middleware := NewMiddleware(tokenValidator, limiterSet)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: routes(middleware),
	}

	slog.Info("server starting", "addr", srv.Addr)
	if err = srv.ListenAndServe(); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
