package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type contextKey string

const tokenKey contextKey = "token"

func appHandler(w http.ResponseWriter, r *http.Request) {
	token, ok := r.Context().Value(tokenKey).(*Token)
	if !ok || token == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := struct {
		AllowedPaths         pq.StringArray ` json:"allowed_paths"`
		LimiterRatePerMinute int            `                   json:"limiter_rate_per_minute"`
		Note                 string         `                   json:"note"`
	}{
		AllowedPaths:         token.AllowedPaths,
		LimiterRatePerMinute: token.LimiterRatePerMinute,
		Note:                 token.Note,
	}
	respBytes, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	_, err = w.Write(respBytes)
	if err != nil {
		slog.Error("error writing response", "error", err)
	}
	slog.Debug("request processed", "response", fmt.Sprintf("%+v", resp))
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func newRouter(authMiddleware, rateLimitMiddleware func(http.Handler) http.Handler) http.Handler {
	r := chi.NewRouter()

	r.Use(chiMiddleware.Logger)
	r.Use(chiMiddleware.Recoverer)

	// unprotected endpoints
	r.Get("/healthz", healthzHandler)

	// protected endpoints
	r.Group(func(r chi.Router) {
		r.Use(authMiddleware)
		r.Use(rateLimitMiddleware)
		r.Get("/app", appHandler)
	})

	return r
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

	err = db.AutoMigrate(&Token{})
	if err != nil {
		slog.Error("failed to migrate", "error", err)
		os.Exit(1)
	}

	limiterSet := NewMemLimiterSet()
	tokenValidator := NewDBTokenValidator(db)

	authMiddleware := NewAuthMiddleware(tokenValidator)
	rateLimitMiddleware := NewRateLimitMiddleware(limiterSet)
	r := newRouter(authMiddleware, rateLimitMiddleware)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	slog.Info("server starting", "addr", srv.Addr)
	if err = srv.ListenAndServe(); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
