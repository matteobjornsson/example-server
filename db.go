package main

import (
	"errors"
	"net/http"
	"strings"

	"github.com/lib/pq"
	"gorm.io/gorm"
)

type ScopedToken struct {
	ID                   int            `gorm:"primaryKey" json:"id"`
	Secret               string         `json:"secret"`
	AllowedPaths         pq.StringArray `gorm:"type:text[]" json:"allowed_paths"`
	LimiterRatePerSecond int            `json:"limiter_rate_per_second"`
	Note                 string         `json:"note"`
}

func (t *ScopedToken) Allowed(r *http.Request) bool {
	path := r.URL.Path
	for _, allowed := range t.AllowedPaths {
		if strings.HasPrefix(path, allowed) {
			return true
		}
	}
	return false
}

type TokenValidator interface {
	Validate(tokenString string) (*ScopedToken, error)
}

type DBTokenValidator struct {
	db *gorm.DB
}

func NewDBTokenValidator(db *gorm.DB) *DBTokenValidator {
	return &DBTokenValidator{db: db}
}

func (v *DBTokenValidator) Validate(tokenString string) (*ScopedToken, error) {
	var token ScopedToken
	result := v.db.Where("secret = ?", tokenString).First(&token)
	if result.Error != nil {
		return nil, errors.New("invalid token")
	}
	return &token, nil
}
