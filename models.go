package main

import (
	"errors"
	"net/http"
	"strings"

	"github.com/lib/pq"
	"gorm.io/gorm"
)

type Token struct {
	ID                   int            `gorm:"primaryKey"  json:"id"`
	Secret               string         `gorm:"unique"      json:"secret"`
	AllowedPaths         pq.StringArray `gorm:"type:text[]" json:"allowed_paths"`
	LimiterRatePerMinute int            `                   json:"limiter_rate_per_minute"`
	Note                 string         `                   json:"note"`
}

func (t *Token) Allowed(r *http.Request) bool {
	path := r.URL.Path
	for _, allowed := range t.AllowedPaths {
		if strings.HasPrefix(path, allowed) {
			return true
		}
	}
	return false
}

type TokenValidator interface {
	Validate(tokenString string) (*Token, error)
}

type DBTokenValidator struct {
	db *gorm.DB
}

func NewDBTokenValidator(db *gorm.DB) *DBTokenValidator {
	return &DBTokenValidator{db: db}
}

func (v *DBTokenValidator) Validate(tokenString string) (*Token, error) {
	var token Token
	result := v.db.Where("secret = ?", tokenString).First(&token)
	if result.Error != nil {
		return nil, errors.New("invalid token")
	}
	return &token, nil
}
