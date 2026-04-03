// Tests Written By LLM
package main

import (
	"net/http"
	"sync"
	"testing"
)

const serverAddr = "http://localhost:8080"
const csvPath = "users.csv"

func loadTestUsers(t *testing.T) []UserRecord {
	t.Helper()
	users, err := LoadUsersFromCSV(csvPath)
	if err != nil {
		t.Fatalf("failed to load users csv: %v", err)
	}
	return users
}

func makeRequest(token string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, serverAddr+"/app", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", token)
	return http.DefaultClient.Do(req)
}

func userByID(users []UserRecord, id string) (UserRecord, bool) {
	for _, u := range users {
		if u.UserID == id {
			return u, true
		}
	}
	return UserRecord{}, false
}

// TestAuthFailure verifies that an invalid token is rejected.
func TestAuthFailure(t *testing.T) {
	resp, err := makeRequest("not-a-real-token")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

// TestRateLimitBreached drains free_user1's bucket and expects a 429.
// Also implicitly tests that valid tokens are accepted (200s during drain).
func TestRateLimitBreached(t *testing.T) {
	users := loadTestUsers(t)
	user, ok := userByID(users, "1")
	if !ok {
		t.Fatal("user 1 not found in csv")
	}

	for i := 0; i < FreeTierLimit; i++ {
		resp, err := makeRequest(user.Token)
		if err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 on request %d, got %d", i+1, resp.StatusCode)
		}
	}

	resp, err := makeRequest(user.Token)
	if err != nil {
		t.Fatalf("over-limit request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected 429 after limit, got %d", resp.StatusCode)
	}
}

// TestRateLimitIsolation exhausts free_user1 and verifies free_user2 is unaffected.
func TestRateLimitIsolation(t *testing.T) {
	users := loadTestUsers(t)

	user1, ok := userByID(users, "3")
	if !ok {
		t.Fatal("user 3 not found in csv")
	}
	user2, ok := userByID(users, "4")
	if !ok {
		t.Fatal("user 4 not found in csv")
	}

	for i := 0; i < ProTierLimit+1; i++ {
		resp, _ := makeRequest(user1.Token)
		if resp != nil {
			resp.Body.Close()
		}
	}

	resp, err := makeRequest(user1.Token)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected 429 after limit, got %d", resp.StatusCode)
	}

	resp, err = makeRequest(user2.Token)
	if err != nil {
		t.Fatalf("user2 request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("free_user2 should not be rate limited; got %d", resp.StatusCode)
	}
}

// TestConcurrentRequestsWithinLimit fires concurrent requests for enterprise_user1,
// all within the tier limit, verifying thread safety and burst handling.
func TestConcurrentRequestsWithinLimit(t *testing.T) {
	users := loadTestUsers(t)
	user, ok := userByID(users, "5")
	if !ok {
		t.Fatal("user 5 not found in csv")
	}

	concurrency := 20 // well within EnterpriseTierLimit (100)
	var wg sync.WaitGroup
	results := make([]int, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := makeRequest(user.Token)
			if err != nil {
				results[idx] = -1
				return
			}
			defer resp.Body.Close()
			results[idx] = resp.StatusCode
		}(i)
	}
	wg.Wait()

	for i, code := range results {
		if code != http.StatusOK {
			t.Errorf("goroutine %d: expected 200, got %d", i, code)
		}
	}
}
