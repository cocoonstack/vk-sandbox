package sandboxdx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestInfo parses sandboxd GET /v1/info into per-pool warm capacity, flattening
// the nested pool key and keeping only the warm/target counts inventory needs.
func TestInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/info" || r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer root-tok" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pools":[` +
			`{"key":{"template":"base:24.04","net":"none","size":"small"},"warm":4,"refilling":0,"target":4,"golden":true},` +
			`{"key":{"template":"rt:24.04","net":"egress","size":"medium"},"warm":1,"target":2}],` +
			`"claimed":2,"hibernated":1}`))
	}))
	defer srv.Close()

	c := NewListClient(srv.URL, "root-tok", 0)
	got, err := c.Info(context.Background())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 pools, got %d: %+v", len(got), got)
	}
	if got[0].Template != "base:24.04" || got[0].Net != "none" || got[0].Size != "small" || got[0].Warm != 4 || got[0].Target != 4 {
		t.Fatalf("pool0 wrong: %+v", got[0])
	}
	if got[1].Template != "rt:24.04" || got[1].Net != "egress" || got[1].Size != "medium" || got[1].Warm != 1 || got[1].Target != 2 {
		t.Fatalf("pool1 wrong: %+v", got[1])
	}
}

// TestInfoStatusError treats a non-200 (e.g. a tenant token's 403) as an error
// rather than reporting empty capacity.
func TestInfoStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	c := NewListClient(srv.URL, "tenant-tok", 0)
	if _, err := c.Info(context.Background()); err == nil {
		t.Fatal("expected error on 403")
	}
}
