package sandboxdx

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestListSandboxes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/sandboxes" || r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer root-tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sandboxes":[{"id":"sb_1","claim_ref":"ns/w1","deadline":"2026-08-17T12:00:00.25Z"},{"id":"sb_2","hibernated":true}]}`))
	}))
	defer srv.Close()

	c := NewListClient(srv.URL, "root-tok", 0)
	got, err := c.ListSandboxes(t.Context())
	if err != nil {
		t.Fatalf("ListSandboxes: %v", err)
	}
	if len(got) != 2 || got[0].ID != "sb_1" || !got[1].Hibernated {
		t.Fatalf("unexpected rows: %+v", got)
	}
	// The claim_ref sandboxd echoes must decode so the publisher can name the entry.
	if got[0].ClaimRef != "ns/w1" {
		t.Fatalf("claim_ref not decoded: got %q, want %q", got[0].ClaimRef, "ns/w1")
	}
	// sandboxd writes deadlines with sub-second precision; the row must decode them.
	if want := time.Date(2026, 8, 17, 12, 0, 0, 250000000, time.UTC); !got[0].Deadline.Time.Equal(want) || !got[1].Deadline.IsZero() {
		t.Fatalf("deadline not decoded: got %v / %v, want %v / zero", got[0].Deadline, got[1].Deadline, want)
	}
}

func TestListSandboxesAuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := NewListClient(srv.URL, "wrong", 0)
	if _, err := c.ListSandboxes(t.Context()); err == nil {
		t.Fatal("expected error on 401")
	}
}
