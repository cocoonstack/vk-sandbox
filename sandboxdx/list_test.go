// Copyright 2026 The CocoonStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sandboxdx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
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
		_, _ = w.Write([]byte(`{"sandboxes":[{"id":"sb_1","claim_ref":"ns/w1"},{"id":"sb_2","hibernated":true}]}`))
	}))
	defer srv.Close()

	c := NewListClient(srv.URL, "root-tok", 0)
	got, err := c.ListSandboxes(context.Background())
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
}

func TestListSandboxesAuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := NewListClient(srv.URL, "wrong", 0)
	if _, err := c.ListSandboxes(context.Background()); err == nil {
		t.Fatal("expected error on 401")
	}
}
