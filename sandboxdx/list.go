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

// Package sandboxdx extends the cocoon-sandbox-operator sandboxd client with the
// root-token operator surfaces this provider needs: the live index (GET
// /v1/sandboxes) for status and the audit-only orphan scan, and warm-pool
// capacity (GET /v1/info) for NodeInventory publishing. Claim and Release stay on
// the operator's client so the wire contract has exactly one home.
package sandboxdx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cocoonstack/vk-cocoon-sandbox/provider"
)

// ListClient reads the sandboxd operator index.
type ListClient struct {
	base   string
	token  string
	client *http.Client
}

// NewListClient builds a ListClient for base (e.g. "http://127.0.0.1:7777")
// authenticating with the node root token.
func NewListClient(base, token string, timeout time.Duration) *ListClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &ListClient{base: base, token: token, client: &http.Client{Timeout: timeout}}
}

type listResponse struct {
	Sandboxes []provider.ListedSandbox `json:"sandboxes"`
}

// ListSandboxes returns the node's live sandboxes. Implements provider.Lister.
func (c *ListClient) ListSandboxes(ctx context.Context) ([]provider.ListedSandbox, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/sandboxes", nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sandboxd list: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sandboxd list: unexpected status %d", resp.StatusCode)
	}
	var out listResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("sandboxd list: decode: %w", err)
	}
	return out.Sandboxes, nil
}
