// Package sandboxdx extends the sandbox-operator sandboxd client with the
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

	"github.com/cocoonstack/vk-sandbox/provider"
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
	var out listResponse
	if err := c.get(ctx, "/v1/sandboxes", "list", &out); err != nil {
		return nil, err
	}
	return out.Sandboxes, nil
}

// get performs an authenticated GET against a sandboxd operator surface and
// decodes the JSON body into v. The body is always drained so the pooled
// connection is reusable.
func (c *ListClient) get(ctx context.Context, path, op string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("sandboxd %s: %w", op, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sandboxd %s: unexpected status %d", op, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return fmt.Errorf("sandboxd %s: decode: %w", op, err)
	}
	return nil
}
