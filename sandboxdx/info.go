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
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

// infoResponse is the subset of GET /v1/info this client reads: the node's warm
// pool capacity. sandboxd also reports claim counts and mesh peers there, which
// NodeInventory publishing does not need.
type infoResponse struct {
	Pools []infoPool `json:"pools"`
}

type infoPool struct {
	Key    infoPoolKey `json:"key"`
	Warm   int         `json:"warm"`
	Target int         `json:"target"`
}

type infoPoolKey struct {
	Template string `json:"template"`
	Net      string `json:"net"`
	Size     string `json:"size"`
}

// Info reads the node's warm-pool capacity from sandboxd GET /v1/info and maps
// each pool to one PoolCapacity for the node's published NodeInventory, so the
// aggregated apiserver can pick a node that already holds a warm microVM for a
// requested (template, net, size). GET /v1/info is a root-token operator surface
// (tenant tokens get 403), so it reuses this client's node root token.
func (c *ListClient) Info(ctx context.Context) ([]extv1beta1.PoolCapacity, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/info", nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sandboxd info: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sandboxd info: unexpected status %d", resp.StatusCode)
	}
	var out infoResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("sandboxd info: decode: %w", err)
	}
	pools := make([]extv1beta1.PoolCapacity, 0, len(out.Pools))
	for _, p := range out.Pools {
		pools = append(pools, extv1beta1.PoolCapacity{
			Template: p.Key.Template,
			Net:      p.Key.Net,
			Size:     p.Key.Size,
			Warm:     p.Warm,
			Target:   p.Target,
		})
	}
	return pools, nil
}
