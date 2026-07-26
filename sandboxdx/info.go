package sandboxdx

import (
	"context"

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
	var out infoResponse
	if err := c.get(ctx, "/v1/info", "info", &out); err != nil {
		return nil, err
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
