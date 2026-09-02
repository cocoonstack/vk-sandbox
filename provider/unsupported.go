package provider

import (
	"context"
	"errors"
	"io"

	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	statsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// ErrNotImplemented marks kubelet surfaces this provider does not serve yet.
// Interactive access to a sandbox goes through the sandbox SDK / preview URLs
// (see the cocoonstack/sandbox docs), not the kubelet exec path.
var ErrNotImplemented = errors.New("vk-sandbox: not implemented; use the sandbox SDK for interactive access")

func (p *Provider) GetContainerLogs(context.Context, string, string, string, api.ContainerLogOpts) (io.ReadCloser, error) {
	return nil, ErrNotImplemented
}

func (p *Provider) RunInContainer(context.Context, string, string, string, []string, api.AttachIO) error {
	return ErrNotImplemented
}

func (p *Provider) AttachToContainer(context.Context, string, string, string, api.AttachIO) error {
	return ErrNotImplemented
}

func (p *Provider) PortForward(context.Context, string, string, int32, io.ReadWriteCloser) error {
	return ErrNotImplemented
}

func (p *Provider) GetStatsSummary(context.Context) (*statsv1alpha1.Summary, error) {
	return &statsv1alpha1.Summary{}, nil
}

func (p *Provider) GetMetricsResource(context.Context) ([]*dto.MetricFamily, error) {
	return nil, nil
}
