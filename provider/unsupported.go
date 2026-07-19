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
var ErrNotImplemented = errors.New("vk-cocoon-sandbox: not implemented; use the sandbox SDK for interactive access")

// GetContainerLogs is not served; sandbox output lives inside the microVM.
func (p *Provider) GetContainerLogs(context.Context, string, string, string, api.ContainerLogOpts) (io.ReadCloser, error) {
	return nil, ErrNotImplemented
}

// RunInContainer is not served.
func (p *Provider) RunInContainer(context.Context, string, string, string, []string, api.AttachIO) error {
	return ErrNotImplemented
}

// AttachToContainer is not served.
func (p *Provider) AttachToContainer(context.Context, string, string, string, api.AttachIO) error {
	return ErrNotImplemented
}

// PortForward is not served; sandboxd preview URLs cover guest port access.
func (p *Provider) PortForward(context.Context, string, string, int32, io.ReadWriteCloser) error {
	return ErrNotImplemented
}

// GetStatsSummary reports an empty summary.
func (p *Provider) GetStatsSummary(context.Context) (*statsv1alpha1.Summary, error) {
	return &statsv1alpha1.Summary{}, nil
}

// GetMetricsResource reports no metric families.
func (p *Provider) GetMetricsResource(context.Context) ([]*dto.MetricFamily, error) {
	return nil, nil
}
