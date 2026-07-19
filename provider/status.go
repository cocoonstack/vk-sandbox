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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetPod serves from the in-memory table — the L0 rule: status reads never
// round-trip the apiserver or sandboxd.
func (p *Provider) GetPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	pod, ok := p.pods[podKey(namespace, name)]
	if !ok {
		return nil, nil
	}
	return pod.DeepCopy(), nil
}

// GetPods returns every tracked pod.
func (p *Provider) GetPods(_ context.Context) ([]*corev1.Pod, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*corev1.Pod, 0, len(p.pods))
	for _, pod := range p.pods {
		out = append(out, pod.DeepCopy())
	}
	return out, nil
}

// GetPodStatus derives status from the claims table (cache-fed, no I/O).
func (p *Provider) GetPodStatus(_ context.Context, namespace, name string) (*corev1.PodStatus, error) {
	key := podKey(namespace, name)
	p.mu.RLock()
	pod := p.pods[key]
	c, hasClaim := p.claims[key]
	p.mu.RUnlock()
	if pod == nil {
		return nil, nil
	}
	if !hasClaim {
		st := pod.Status.DeepCopy()
		st.Phase = corev1.PodPending
		return st, nil
	}
	st := runningStatus(pod, c)
	return &st, nil
}

// runningStatus renders the canonical Running status for a claimed pod: the
// sandbox VM address as the pod IP and one synthetic ready container per spec
// container (the workload runs inside the microVM, not as containers).
func runningStatus(pod *corev1.Pod, c Claim) corev1.PodStatus {
	now := metav1.Now()
	ip := claimIP(c.Address)
	st := corev1.PodStatus{
		Phase:     corev1.PodRunning,
		HostIP:    ip,
		StartTime: &now,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		},
	}
	if ip != "" {
		st.PodIP = ip
		st.PodIPs = []corev1.PodIP{{IP: ip}}
	}
	for _, ctr := range pod.Spec.Containers {
		st.ContainerStatuses = append(st.ContainerStatuses, corev1.ContainerStatus{
			Name:  ctr.Name,
			Ready: true,
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: now},
			},
			Image:   ctr.Image,
			ImageID: "sandboxd://" + c.ID,
		})
	}
	return st
}
