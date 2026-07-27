package provider

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ReasonLeaseExpired marks a pod whose sandbox lease ended and whose microVM
// the reaper destroys at that deadline.
const ReasonLeaseExpired = "SandboxLeaseExpired"

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
	hasClaim = hasClaim && p.settled(key)
	p.mu.RUnlock()
	if pod == nil {
		return nil, nil
	}
	if !hasClaim {
		st := pod.Status.DeepCopy()
		st.Phase = corev1.PodPending
		return st, nil
	}
	if !c.Deadline.IsZero() && time.Now().After(c.Deadline.Time) {
		st := expiredStatus(pod, c)
		return &st, nil
	}
	st := runningStatus(pod, c)
	return &st, nil
}

// expiredStatus reports a pod whose sandbox lease has ended. The reaper destroys
// the VM at the deadline and nothing in the stack renews a lease, so continuing
// to report Running would hide a dead workload indefinitely.
func expiredStatus(pod *corev1.Pod, c Claim) corev1.PodStatus {
	st := corev1.PodStatus{
		Phase:     corev1.PodFailed,
		Reason:    ReasonLeaseExpired,
		Message:   "sandbox lease ended at " + c.Deadline.Format(time.RFC3339) + "; the microVM is reaped at the deadline",
		StartTime: &c.ClaimedAt,
	}
	for _, ctr := range pod.Spec.Containers {
		st.ContainerStatuses = append(st.ContainerStatuses, corev1.ContainerStatus{
			Name: ctr.Name,
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   1,
					Reason:     ReasonLeaseExpired,
					FinishedAt: c.Deadline,
				},
			},
			Image:   ctr.Image,
			ImageID: "sandboxd://" + c.ID,
		})
	}
	return st
}

// runningStatus renders the canonical Running status for a claimed pod: the
// sandbox VM address as the pod IP and one synthetic ready container per spec
// container (the workload runs inside the microVM, not as containers).
func runningStatus(pod *corev1.Pod, c Claim) corev1.PodStatus {
	started := c.ClaimedAt
	if started.IsZero() {
		started = metav1.Now()
	}
	ip := claimIP(c.Address)
	st := corev1.PodStatus{
		Phase:     corev1.PodRunning,
		HostIP:    ip,
		StartTime: &started,
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
				Running: &corev1.ContainerStateRunning{StartedAt: started},
			},
			Image:   ctr.Image,
			ImageID: "sandboxd://" + c.ID,
		})
	}
	return st
}
