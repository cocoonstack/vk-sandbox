package provider

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// ReasonLeaseExpired marks a pod whose sandbox lease ended and whose microVM
// the reaper destroys at that deadline.
const ReasonLeaseExpired = "SandboxLeaseExpired"

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
	st.ContainerStatuses = containerStatuses(pod, c, corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			ExitCode:   1,
			Reason:     ReasonLeaseExpired,
			FinishedAt: c.Deadline,
		},
	}, false)
	return st
}

// runningStatus renders the canonical Running status for a claimed pod: the
// sandbox VM address as the pod IP and one synthetic ready container per spec
// container (the workload runs inside the microVM, not as containers).
func runningStatus(pod *corev1.Pod, c Claim) corev1.PodStatus {
	ip := claimIP(c.Address)
	st := corev1.PodStatus{
		Phase:     corev1.PodRunning,
		HostIP:    ip,
		StartTime: &c.ClaimedAt,
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
	st.ContainerStatuses = containerStatuses(pod, c, corev1.ContainerState{
		Running: &corev1.ContainerStateRunning{StartedAt: c.ClaimedAt},
	}, true)
	return st
}

func containerStatuses(pod *corev1.Pod, c Claim, state corev1.ContainerState, ready bool) []corev1.ContainerStatus {
	var out []corev1.ContainerStatus
	for _, ctr := range pod.Spec.Containers {
		out = append(out, corev1.ContainerStatus{
			Name:    ctr.Name,
			Ready:   ready,
			State:   state,
			Image:   ctr.Image,
			ImageID: "sandboxd://" + c.ID,
		})
	}
	return out
}
