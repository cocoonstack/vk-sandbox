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
	"strings"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// controllerOwnerRef returns the pod's controller owner reference, or nil for
// bare pods.
func controllerOwnerRef(pod *corev1.Pod) *metav1.OwnerReference {
	for i := range pod.OwnerReferences {
		ref := &pod.OwnerReferences[i]
		if ref.Controller != nil && *ref.Controller {
			return ref
		}
	}
	return nil
}

// ownerGVR maps an ownerReference (apiVersion, kind) to the GVR used for the
// destroy-authorization quorum read. Unknown shapes return ok=false and the
// caller preserves.
func ownerGVR(apiVersion, kind string) (schema.GroupVersionResource, bool) {
	if kind == "" {
		return schema.GroupVersionResource{}, false
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupVersionResource{}, false
	}
	return gv.WithResource(pluralResource(strings.ToLower(kind))), true
}

// pluralResource maps a lowercase kind to its REST resource name with the
// English rules that hold for every Kubernetes controller kind
// (sandbox→sandboxes, replicaset→replicasets, policy→policies). Naive +"s"
// destroyed a live-owner VM on 2026-07-17: "Sandbox" became "sandboxs", the
// endpoint 404'd, and the 404 read as "owner deleted". A wrong guess is still
// safe now — destroyAuthorized treats an endpoint-level 404 (no Details.Name)
// as unverifiable (preserve), never owner-gone.
func pluralResource(lower string) string {
	switch {
	case strings.HasSuffix(lower, "s"), strings.HasSuffix(lower, "x"),
		strings.HasSuffix(lower, "z"), strings.HasSuffix(lower, "ch"),
		strings.HasSuffix(lower, "sh"):
		return lower + "es"
	case strings.HasSuffix(lower, "y") && len(lower) > 1 && !strings.ContainsRune("aeiou", rune(lower[len(lower)-2])):
		return lower[:len(lower)-1] + "ies"
	default:
		return lower + "s"
	}
}

// authVerdict is the destroy-authorization decision for one pod deletion.
type authVerdict int

const (
	// authPreserve: pod deletion is NOT authority over the sandbox — keep the
	// claim alive for a same-key replacement pod to adopt (pod churn, eviction
	// storms, provider restart, or an unverifiable owner query).
	authPreserve authVerdict = iota
	// authRelease: the owner CR is confirmed gone or in teardown — releasing
	// the node-local sandbox is authorized.
	authRelease
)

// destroyAuthorized decides whether deleting this pod authorizes releasing its
// node-local sandbox (which destroys the VM). The contract, carried over from
// the vk-cocoon delete-authorization work (2026-07-17):
//
//   - Pod deletion alone is NEVER VM authority. Node-NotReady taint evictions
//     delete every pod on a node while the sandboxes keep serving users.
//   - Release is authorized only when the pod's controller owner CR (the
//     agents.x-k8s.io Sandbox created by sandbox-operator) is confirmed
//     gone — a structured NotFound naming the owner in Details.Name — or is in
//     teardown (non-zero deletionTimestamp).
//   - An endpoint-level 404 without Details.Name means the GVR guess was wrong,
//     not that the owner is gone: preserve.
//   - Any query error preserves: better to leak a warm sandbox than to destroy
//     one the owner still expects.
//   - A bare pod (no controller ownerReference) is its own authority: the pod
//     IS the teardown intent, so its deletion releases.
func destroyAuthorized(ctx context.Context, dyn dynamic.Interface, pod *corev1.Pod) (authVerdict, string) {
	ref := controllerOwnerRef(pod)
	if ref == nil {
		return authRelease, "bare pod: pod deletion is the owner teardown"
	}
	if dyn == nil {
		return authPreserve, "no dynamic client: owner state unverifiable"
	}
	gvr, ok := ownerGVR(ref.APIVersion, ref.Kind)
	if !ok {
		return authPreserve, "owner GVR unresolvable for kind " + ref.Kind
	}
	obj, err := dyn.Resource(gvr).Namespace(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			if n := notFoundName(err); n == ref.Name {
				return authRelease, "owner " + ref.Kind + " " + ref.Name + " confirmed gone (structured NotFound)"
			}
			return authPreserve, "ambiguous 404 for owner " + ref.Kind + " " + ref.Name + " (no Details.Name match): endpoint shape unverified"
		}
		return authPreserve, "owner query failed: " + err.Error()
	}
	if obj.GetDeletionTimestamp() != nil {
		return authRelease, "owner " + ref.Kind + " " + ref.Name + " in teardown (deletionTimestamp set)"
	}
	if string(obj.GetUID()) != string(ref.UID) && ref.UID != "" {
		// Same-name owner with a different UID: the referenced owner generation
		// is gone and something new took its name. The referenced owner no
		// longer exists, so its teardown is complete.
		return authRelease, "owner " + ref.Kind + " " + ref.Name + " UID rotated: referenced generation gone"
	}
	return authPreserve, "owner " + ref.Kind + " " + ref.Name + " alive: pod deletion is not VM authority"
}

// notFoundName extracts the object name a genuine NotFound status carries in
// Details.Name; empty when the error is not a structured NotFound.
func notFoundName(err error) string {
	var se k8serrors.APIStatus
	if errors.As(err, &se) {
		if d := se.Status().Details; d != nil {
			return d.Name
		}
	}
	return ""
}
