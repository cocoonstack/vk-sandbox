package provider

import (
	"context"
	"errors"
	"strings"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

const (
	// authPreserve keeps the claim for a same-key replacement pod to adopt.
	authPreserve authVerdict = iota
	// authRelease permits teardown under the owner or bare-Pod contract.
	authRelease
)

// authVerdict is the destroy-authorization decision for one pod deletion.
type authVerdict int

// ownerGVR derives the owner's GVR from its ownerReference; an unknown shape preserves.
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

// pluralResource applies the es/ies rules; a naive +"s" made Sandbox "sandboxs" and its
// 404 read as owner gone (2026-07-17). A wrong guess now preserves, never releases.
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

// destroyAuthorized releases for a bare pod (nil ref), or when the controller
// owner is confirmed gone by name, in teardown, expired, or replaced under a
// new UID. A 404 without Details.Name and any query error preserve.
func destroyAuthorized(ctx context.Context, dyn dynamic.Interface, namespace string, ref *metav1.OwnerReference) (authVerdict, string) {
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
	obj, err := dyn.Resource(gvr).Namespace(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
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
	if ownerExpired(obj) {
		return authRelease, "owner " + ref.Kind + " " + ref.Name + " expired: the operator tore its workload down"
	}
	if ref.UID != "" && obj.GetUID() != ref.UID {
		return authRelease, "owner " + ref.Kind + " " + ref.Name + " UID rotated: referenced generation gone"
	}
	return authPreserve, "owner " + ref.Kind + " " + ref.Name + " alive: pod deletion is not VM authority"
}

func ownerExpired(obj *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conditions {
		m, ok := c.(map[string]any)
		if ok && m["type"] == string(sandboxv1beta1.SandboxConditionReady) {
			return m["reason"] == sandboxv1beta1.SandboxReasonExpired
		}
	}
	return false
}

// notFoundName is the Details.Name a structured NotFound carries; empty otherwise.
func notFoundName(err error) string {
	var se k8serrors.APIStatus
	if errors.As(err, &se) {
		if d := se.Status().Details; d != nil {
			return d.Name
		}
	}
	return ""
}
