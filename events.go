package main

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

var _ record.EventRecorder = warningEvents{}

type warningEvents struct{ record.EventRecorder }

func (w warningEvents) Event(object runtime.Object, eventtype, reason, message string) {
	if eventtype != corev1.EventTypeNormal {
		w.EventRecorder.Event(object, eventtype, reason, message)
	}
}

func (w warningEvents) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...any) {
	if eventtype != corev1.EventTypeNormal {
		w.EventRecorder.Eventf(object, eventtype, reason, messageFmt, args...)
	}
}

func (w warningEvents) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...any) {
	if eventtype != corev1.EventTypeNormal {
		w.EventRecorder.AnnotatedEventf(object, annotations, eventtype, reason, messageFmt, args...)
	}
}
