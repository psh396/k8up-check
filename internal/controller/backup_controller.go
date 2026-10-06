/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	k8upv1 "github.com/k8up-io/k8up/v2/api/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// backupWatchAnnotation marks a Backup to be watched by this controller.
// Backups carrying this annotation (any value) are reconciled for metrics.
const backupWatchAnnotation = "k8up.check.backup/watch"

// scheduleKind is the Kind of the k8up resource that owns scheduled Backups.
const scheduleKind = "Schedule"

// BackupReconciler reconciles a Backup object
type BackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=k8up.io,resources=backups,verbs=get;list;watch
// +kubebuilder:rbac:groups=k8up.io,resources=backups/status,verbs=get
// +kubebuilder:rbac:groups=k8up.io,resources=schedules,verbs=get;list;watch

// Reconcile records a single Prometheus gauge describing the completion status
// of the observed Backup (1 = succeeded, 0 = failed).
func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var backup k8upv1.Backup
	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			// Backup was deleted, drop its metric series.
			backupCompletionSuccess.DeleteLabelValues(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Only meter Backups created from a namespace trigger annotation or
	// explicitly marked for watching; ignore unrelated Backups such as k8up's
	// own scheduled Backups in the infra namespace.
	if !shouldWatchBackup(&backup) {
		backupCompletionSuccess.DeleteLabelValues(req.Namespace, req.Name)
		return ctrl.Result{}, nil
	}

	// Only report a value once the Backup has reached a terminal state.
	if !backup.Status.HasFinished() {
		return ctrl.Result{}, nil
	}

	value := 0.0
	if backup.Status.HasSucceeded() {
		value = 1.0
	}
	backupCompletionSuccess.WithLabelValues(backup.Namespace, backup.Name).Set(value)
	log.Info("Recorded Backup completion metric",
		"namespace", backup.Namespace, "name", backup.Name, "success", value == 1.0)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager. It watches Backups
// carrying the watch annotation or owned by a Schedule, and also watches
// Schedules to enqueue the Backups they own.
func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&k8upv1.Backup{}, builder.WithPredicates(predicate.NewPredicateFuncs(shouldWatchBackup))).
		Watches(&k8upv1.Schedule{}, handler.EnqueueRequestsFromMapFunc(r.backupsForSchedule)).
		Named("backup").
		Complete(r)
}

// shouldWatchBackup returns true when a Backup should be metered: it either
// carries the explicit watch annotation, or it was created from a namespace
// trigger annotation (identified by the trigger label). k8up's own scheduled
// Backups are intentionally excluded so the metric only reflects the customer
// namespaces that were annotated.
func shouldWatchBackup(obj client.Object) bool {
	if _, ok := obj.GetAnnotations()[backupWatchAnnotation]; ok {
		return true
	}
	_, ok := obj.GetLabels()[backupTriggerLabel]
	return ok
}

// backupsForSchedule enqueues all Backups in the Schedule's namespace that are
// owned by the given Schedule.
func (r *BackupReconciler) backupsForSchedule(ctx context.Context, obj client.Object) []reconcile.Request {
	var backups k8upv1.BackupList
	if err := r.List(ctx, &backups, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for i := range backups.Items {
		b := &backups.Items[i]
		if _, ok := b.Labels[backupTriggerLabel]; !ok {
			continue
		}
		for _, ref := range b.GetOwnerReferences() {
			if ref.Kind == scheduleKind && ref.UID == obj.GetUID() {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{Namespace: b.Namespace, Name: b.Name},
				})
				break
			}
		}
	}
	return requests
}
