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
	"fmt"
	"math/rand/v2"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	k8upv1 "github.com/k8up-io/k8up/v2/api/v1"
)

// backupTriggerAnnotation marks a Namespace for an ad-hoc Backup. The
// annotation's value optionally names the k8up Schedule that supplies the
// Backup spec (backend, selectors, ...). An empty value picks the only
// Schedule in the Namespace.
const backupTriggerAnnotation = "k8up.check.backup/trigger"

// backupTriggerLabel is set on every Backup created from the trigger
// annotation. Its presence (and value) makes reconciliation idempotent.
const backupTriggerLabel = "k8up.check.backup/trigger"

const (
	backupNamePrefix   = "k8up-lagoon-backup-schedule-backup-"
	backupRandomDigits = 6
	maxNameAttempts    = 5
)

// NamespaceReconciler creates an ad-hoc k8up Backup in every Namespace that
// carries the trigger annotation, copying the spec from the Namespace's
// k8up Schedule.
type NamespaceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder recorder.EventRecorder
}

// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=k8up.io,resources=backups,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=k8up.io,resources=schedules,verbs=get;list;watch

// Reconcile creates a Backup named "k8up-lagoon-backup-schedule-backup-<n>"
// in the annotated Namespace, unless a Backup for that Schedule was already
// created.
func (r *NamespaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var ns corev1.Namespace
	if err := r.Get(ctx, req.NamespacedName, &ns); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	scheduleName, triggered := ns.GetAnnotations()[backupTriggerAnnotation]
	if !triggered {
		return ctrl.Result{}, nil
	}

	schedule, err := r.resolveSchedule(ctx, &ns, scheduleName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if schedule == nil {
		return ctrl.Result{}, nil
	}

	exists, err := r.backupExists(ctx, ns.Name, schedule.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if exists {
		log.Info("Backup already created for trigger, skipping",
			"namespace", ns.Name, "schedule", schedule.Name)
		return ctrl.Result{}, nil
	}

	backup, err := r.createBackup(ctx, schedule)
	if err != nil {
		return ctrl.Result{}, err
	}

	log.Info("Created Backup", "namespace", backup.Namespace, "name", backup.Name,
		"schedule", schedule.Name)
	r.Recorder.Eventf(&ns, nil, corev1.EventTypeNormal, "BackupCreated", "Create",
		"Created Backup %s from Schedule %s", backup.Name, schedule.Name)

	return ctrl.Result{}, nil
}

// resolveSchedule returns the Schedule the Backup spec is taken from. A
// non-empty trigger value selects the Schedule by name; an empty value
// requires exactly one Schedule in the Namespace. Missing or ambiguous
// Schedules are reported as warning events and yield nil.
func (r *NamespaceReconciler) resolveSchedule(ctx context.Context, ns *corev1.Namespace, name string) (*k8upv1.Schedule, error) {
	log := logf.FromContext(ctx)

	if name != "" {
		var schedule k8upv1.Schedule
		err := r.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: name}, &schedule)
		if apierrors.IsNotFound(err) {
			log.Info("Triggered Schedule not found", "namespace", ns.Name, "schedule", name)
			r.Recorder.Eventf(ns, nil, corev1.EventTypeWarning, "ScheduleNotFound", "Get",
				"Schedule %q not found in namespace %s", name, ns.Name)
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &schedule, nil
	}

	var schedules k8upv1.ScheduleList
	if err := r.List(ctx, &schedules, client.InNamespace(ns.Name)); err != nil {
		return nil, err
	}
	switch len(schedules.Items) {
	case 0:
		log.Info("Trigger annotation set but no Schedule found", "namespace", ns.Name)
		r.Recorder.Eventf(ns, nil, corev1.EventTypeWarning, "ScheduleNotFound", "Get",
			"No Schedule found in namespace to create the Backup from")
		return nil, nil
	case 1:
		return &schedules.Items[0], nil
	default:
		log.Info("Trigger annotation set but multiple Schedules found", "namespace", ns.Name)
		r.Recorder.Eventf(ns, nil, corev1.EventTypeWarning, "ScheduleAmbiguous", "Get",
			"Found %d schedules in namespace %s, set the %s annotation to a single Schedule name",
			len(schedules.Items), ns.Name, backupTriggerAnnotation)
		return nil, nil
	}
}

// backupExists reports whether a Backup for the given Schedule was already
// created by this controller. To trigger another Backup, delete the existing
// one and touch the trigger annotation again.
func (r *NamespaceReconciler) backupExists(ctx context.Context, namespace, scheduleName string) (bool, error) {
	var backups k8upv1.BackupList
	if err := r.List(ctx, &backups,
		client.InNamespace(namespace),
		client.MatchingLabels{backupTriggerLabel: scheduleName},
	); err != nil {
		return false, err
	}
	return len(backups.Items) > 0, nil
}

// createBackup builds the Backup from the Schedule and creates it, retrying
// with a new random name on collisions.
func (r *NamespaceReconciler) createBackup(ctx context.Context, schedule *k8upv1.Schedule) (*k8upv1.Backup, error) {
	log := logf.FromContext(ctx)

	for range maxNameAttempts {
		backup, err := buildBackup(r.Scheme, schedule, generateBackupName())
		if err != nil {
			return nil, err
		}
		if backup.Spec.Backend == nil {
			log.Info("Schedule provides no backend, the Backup will fail without one",
				"namespace", schedule.Namespace, "schedule", schedule.Name)
		}

		if err := r.Create(ctx, backup); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return nil, err
		}
		return backup, nil
	}
	return nil, fmt.Errorf("could not generate a unique name for the Backup after %d attempts", maxNameAttempts)
}

// generateBackupName returns "k8up-lagoon-backup-schedule-backup-<number>".
func generateBackupName() string {
	return fmt.Sprintf("%s%0*d", backupNamePrefix, backupRandomDigits, rand.IntN(1000000))
}

// buildBackup renders the Backup template from the Schedule: spec comes from
// the Schedule's backup section, with the Schedule-level backend, resources,
// pod security context and pod config applied as defaults.
func buildBackup(scheme *runtime.Scheme, schedule *k8upv1.Schedule, name string) (*k8upv1.Backup, error) {
	backup := &k8upv1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: schedule.Namespace,
			Labels: map[string]string{
				backupTriggerLabel:           schedule.Name,
				k8upv1.LabelK8upScheduleName: schedule.Name,
			},
		},
	}
	if schedule.Spec.Backup != nil {
		backup.Spec = *schedule.Spec.Backup.BackupSpec.DeepCopy()
	}
	applyScheduleDefaults(&backup.Spec, schedule)

	if err := controllerutil.SetControllerReference(schedule, backup, scheme); err != nil {
		return nil, err
	}
	return backup, nil
}

// applyScheduleDefaults mirrors k8up's own schedule handling: fields unset on
// the Backup section fall back to the Schedule-level values.
func applyScheduleDefaults(spec *k8upv1.BackupSpec, schedule *k8upv1.Schedule) {
	if spec.Backend == nil {
		spec.Backend = schedule.Spec.Backend.DeepCopy()
	}
	if spec.PodSecurityContext == nil {
		spec.PodSecurityContext = schedule.Spec.PodSecurityContext.DeepCopy()
	}
	if spec.PodConfigRef == nil && schedule.Spec.PodConfigRef != nil {
		podConfigRef := *schedule.Spec.PodConfigRef
		spec.PodConfigRef = &podConfigRef
	}
	if len(spec.Resources.Requests) == 0 && len(spec.Resources.Limits) == 0 {
		spec.Resources = *schedule.Spec.ResourceRequirementsTemplate.DeepCopy()
	}
}

// hasTriggerAnnotation reports whether the object carries the trigger
// annotation, whatever its value.
func hasTriggerAnnotation(obj client.Object) bool {
	_, ok := obj.GetAnnotations()[backupTriggerAnnotation]
	return ok
}

// namespacesForSchedule enqueues the Schedule's Namespace when it carries the
// trigger annotation, so a Schedule created (or changed) after the annotation
// still triggers the Backup.
func (r *NamespaceReconciler) namespacesForSchedule(ctx context.Context, obj client.Object) []reconcile.Request {
	var ns corev1.Namespace
	if err := r.Get(ctx, types.NamespacedName{Name: obj.GetNamespace()}, &ns); err != nil {
		return nil
	}
	if !hasTriggerAnnotation(&ns) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: ns.Name}}}
}

// SetupWithManager sets up the controller with the Manager. It watches
// annotated Namespaces and Schedules to re-enqueue their Namespace.
func (r *NamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("namespace")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}, builder.WithPredicates(predicate.NewPredicateFuncs(hasTriggerAnnotation))).
		Watches(&k8upv1.Schedule{}, handler.EnqueueRequestsFromMapFunc(r.namespacesForSchedule)).
		Named("namespace").
		Complete(r)
}
