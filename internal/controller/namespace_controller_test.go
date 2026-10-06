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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	k8upv1 "github.com/k8up-io/k8up/v2/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace    = "namespace-name"
	testScheduleName = "my-schedule"
)

var _ = Describe("Namespace Controller", func() {
	var (
		r        *NamespaceReconciler
		recorder *events.FakeRecorder
	)

	newSchedule := func(name string, mutate func(*k8upv1.Schedule)) *k8upv1.Schedule {
		schedule := &k8upv1.Schedule{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: k8upv1.ScheduleSpec{
				Backend: &k8upv1.Backend{
					RepoPasswordSecretRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "baas-repo-pw"},
						Key:                  "repo-pw",
					},
					S3: &k8upv1.S3Spec{Bucket: "bucket"},
				},
				Backup: &k8upv1.BackupSchedule{},
			},
		}
		if mutate != nil {
			mutate(schedule)
		}
		return schedule
	}

	newNamespace := func(annotations map[string]string) *corev1.Namespace {
		return &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: testNamespace, Annotations: annotations},
		}
	}

	setup := func(objects ...client.Object) {
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme.Scheme).
			WithObjects(objects...).
			Build()
		recorder = events.NewFakeRecorder(64)
		r = &NamespaceReconciler{Client: fakeClient, Scheme: scheme.Scheme, Recorder: recorder}
	}

	listBackups := func() []k8upv1.Backup {
		var backups k8upv1.BackupList
		Expect(r.List(context.Background(), &backups, client.InNamespace(testNamespace))).To(Succeed())
		return backups.Items
	}

	reconcile := func() (ctrl.Result, error) {
		return r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: testNamespace},
		})
	}

	Context("when the Namespace carries the trigger annotation", func() {
		It("creates a Backup from the named Schedule", func() {
			setup(
				newNamespace(map[string]string{backupTriggerAnnotation: testScheduleName}),
				newSchedule(testScheduleName, nil),
			)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			backups := listBackups()
			Expect(backups).To(HaveLen(1))

			backup := backups[0]
			Expect(backup.Name).To(HavePrefix(backupNamePrefix))
			Expect(backup.Namespace).To(Equal(testNamespace))
			Expect(backup.Labels[backupTriggerLabel]).To(Equal(testScheduleName))
			Expect(backup.Spec.Backend.S3.Bucket).To(Equal("bucket"))
			Expect(backup.Spec.Backend.RepoPasswordSecretRef.Name).To(Equal("baas-repo-pw"))
			Expect(backup.Spec.Backend.RepoPasswordSecretRef.Key).To(Equal("repo-pw"))
			Expect(backup.OwnerReferences).To(HaveLen(1))
			Expect(backup.OwnerReferences[0].Kind).To(Equal("Schedule"))
		})

		It("does not create a second Backup on re-reconcile", func() {
			setup(
				newNamespace(map[string]string{backupTriggerAnnotation: testScheduleName}),
				newSchedule(testScheduleName, nil),
			)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(listBackups()).To(HaveLen(1))
		})

		It("applies Schedule-level defaults to the Backup spec", func() {
			setup(
				newNamespace(map[string]string{backupTriggerAnnotation: testScheduleName}),
				newSchedule(testScheduleName, func(s *k8upv1.Schedule) {
					s.Spec.PodSecurityContext = &corev1.PodSecurityContext{RunAsUser: ptr.To[int64](1000)}
					s.Spec.ResourceRequirementsTemplate = corev1.ResourceRequirements{
						Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
					}
					s.Spec.PodConfigRef = &corev1.LocalObjectReference{Name: "podconfig"}
				}),
			)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			backups := listBackups()
			Expect(backups).To(HaveLen(1))
			Expect(backups[0].Spec.PodSecurityContext.RunAsUser).To(HaveValue(Equal(int64(1000))))
			Expect(backups[0].Spec.PodConfigRef.Name).To(Equal("podconfig"))
			Expect(backups[0].Spec.Resources.Limits).To(HaveKey(corev1.ResourceCPU))
		})

		It("falls back to the only Schedule when the annotation has no value", func() {
			setup(
				newNamespace(map[string]string{backupTriggerAnnotation: ""}),
				newSchedule("only-schedule", nil),
			)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			backups := listBackups()
			Expect(backups).To(HaveLen(1))
			Expect(backups[0].Labels[backupTriggerLabel]).To(Equal("only-schedule"))
		})

		It("creates nothing when the named Schedule does not exist", func() {
			setup(newNamespace(map[string]string{backupTriggerAnnotation: "missing"}))

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(listBackups()).To(BeEmpty())
			Expect(<-recorder.Events).To(ContainSubstring("ScheduleNotFound"))
		})

		It("creates nothing when the annotation is empty and multiple Schedules exist", func() {
			setup(
				newNamespace(map[string]string{backupTriggerAnnotation: ""}),
				newSchedule("one", nil),
				newSchedule("two", nil),
			)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(listBackups()).To(BeEmpty())
			Expect(<-recorder.Events).To(ContainSubstring("ScheduleAmbiguous"))
		})
	})

	Context("when the Namespace has no trigger annotation", func() {
		It("creates nothing", func() {
			setup(newNamespace(nil), newSchedule(testScheduleName, nil))

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(listBackups()).To(BeEmpty())
		})
	})

	Context("when the Namespace does not exist", func() {
		It("returns without error", func() {
			setup()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("generated Backup names", func() {
		It("uses the documented prefix and a numeric suffix", func() {
			for range 5 {
				name := generateBackupName()
				Expect(name).To(HavePrefix(backupNamePrefix))
				suffix := strings.TrimPrefix(name, backupNamePrefix)
				Expect(suffix).To(HaveLen(backupRandomDigits))
				for _, c := range suffix {
					Expect(c).To(BeNumerically(">=", '0'))
					Expect(c).To(BeNumerically("<=", '9'))
				}
			}
		})
	})
})
