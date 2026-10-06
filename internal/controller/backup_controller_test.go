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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	k8upv1 "github.com/k8up-io/k8up/v2/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("Backup Controller", func() {
	var backup *k8upv1.Backup
	var registry *prometheus.Registry

	BeforeEach(func() {
		backupCompletionSuccess.Reset()
		registry = prometheus.NewRegistry()
		registry.MustRegister(backupCompletionSuccess)
		backup = &k8upv1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "k8up-lagoon-backup-schedule-backup-507353",
				Namespace: "test6-drupal-example-simple-test9",
				Labels: map[string]string{
					backupTriggerLabel: "k8up-lagoon-backup-schedule",
				},
			},
		}
	})

	AfterEach(func() {
		backupCompletionSuccess.Reset()
	})

	metricCount := func() int {
		families, err := registry.Gather()
		Expect(err).NotTo(HaveOccurred())
		return len(families)
	}

	reconcile := func() {
		r := &BackupReconciler{
			Client: fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(backup).Build(),
			Scheme: scheme.Scheme,
		}
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: backup.Namespace, Name: backup.Name},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	It("does not report completion while a triggered Backup waits for PreBackupPods", func() {
		backup.Status.Conditions = []metav1.Condition{{
			Type: "PreBackupPodReady", Status: metav1.ConditionUnknown, Reason: "Waiting",
			Message: "waiting for 1 PreBackupPods to become ready",
		}}

		Expect(shouldWatchBackup(backup)).To(BeTrue())
		reconcile()

		Expect(metricCount()).To(Equal(0))
	})

	DescribeTable("reports terminal results in the triggered Backup's namespace",
		func(reason string, value float64) {
			backup.Status.Conditions = []metav1.Condition{{
				Type: k8upv1.ConditionCompleted.String(), Status: metav1.ConditionTrue, Reason: reason,
			}}

			reconcile()

			families, err := registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			Expect(families).To(HaveLen(1))
			Expect(families[0].Metric).To(HaveLen(1))
			Expect(families[0].Metric[0].GetGauge().GetValue()).To(Equal(value))
			labels := map[string]string{}
			for _, label := range families[0].Metric[0].Label {
				labels[label.GetName()] = label.GetValue()
			}
			Expect(labels).To(Equal(map[string]string{"backup_namespace": backup.Namespace, "name": backup.Name}))
		},
		Entry("success", "Succeeded", 1.0),
		Entry("failure", "Failed", 0.0),
	)

	It("ignores unrelated scheduled Backups in the infra namespace", func() {
		backup.Namespace = "infra-cluster-backup"
		backup.Labels = map[string]string{k8upv1.LabelK8upScheduleName: "objects"}
		backup.Status.Conditions = []metav1.Condition{{
			Type: k8upv1.ConditionCompleted.String(), Status: metav1.ConditionTrue, Reason: "Succeeded",
		}}
		backupCompletionSuccess.WithLabelValues(backup.Namespace, backup.Name).Set(1)

		reconcile()

		Expect(metricCount()).To(Equal(0))
	})

	It("reports explicitly watched Backups without a trigger label", func() {
		backup.Labels = nil
		backup.Annotations = map[string]string{backupWatchAnnotation: ""}
		backup.Status.Conditions = []metav1.Condition{{
			Type: k8upv1.ConditionCompleted.String(), Status: metav1.ConditionTrue, Reason: "Succeeded",
		}}

		reconcile()

		Expect(metricCount()).To(Equal(1))
	})
})
