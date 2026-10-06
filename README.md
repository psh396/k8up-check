# k8up-check

Kubernetes operator that automates ad-hoc [k8up](https://k8up.io) backups
and exports a completion metric for the backups it watches.

## Description

k8up-check does not define any CRDs of its own. It works entirely off existing
k8up `Schedule` and `Backup` resources and two annotations:

- **Trigger an ad-hoc backup.** Annotate a namespace with
  `k8up.check.backup/trigger`. The operator creates a one-off k8up `Backup` by
  copying the spec (backend, pod security context, resources, pod config) from a
  `Schedule` in that namespace. The annotation value names the Schedule to use;
  leave it empty when the namespace holds exactly one Schedule. The Backup is
  owned by the Schedule, so it is garbage-collected with it, and the operator
  only creates one Backup per Schedule until the existing one is removed.

- **Track backup results.** Backups created from a trigger annotation (the
  operator stamps them with the `k8up.check.backup/trigger` label), or any
  Backup carrying the `k8up.check.backup/watch` annotation, are watched to
  completion. k8up's own scheduled Backups are ignored, so the metric only
  reflects the customer namespaces that were annotated. Once a watched Backup
  reaches a terminal state the operator sets the gauge
  `k8up_backup_completion_success{backup_namespace,name}` to `1` (succeeded) or `0`
  (failed) — `backup_namespace` being the annotated namespace the Backup runs in — and
  drops the series when the Backup is deleted.

The `backup_namespace` label identifies the customer namespace.
A Backup waiting for PreBackupPods has not completed and does not emit this
completion metric yet.

For example, query the result of a triggered customer Backup with:

```promql
k8up_backup_completion_success{backup_namespace="customer_namespace"}
```

### Prerequisites
- go version v1.26+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster with the k8up CRDs installed.

## Getting Started

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/k8up-check:tag
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/k8up-check:tag
```

k8up-check has no CRDs of its own, so there is nothing to `make install`. It only
needs the k8up CRDs, which k8up installs separately.

### By providing a Helm Chart

A Helm chart is maintained under `dist/chart`. Install it with Helm directly:

```sh
helm install backup-check ./dist/chart \
  --namespace k8up-check-system --create-namespace \
  --set manager.image.repository=<some-registry>/k8up-check \
  --set manager.image.tag=tag
```


For development you can also drive the release through the Makefile:

```sh
make helm-deploy IMG=<some-registry>/k8up-check:tag   # upgrade --install the release
make helm-status                                      # show release status
make helm-uninstall                                   # remove the release
```
