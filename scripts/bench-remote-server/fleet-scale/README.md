# Fleet-scale destructive probe

`fleet-scale` creates and mutates disposable `beads_perf_lab_scale_*`
databases. Numeric loopback is necessary but not sufficient isolation because
a local port can forward to a remote server. Before creating any database, the
command therefore requires both:

- `--ack-create-synthetic-databases`; and
- a non-nil canonical `--lab-id` exactly matching the pre-existing control row
  in `beads_perf_lab_control.lab_identity`.

The fixed read-only identity query requires exactly one row whose
`environment` is `synthetic` and whose `lab_id` is the supplied UUID. A missing
database, table, row, duplicate row, environment mismatch, or UUID mismatch
fails before `setupDatabases`. The harness never creates, updates, or repairs
the control database or marker; provisioning must establish and independently
audit it before this probe is enabled.

Ports `3306` and `3307`, hostnames, non-loopback addresses, and output paths
outside the explicit `--output-root` are rejected. The root must be an existing
direct `/private/tmp/beads-fleet-scale-*` directory. Its directory components
and the final file are opened relative to that root with `O_NOFOLLOW`; results
are written as a `0600` regular file.
