# Lab-only Dolt bootstrap

This command prepares one existing synthetic Beads database for the near-data
gateway. It cannot create a database or initialize/migrate the Beads schema.
It connects as passwordless `root` only over a numeric loopback address and
fails before mutation unless all of these identities match:

- the selected database is the requested `beads_perf_lab_...` database;
- `metadata._project_id` is the requested canonical project UUID;
- any existing `_beads_perf_lab_attestation_v1` marker is the same lab,
  database, and project identity.

The marker value is canonical JSON:

```json
{"version":1,"environment":"synthetic","lab_id":"<uuid>","database_name":"beads_perf_lab_<name>","project_id":"<uuid>"}
```

The command derives a distinct account from the complete canonical project UUID
(`32-lowercase-hex@%`), resets only that account, and grants exactly:

```text
SELECT, INSERT, UPDATE, DELETE, EXECUTE ON `beads_perf_lab_<name>`.*
```

`EXECUTE` is required for the gateway's `CALL DOLT_COMMIT(...)`. Global
`USAGE` may appear in `SHOW GRANTS`; any other global privilege (including
`FILE`), cross-database scope, table scope, role, grant option, or unexpected
database privilege makes the command fail closed. Raw grant rows are not
printed.

Example (placeholders only):

```sh
go run ./scripts/bench-remote-server/lab-bootstrap \
  --host 127.0.0.1 --port 13360 \
  --database beads_perf_lab_example \
  --project-id 11111111-1111-4111-8111-111111111111 \
  --lab-id 22222222-2222-4222-8222-222222222222 \
  --password-file /private/tmp/beads-perf-lab-runtime/dolt-password
```

Ports `3306` and `3307` are rejected because they are reserved as production-
like defaults in this lab. Use a separately allocated loopback-only port.

The password file's final path component is opened without following symlinks,
must be a regular file with mode exactly `0600`, and must contain exactly 64
lowercase hexadecimal characters (an optional surrounding newline is trimmed).
Dolt 2.1.10 rejects bind placeholders in `CREATE USER` and `ALTER USER`, so the
validated password is embedded in those two DDL statements. The restricted
alphabet cannot terminate the SQL string or inject syntax. The statements,
password, and DSN are never printed or logged; underlying errors from those two
statements are deliberately suppressed in case a server parser echoes SQL.

## Deliberate limitations

- The per-project account uses `@%` for Dolt host-matching compatibility; the SQL listener
  must remain loopback-only or separately firewalled. Database authorization
  still limits the account to the one attested database.
- The integration test mutates the lab attestation and the derived lab account and
  therefore runs only when `BEADS_PERF_LAB_BOOTSTRAP_INTEGRATION=1` and all
  `BEADS_PERF_LAB_*` variables are explicitly supplied.
