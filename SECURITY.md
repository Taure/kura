# Security Policy

## Supported versions

Security fixes are released for the latest 2.x minor. Earlier minors and
1.x get no fixes: upgrade to the latest 2.x release.

| Version | Supported |
| --- | --- |
| latest 2.x | yes |
| older 2.x | no, upgrade |
| 1.x | no |

## Reporting a vulnerability

Report privately through GitHub: open the
[Security tab](https://github.com/Taure/kura/security) of this repository
and choose "Report a vulnerability". Do not open a public issue or pull
request for a suspected vulnerability.

Include the kura version, the backend and its version, and the smallest
schema, query or changeset that shows the problem. You will get a reply in
the advisory thread. A fix is released before the advisory is published,
and the reporter is credited unless they ask not to be.

## Scope

In scope, in this repository:

- SQL built by the query builder and the dialects: a value reaching the
  statement text instead of a bound parameter, or an identifier that is
  not quoted.
- Changesets: a param reaching a field it was not cast to, directly or
  through `cast_assoc`, `cast_embed` and `put_assoc`.
- Encryption at rest: nonce, key and frame handling, a ciphertext accepted
  for a column or key it was not written for, plaintext or key material in
  an error, a log line or telemetry.
- Multitenancy: a statement kura builds running outside the tenant the
  process is in.
- Redaction: a redacted or encrypted field's value in telemetry, the query
  log or the audit log.
- Migrations: DDL built from operations, the migration lock and the
  version table.

Out of scope:

- Raw SQL you write yourself: `query/3` and `{fragment, SQL, Params}` text
  run as given. Values belong in their parameters.
- The backends and their drivers. Report those to
  [kura_postgres](https://github.com/Taure/kura_postgres) or
  [kura_sqlite](https://github.com/Taure/kura_sqlite).
- The database server, its configuration and its own statement log.
- Key storage. kura reads keys from the keyring you configure; keeping
  them is the application's job.

## What kura does and does not protect

- [Encryption at rest](README.md#encryption-at-rest): what is encrypted,
  the frame formats, key rotation, and what an encrypted field cannot be
  used for.
- [Multitenancy](guides/multitenancy.md): how statements are scoped, what
  is not scoped, and `require_tenant`.
- [Redacted fields](guides/telemetry.md#redacted-fields): which values are
  replaced in telemetry and logs, and which are not.
- [Configuration](README.md#configuration), under "Defaults that change in
  3.0": the settings whose 2.x default is the permissive one. Set them now.
