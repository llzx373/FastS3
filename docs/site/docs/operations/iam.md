# IAM multi-tenant ops

For teams migrating from MinIO: concept mapping (User / Group / Policy / Service Account)
via the console, REST, or `fasts3d iam` — **not** the `mc admin` wire protocol.
Protocol-level decisions: [Compatibility matrix · IAM](../reference/compat.md).

## 1. Red line: the `mc admin` binary is not supported

FastS3 **does not implement** the `/minio/admin/v3` wire protocol; subcommands such as `mc admin user/group/policy/
svcacct` are **ineffective** against this service (ADR-28 DI8.3/DI10, explicit non-goal). What is aligned
is **concepts and canned policy names** (User/Group/Policy/Service Account, `readonly`/
`readwrite`/…), not the wire protocol — operational habits can transfer; the binary cannot.
Day-to-day identity management uses the console IAM pages, `/api/iam/*` REST, or **`fasts3d iam`**
(via the running instance's admin channel, same API as the console; see [CLI](../reference/cli.md)).

## 2. Login and identity sources

Console login (`/api/login` and `/api/oidc/login`) identity resolution order:

1. **Local configured users** (`[[web.users]]`, synced at startup to same-named
   IAM Users in tenant `default`: role=admin → attach `consoleAdmin`, readonly → attach `readonly`,
   idempotent, attach only when there are no existing attachments);
2. **LDAP bind** (when enabled): a successful bind only proves the directory credentials are valid; identity must be an already-synced
   IAM User; no matching User → 401 `no_such_user` (sync first, then log in; no auto-provisioning);
3. **IAM user password** (when the first two miss): `POST /v1/iam/verify-password`,
   constant-time compare; unknown user / no local password / wrong password all return the same 401, no existence leak;
4. **OIDC SSO**: `POST /api/oidc/login`, sub → IAM User; unknown sub JIT-provisions
   into the configured default group (JIT never attaches policies directly, never grants `consoleAdmin` from a claim).

**JWT only proves “who logged in” (identity-only)**; all authorization decisions go through IAM effective policies
(`admin:*` action family, `POST /v1/iam/authorize` evaluation); the `role` claim in the JWT
is only a UI transitional hint. Details: [Compatibility matrix](../reference/compat.md) M18 C1 section.

## 3. MinIO concepts → FastS3 mapping

Console pages are under the “IAM” group in the Web console (9090) nav; API column is the Node management-plane
proxy path (`/api/iam/*`; Rust admin side is `/v1/iam/*`, loopback/unix trusted channel).

| MinIO operation | FastS3 console | FastS3 API | CLI (`fasts3d`) |
| --- | --- | --- | --- |
| `mc admin user add` | Users page → New | `POST /api/iam/users` | `iam users create --name …` |
| `mc admin user list` / `info` | Users page list | `GET /api/iam/users[?tenant=]` | `iam users list` / `get` |
| `mc admin user enable` / `disable` | Users page → enable/disable switch | `PATCH /api/iam/users/{tenant}/{name}` (`enabled` boolean) | `iam users update --enable\|--disable` |
| `mc admin user remove` | Users page → Delete (must revoke all of its SAs first) | `DELETE /api/iam/users/{tenant}/{name}` | `iam users delete` |
| `mc admin user policy` (change attachments) | Users page → Edit policies | `PATCH .../users/{tenant}/{name}` (`policies` full-table replace) | `iam users update --policies a,b` |
| `mc admin group add/enable/disable/info` | Groups page | `POST /api/iam/groups`, `GET .../groups`, `PATCH .../groups/{tenant}/{name}` | `iam groups list\|create\|get\|update\|delete` |
| `mc admin group remove` | Groups page → Delete | `DELETE /api/iam/groups/{tenant}/{name}` | `iam groups delete` |
| `mc admin policy create` | Policies page → New | `POST /api/iam/policies` | `iam policies create --name … --file p.json` |
| `mc admin policy list` / `info` | Policies page list (includes canned, read-only) | `GET /api/iam/policies[?tenant=]` | `iam policies list` / `get` |
| `mc admin policy attach` / `detach` | Users/groups page, change `policies` | Same PATCH users/groups | `iam users\|groups update --policies` |
| `mc admin policy remove` | Policies page → Delete (still attached → 409) | `DELETE /api/iam/policies/{tenant}/{name}` | `iam policies delete` |
| `mc admin svcacct add/list/remove` | Service accounts page | `GET/POST/DELETE /api/iam/service-accounts[/{access}]` | `iam sa list\|create\|get\|delete` |
| MinIO STS `AssumeRole` | — (client calls directly) | `POST /v1/iam/assume-role`; Node `POST /api/sts?Action=AssumeRole` | — |
| Other `mc admin` | Dashboard / audit / doctor | `/v1/admin/status` etc. | `audit query` / `keys list` / `doctor` |

Field names follow MinIO ops habits (`accessKey`/`policy`/`members`); paths are not copied
(ADR-28 DI8.1). SA secret is **echoed only in the create response**, consistent with `mc admin svcacct add`
printing a one-time secret.

## 4. Canned policy mapping

Built-in canned policies are code constants: **read-only, not persisted**, cannot PATCH/DELETE
(`policy_readonly`); custom policies that collide with reserved names are rejected (`policy_name_reserved`).

| MinIO canned | FastS3 | Contents (FastS3 action translation) |
| --- | --- | --- |
| `readonly` | `readonly` (same name) | `s3:Get*/List*/Head*` |
| `readwrite` | `readwrite` (same name) | `s3:*` |
| `writeonly` | `writeonly` (same name) | `s3:Put*/Delete*/CreateBucket/Abort*/Restore*/Multipart` |
| `diagnostics` | `diagnostics` (same name) | Management-plane read-only `admin:List*/Get*` + s3 read |
| `consoleAdmin` | `consoleAdmin` (same name; **root-grant only**) | `admin:*` + `s3:*`, cluster-wide, including tenant management |
| — (no MinIO equivalent) | `tenantAdmin` (FastS3 addition) | In-tenant user/group/policy/SA/role management + `s3:*`; cross-tenant is hard-denied at evaluation |

Resource is always literal `*` (this engine's service-level action resource semantics); do not write `arn:aws:s3:::*`.

## 5. Production checklist: “root for bootstrap only” (ADR-28 DI4)

“root” = the console bootstrap account: the first `[[web.users]]` user with role=admin in the config file;
after startup sync it is attached to `consoleAdmin` (cluster-wide, including tenant management). Day-to-day ops **does not depend** on it:

1. **Bootstrap**: root logs into the console → Tenants page (visible only to consoleAdmin) creates department tenants
   → in each tenant create the first `tenantAdmin` user (set a strong password);
2. **Vault**: put the root password in a vault (password manager); do not use it day-to-day, do not distribute it;
   reclaim path = detach that user's attachments on the IAM side (startup sync is idempotent and will not resurrect reclaimed attachments);
3. **Day-to-day**: department admins use **their own console accounts** (attached to `tenantAdmin`) to manage this tenant's
   users/groups/policies/service accounts/roles and this tenant's buckets; ordinary users **self-serve** create/revoke
   their own SAs on the service-account page (owner=self is always allowed, no admin needed);
4. **Data-plane red line**: the root bootstrap account never holds and never uses a data-plane AK; forbid “everyone
   shares one AK as super-admin” — application keys always go through user self-serve SAs (can attach an embedded policy to shrink permissions);
5. **Audit**: data-plane audit entries `who` = initiator access key / user, per-operation traceable;
   console login source and identity changes (local/ldap/iam/oidc) are recorded in identity events
   (`GET /api/identity-events`); auth-failure side notes land in `auth_note`
   (`key_disabled`/`key_not_found`/`session_token_invalid`/`user_disabled`).
   Periodic review: root-account login events should appear only in bootstrap/emergency scenarios; day-to-day use
   is a policy-violation signal.

Drill script `tests/iam/delegated_admin_drill.sh` (M18/C2) covers this checklist end to end:
root creates tenant + tenantAdmin → admin creates a user attached to `readwrite` → user self-serves an SA →
SA reads/writes this tenant's buckets, List/GET on another tenant fails — no root data-plane AK used throughout.
