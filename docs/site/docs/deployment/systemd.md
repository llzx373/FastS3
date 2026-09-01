# systemd

Source files: `deploy/systemd/` (`install-systemd.sh` + two hardened units).
Container path: [Containers](container.md).

## Architecture

| Unit | Process | Description |
| --- | --- | --- |
| `fasts3.service` | `fasts3d serve --config /etc/fasts3/fasts3.toml` | Data plane: S3(9000) + admin (unix socket); replication port default 9445 (mTLS, see `[replication]` in config) |
| `fasts3-web.service` | `node dist/index.js`(FS3_WEB_CONFIG=/etc/fasts3/web.json) | Management plane: loopback 127.0.0.1:9090 only; stateless (all state on the Rust side) |

## Hardening (data plane; comments in the unit file)

```ini
LimitMEMLOCK=infinity        # io_uring registered buffers need mlock; the default 64KiB cap fails registration
NoNewPrivileges=yes          # Forbid setuid / file-capability privilege escalation
ProtectSystem=strict         # /, /usr, /boot, /etc mounted read-only
ProtectHome=yes              # /home /root /run/user read-only/invisible
PrivateTmp=yes               # Private /tmp and /var/tmp
ProtectKernelTunables=yes    # /proc/sys /sys read-only (block changing global kernel params)
ProtectKernelModules=yes     # Forbid loading kernel modules
ProtectControlGroups=yes     # cgroup read-only
ReadWritePaths=/var/lib/fasts3 /run/fasts3 /etc/fasts3
                             # Data path + admin socket + config hot-reload write path
UMask=0077                   # New files owner-only read/write
KillSignal=SIGTERM           # Graceful drain (in-flight requests → checkpoint → exit)
TimeoutStopSec=10            # Drain window; SIGKILL on timeout
Restart=on-failure           # Auto-restart on crash
RestartSec=2s                # Backoff
```

- Data plane runs as root by default (raw device + io_uring privileges); non-root form needs
  `AmbientCapabilities=CAP_SYS_ADMIN CAP_IPC_LOCK` and device ACLs.
- Management plane has no disk-write requirement: **do not set ReadWritePaths** (fully read-only); the same `NoNewPrivileges` and other hardening still apply.

## Install / uninstall

```bash
# Install (unit → /etc/systemd/system; create /etc/fasts3, /var/lib/fasts3(+meta);
# first install copies config templates; daemon-reload and enable --now)
sudo deploy/systemd/install-systemd.sh install

# Status / uninstall
sudo deploy/systemd/install-systemd.sh status
sudo deploy/systemd/install-systemd.sh uninstall     # keep data and config
```

Environment variables: `UNIT_DIR` (default /etc/systemd/system; packaged form uses /lib/systemd/system),
`NO_START=1` (install only, do not start; WSL/container CI), `CONFIG` (template path).

## Directories and permissions

| Path | Owner/mode | Purpose |
| --- | --- | --- |
| `/etc/fasts3/` | root:root 0750 | Config (`fasts3.toml` 0640, `web.json` 0600) |
| `/var/lib/fasts3/` | root:root 0750 | Data: disk image + `meta/` (rocksdb) |
| `/run/fasts3/` | systemd RuntimeDirectory 0750 | Runtime files such as admin.sock |

## Config hot reload

After changing `/etc/fasts3/fasts3.toml`:

```bash
sudo systemctl reload fasts3        # admin H3 hot reload: rate limits / anonymous read / config keys
# Remaining fields (storage layout, etc.) require a restart:
sudo systemctl restart fasts3
```

## Environments without systemd (WSL/containers)

Unit files and the script can still be installed, **but there is no PID 1 supervision**; for drills/container scenarios use:

```bash
nohup fasts3d serve --config /etc/fasts3/fasts3.toml >/var/log/fasts3.log 2>&1 &
# Stop: pkill -TERM -f 'fasts3d serve'
```
