# TiDB isolated integration

This runner starts a new one-PD/one-TiKV/one-TiDB 8.5.8 cluster in Docker,
with a random project name, fresh container storage and an IPv4 loopback-only
random SQL port. It checks the container ID, project label, run label and port
before allowing a test to connect. It creates no resources in another cluster.

It exports an explicitly selected C++ Git commit and its pinned protobuf
submodule into an ignored `build/` directory; uncommitted checkout contents
are not included. It then exercises:

* Python portal DDL, authentication, ETag concurrency, whitelist and
  announcement CRUD against real TiDB/TiKV.
* All Go `TestTeamTiDB*` cases, including apply/invite, concurrent membership,
  replay, original identity and ownership fences.
* The native Mall **production mode** with `require-tidb=true` and a real
  NORMAL writer registration; the Python HTTP proxy creates and lists an SKU
  and checks authentication and idempotent replay.

The runner currently supports Windows. Install Python with this project’s
`.[cluster,dev]` extras, Docker Desktop with Compose, Git, Go, Maven, a MySQL
command-line client, and `protoc`. The C++ checkout must have the protobuf
submodule initialized with the commit referenced by the selected C++ commit.
Go/Maven dependencies and the pinned TiDB images must be present or downloadable.
The Mall test uses a Java container solely as the native peer; the Python
server itself does not require Java.

```powershell
.venv/Scripts/python.exe tools/tidb_interop/run.py `
  --cpp-repo D:/path/to/xuanming-server-mmo-cpp `
  --cpp-ref 098f18ade7a3a8692698add32dc64556a665c7dc `
  --mysql D:/path/to/mysql.exe `
  --protoc C:/path/to/protoc.exe `
  --mvn D:/path/to/mvn.cmd
```

`--java-image` overrides the default `eclipse-temurin:21-jre`. If Docker's
automatic address pool is exhausted, pass an **unused** private IPv4 network,
for example `--subnet 10.248.174.0/24`; Docker rejects overlapping networks.
Use a commit that includes the Go Team caller-identity fix. Tests can take
several minutes and require sufficient memory for the isolated cluster.
`--only-mall` is available for troubleshooting; its output explicitly marks
the scope as `mall-only` and must not be reported as the full acceptance.

The per-run `acceptance.json`, portal/Mall reports and Go log are kept under
the ignored `build/tidb-interop-<id>/` directory. The runner removes only
containers and volumes from its own verified Compose project, on success or
failure. Generated authorizations and passwords are test-only; they are not
printed. Private test control directories are deleted after their resolved
paths and run-specific names are checked, including on failure.

This is a real TiDB/TiKV integration in production Mall mode, **not** an I3
qualification or multi-replica production-failover certification. It uses a
local-test `SELECT_ONLY` lock declaration and one replica, not an operator's
production physical-authority or lock-qualification report. It also does not
by itself establish a complete Python-to-Go multi-service TiDB acceptance.
