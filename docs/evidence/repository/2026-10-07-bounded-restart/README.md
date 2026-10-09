# Redis restart and fresh-JVM history

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
passed in 3m40s. Sol reviewed the restart test and port configuration.

The original host closes and saves the request, receipt, document and generation.
After that JVM exits, Docker restarts the same Redis container. The harness checks
a new start timestamp, the expected host port and PING. A fresh JVM uses the same
PostgreSQL database, Redis endpoint and generation. It creates no Git registry;
any live schema request throws. Receipt replay matches the saved response, and
local/gRPC history matches the saved document, metadata, policy and raw bytes.

Redis uses AOF with always-fsync and no eviction. This proves graceful restart
with the same container storage. It does not prove power-loss durability, volume
replacement, interrupted writes or multiple revision restoration.

Docker changed an automatically assigned host port in the initial test. The
harness now specifies a fixed test port. Selecting a free local port before Docker
binds it has a small race: contention fails container startup. No backend endpoint
fallback or identity change was added. Remote-Docker portability is unqualified.
