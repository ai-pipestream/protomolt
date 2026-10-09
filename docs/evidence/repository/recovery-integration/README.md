# Recovery integration checks

Base: 5f8acfb413febd0a3ce1233d06d9a143d1220f86.
Includes historical-read 307d046f1 and archive recovery a04bb5c63.
Efficiency remains outside this PR pending expiry and lock-window review.

Local checks on 2026-10-09:

- Review checkout containing all original branches: 62 focused tests passed with
  0 failures/errors/skips, in 1m43s. Includes 42 mode tests. Passing tests do not
  resolve the efficiency review findings.
- Recovery checkout fb67e50a4247cd3c82ec641cefee21081fa2307f plus this wiring patch:
  archive qualification passed in 1m57s, 10 tests, 0 failures/errors/skips.
  archive-summary.json records 2 restored backups and 8 negative cases.
- Selecting ArchiveBackupQualificationIT without the init script failed in 3s:
  No tests found for given includes. This checks the ordinary-task exclusion.
  That command overwrote the earlier XML; the 10-test count was read beforehand.
  Per-case JSON was preserved.

Commands use flock -w 1800 /tmp/protomolt-repository-qualification.lock:

    ./gradlew -I repo/container/src/test/resources/archive-backup-qualification/qualification.init.gradle :protomolt-repo-container:test --tests '*ArchiveBackupQualificationIT' --max-workers=2 --console=plain
    ./gradlew :protomolt-repo-container:test --tests '*ArchiveBackupQualificationIT' --max-workers=2 --console=plain

The longer lock wait changes no test timeout. Hosted CI has not been verified here.
Earlier agent evidence is retained without changing the reported skips.
