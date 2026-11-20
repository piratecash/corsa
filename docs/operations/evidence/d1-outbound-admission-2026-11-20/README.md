# Outbound admission test evidence

`SUMMARY.json` identifies the tested base, exact three-post manifest and test scope. `RAW.tar.xz` preserves full original-red and corrected ordinary/race command streams. `SOURCE.tar.xz` retains the posts, controllers, source inventories, results and independent receipts; no ELF or build cache is included.

`RAW-MEMBERS.json`, `SOURCE-MEMBERS.json` and `ORIGINS.json` identify every archive member and its actual local origin. Archives use deterministic USTAR headers. `SHA256SUMS` covers all top-level files except itself. `prepare_evidence.py` only collects retained data; it does not execute tests, applications or Git.

The original RED wrapper STOP is retained, including the incorrect expected failure count. Corrected code passed 11 parent/65 child tests in both ordinary and race modes. Native sender-false causes remain UNKNOWN; no O‑1a traffic result or release acceptance is asserted.
