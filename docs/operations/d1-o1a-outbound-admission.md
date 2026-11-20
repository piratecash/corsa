# Outbound route-frame admission accounting

The route-wire observer now records an outbound full-sync frame only after the original session queue accepts it. A disconnected peer, full queue or closed queue returns false before publication; the refused frame contributes neither offered bytes nor completed-pass credit. Inbound synchronous writes retain their earlier conservative observation because an error can follow a write.

This changes observation only. Sender admission, routing, wire frames, limits and deadlines are unchanged. A partially admitted pass, a later admitted queue discard, partial I/O, clock/window failure, byte/digest mismatch and reciprocal-session checks remain strict failures.

On the previous code, the new tests reproduced rejected frames in the offered count. The initial QA wrapper expected four assertion failures but received five: its extra failure correctly exposed two offered chunks despite only one queue admission. That wrapper STOP and logs are preserved. After localization, only the corrected ordinary and race runs were executed; each passed **11 parent tests and 65 child cases**, including a real complete pass after three kinds of refusal and a writer completion preceding its observer callback. All 2,109 tracked Go/module inputs were unchanged during the virtual-source tests.

[Retained evidence](evidence/d1-outbound-admission-2026-11-20/README.md) binds the exact source posts, both controllers and complete logs. These are unit/race results, not native traffic metrics. The producer return site in [the earlier native failure](d1-o1a-zero-offer-native-result.md) remains UNKNOWN. O‑1a, final D1 acceptance, public activation and release are not granted by this fix.
