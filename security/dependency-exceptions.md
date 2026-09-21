# Temporary dependency exceptions

Status: approved 2026-09-21, expires at 2026-10-21 UTC (CI fails on that date).
Owner: LaminarDB maintainers responsible for [PR #540](https://github.com/laminardb/laminardb/pull/540).
The four residual exceptions below were explicitly accepted for this interval under S4.
Machine-readable versions, sources, checksums and reasons are in `dependency-exceptions.json`.

| Advisory | Pinned crate | Accepted residual risk |
| --- | --- | --- |
| [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) | rsa 0.9.10 | No patched release. reqsign cloud authentication uses randomized signing rather than RSA decryption, but this does not establish absence of timing leakage or private-key exposure. |
| [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436.html) | paste 1.0.15 | Unmaintained compile-time dependency through DataFusion/Parquet/tokenizers. |
| [RUSTSEC-2026-0173](https://rustsec.org/advisories/RUSTSEC-2026-0173.html) | proc-macro-error2 2.0.1 | Unmaintained compile-time dependency through validator_derive in Delta Lake. |
| [RUSTSEC-2024-0384](https://rustsec.org/advisories/RUSTSEC-2024-0384.html) | instant 0.1.13 | Unmaintained dependency in the wider lockfile, including reqwest-retry/wasm-timer and old parking_lot. |

Two additional version-only exceptions cover RUSTSEC-2026-0194 and RUSTSEC-2026-0195 **only
for the verified quick-xml backport**, with the same deadline. They do not accept the unfixed
XML vulnerabilities. Upstream provenance and reproduction commands are in
[`vendor/quick-xml/SECURITY-BACKPORT.md`](../vendor/quick-xml/SECURITY-BACKPORT.md).

Before either scanner, CI verifies expiry, exact lockfile identities and checksums, the
local patch path, its entire source digest, and agreement between both scanner ignore lists.
Changing any accepted version or source requires a new review; other advisories retain their
existing severity. Local verification must likewise run the guard before either scanner:

```sh
python tools/check_dependency_exceptions.py
cargo audit --deny warnings
cargo deny --locked --workspace check
```

Scope: embedded, single-node server and cluster builds from this workspace use the XML patch.
Published library consumers do **not** inherit Cargo workspace patches. Consumers using the
affected cloud storage or connectors must apply the same backport in their application's root manifest
or use a compatible dependency graph with quick-xml >=0.41; the published dependency graph
is not qualified by this workspace-only repair. The release workflow therefore blocks registry
publication before any crate is uploaded while this backport is required; this guard does not
block binaries built from the verified workspace. `--publication` on the guard exercises that
nonpublishing rejection locally. Remove the patch and XML exceptions when the
current object_store/OpenDAL generation accepts a fixed release, or after a separately
validated analytical dependency migration. Revisit the four residual findings before expiry;
there is no automatic extension.
