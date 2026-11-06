# Neo4j coexistence validation

Validation performed on Linux x86-64 with GCC 14 on 2026-11-06. This is a development validation record, not a release certification.

## Corrections after the first audit

- OpenHands ownership-matching test now uses MCP ID `codebase-memory-neo4j`.
- The release archive's negative executable-mode test now mutates the actual member, `server/codebase-memory-mcp-neo4j`.
- Native formatting follows CI's clang-format 20. A local variable in MCP traversal now has the narrower scope requested by cppcheck.
- The full sanitizer run exposed a null-key `strlen` crash in Helm chart-image linking. The link builder now guards the nullable key pointer directly before measuring or emitting it. The same 184-test grammar/Helm suite that crashed passes after the correction.

## Results

- Canonical contract gate, `scripts/test.sh --contracts-only`: all 42 checks passed in one run.
- Full plain native runner: 192 suites, 9,993 passed, 1 failed, 10 skipped. The failure was `daemon_frontend_eof_drain_timeout_cancels_and_returns_success`, which passed on the earlier complete audit and on the isolated 14-test suite rerun. This intermittent failure remains unresolved; the full run was not clean.
- Final native MCP/configuration/daemon frontend subset: 435 passed, 4 skipped.
- Final ASan/UBSan MCP/configuration/daemon frontend subset: 435 passed, 4 skipped.
- ASan/UBSan grammar/Helm suite after the null-key correction: 184 passed.
- Complete ASan/UBSan run: all 192 suites were attempted; 9,919 tests passed and 10 were skipped, but the grammar/Helm suite crashed after 109 tests. Its remaining 75 tests ran in the corrected 184-test rerun. All 9,994 non-skipped native cases therefore passed across the broad run and corrected-suite reruns, not in one clean complete run. The harness printed zero failed assertions, but correctly returned failure for the sanitizer crash; that exit is not counted as a passing full run.
- Formatting check with clang-format 20: passed. Final MCP source check with cppcheck 2.20.0: passed.
- Earlier unchanged surfaces: frontend 4,001 unit tests and TypeScript/Vite production build passed; Python wrapper 45 passed; npm wrapper 20 passed and 3 skipped; Go wrapper tests passed; live Docker Neo4j integration 29 passed; all six production-process lifecycle guards passed.
- The isolated real-upstream installation/reinstallation/update/uninstallation regression passed, preserving upstream executable, updater, registrations, integration resources, and running daemon.

Assertions and sanitizers were not disabled to make a test pass. The sanitizer runner confirmed `sanitized=1 test_seams=1`. The plain test build downgraded GCC's existing `free-nonheap-object` diagnostic from an error in a daemon test fixture; assertions remained enabled.

## Remaining limitations

- UBSan emits nonfatal diagnostics for null arguments in existing callable-signature, Perl, and vendored ObjectScript scanner code. A passing runner exit does not establish sanitizer-clean behavior.
- The memory-core allocation-budget linter fails in both this branch and a source snapshot of original main `67518b6f`. Counts exceed the existing baseline in Cypher, MCP, Neo4j, pipeline, and store code. The baseline was not raised or the check disabled. Cppcheck also reports constant-condition style findings in the existing production-disabled allocation-fault seams in `pipeline.c`.
- Four existing frontend release-proof tests fail because their August evidence, candidate commit binding, and artifact inventory are stale. Regeneration was attempted, but its evaluation prerequisite check fails: the expected `graph-ui/cbm` backend build and the local `Qwen3.5-2B-Q4_K_M.gguf` and `gemma-4-E4B-it-Q4_K_M.gguf` models are absent. Historical timestamps, hashes, and source commits were not rewritten to manufacture fresh evidence.
- Native Windows/PowerShell and macOS coexistence, the full graph Docker image build, and a complete clean CI gate have not been validated here. Linux packaging contracts do not substitute for those platform runs.
- A future release must produce the renamed archives and refresh package-manager versions/checksums. No release was published. See [FORK_COEXISTENCE.md](FORK_COEXISTENCE.md) for migration and explicitly shared-cache/port/Neo4j graph risks.

Run the canonical merge gate with `scripts/test.sh` and lint with `scripts/lint.sh --ci CLANG_FORMAT=clang-format-20` in a fully provisioned environment. Container tests require Unix sockets and a process reaper; the local audit used a Linux child-subreaper wrapper. Detailed local logs are retained under `/workspace/cbm-validation` in the development workspace.
