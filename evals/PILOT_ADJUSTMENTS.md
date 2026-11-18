# Pilot adjustments (first run, 2026-11-12)

Case-file changes made during the first execution, where the eval (not the product) was wrong.

| Step | Was | Now | Why |
|---|---|---|---|
| S01-20 | fast index has the same File count as full | 12 <= File count < full | `mode: fast` excludes `docs/` and test directories by design; the index reply lists them under `excluded` (19 vs 24 File nodes) |
| S02-10 | see case file | | SQLite picks the UNIQUE(source_id, target_id, type, ...) autoindex as a covering index; any source_id index is correct |
| S02-16 | see case file | | file_hashes column is rel_path, not path |
| S02-17 | see case file | | file_hashes column is rel_path, not path |
| SG-01 | see case file | | a Folder node is also named hub; restrict to label Function |
| SG-02 | see case file | | a Folder node is also named hub; restrict to label Function |
| SG-19 | see case file | | include_connected puts neighbours in a 'connected' column of the matched row, not as extra rows |
| SG-32 | see case file | | compose services are named Service/<name> (qn suffix docker-compose::compose/Service/-/<name>) |
| SG-07a | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| SG-07b | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| SG-07c | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| SG-07d | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| SG-07e | see case file | | new step pinning the regex/glob bracket-class defect found in the pilot |
| PG-11 | see case file | | default max_output_tokens (3200) truncates the 137-node page; raise the budget to test paging alone |
| PG-04 | see case file | | out-of-schema value is clamped, not rejected; kept as a low-severity finding at tier 2 |
| PG-06 | see case file | | out-of-schema value is clamped, not rejected; kept as a low-severity finding at tier 2 |
| QG-17 | see case file | | toLower() on the left of a WHERE predicate is a parse error; kept as a finding at tier 2; QG-17a checks the supported form |
| QG-17a | see case file | | supported form of QG-17 |
| QG-18 | see case file | | aggregate column is named COUNT(c) and values are strings |
| TP-19 | see case file | | compose services are found as Service/<name> |
| TP-20 | see case file | | out-of-range depth is accepted, not rejected; kept as a low-severity finding at tier 2 |
| TP-21 | see case file | | out-of-range depth is accepted, not rejected; kept as a low-severity finding at tier 2 |
| CS-27 | see case file | | an unknown file gives an empty outline (total 0), an honest answer |
| COV-16 | see case file | | aggregate column is COUNT(c), values are strings |
| COV-17 | see case file | | string concatenation with + is not in the Cypher subset; return the pair as two columns |
| COV-20 | see case file | | compose resources are named Service/<name> |
| COV-31 | see case file | | check the exact verdict fields |
| COV-32 | see case file | | check the exact verdict fields |
| COV-33 | see case file | | check the exact verdict fields |
| COV-37 | see case file | | check the exact verdict fields |
| COV-39 | see case file | | check the exact verdict field |
| COV-42 | see case file | | full diagnostics report parse_partial / parse_unusable / skipped / not_indexed, not a 'coverage' key |
| DC-04 | see case file | | format_cents lives in the changed file (a seed); its callers notify and Order.total are impacted |
| DC-05v | see case file | | via moved before the re-index so there are impacted rows |
| DC-08 | see case file | | after re-index the only seed is the new, uncalled extra_fn, so nothing is impacted; via is checked in DC-05v |
| DC-10 | see case file | | edge_types/fixpoint/via require direction inbound (documented error) |
| DC-18 | see case file | | compare_graphs requires two distinct projects; compare with a second project of the same repo |
| AR-16 | see case file | | set_sections was removed as a write primitive; write the ADR with mode update |
| AR-18 | see case file | | argument is traces: [{caller, callee}] with qualified names |
| S03-01 | see case file | | BM25 rows carry qn (no name column) |
| S03-02 | see case file | | BM25 rows carry qn (no name column) |
| S03-03 | see case file | | BM25 rows carry qn (no name column) |
| S03-08 | see case file | | BM25 rows carry qn (no name column) |
| gen_medium.py | see case file | | the indexer skips a directory named gen as generated code; the package root is now com/scale |
| N01-13 | see case file | | a Folder node is also named hub; restrict to label Function |
| N02-01..03 | see case file | | the cbm_project_name constraint has a backing RANGE index named cbm_project_name; count only cbm_node_* indexes |
| N02-12 | see case file | | PartialTop is the good outcome; forbid only a plain Sort/Top (exact operator names) |
| N02-29 | see case file | | a re-index with no source change skips the sync, so ensure_schema does not run; kept as a finding (tier 2) |
| N02-30 | see case file | | kept as a finding: the dropped index is still missing after a no-change re-index |
| N02-30a | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N02-30b | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N02-30c | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N02-30d | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N02-30e | see case file | | repair after the N02-29/30 finding so later suites run with all indexes |
| N03-10 | see case file | | kept as a finding: a docstring edit re-creates the file's rows (+4 -4) instead of changing them in place (~k) |
| N03-30a | see case file | | a sync is skipped when the generation is unchanged; deleting the state file alone does not force one |
| N04-02a | see case file | | a sync is skipped when the generation is unchanged |
| N04-17 | see case file | | DEFECT PIN kept: after the graph was deleted inside Neo4j, a no-change re-index answers 'unchanged' and serves an empty graph |
| N04-19a | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19b | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19c | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19d | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19e | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N04-19f | see case file | | repair after the deleted-graph defect so later suites see a healthy graph |
| N06-06 | see case file | | kept as a finding: native replies carry no graph_backend block (tier 2) |
| N07-06a | see case file | | pin the credential-poisoning defect and isolate later steps from it |
| N07-06b | see case file | | pin the credential-poisoning defect and isolate later steps from it |
| N07-17 | see case file | | count only cbm_node_* indexes (the constraint has its own backing index) |
| N05-24 | see case file | | DEFECT PIN: the existing project must keep reading after another project's failed login |
| N05-25/26 | see case file | | restart the daemon after the bad-login pin |
| N05-12 | see case file | | kept as a finding: check_index_coverage is refused while Neo4j is down although its data is local (tier 2) |
| N99-06 | see case file | | count only cbm_node_* indexes |
| N09-19 | see case file | | BM25 rows carry qn (no name column) |
| DC-17 | see case file | | on a clean tree detect_changes reads no graph rows, so it names no backend; accept that |
| N08-02/03/04/12, compare_backends.py | see files | | normalise repo paths and both project names; ignore BM25/semantic score values and row order where the twin projects' different names change them; skip environment-dependent steps (project lists, ADR state, a different second project) |
| SG-26/27 | see case file | | raise max_output_tokens so the comparison is not cut by the output budget (the longer project name fits fewer rows) |
| N02-07..15 | see case file | | EXPLAIN on a tiny graph is cost-dependent (planner chose NodeIndexScan); demoted to tier 2; PROFILE with a database-access ceiling on eval-medium is the tier-1 check |
| N09-10 | see case file | | the next class in the same package: new C().run() resolves to run() |
| N09-10/10b/10c | see case file | | the cross-package chained call resolves to the class, not run(); tier-1 check moved to a same-package pair, cross-package kept as a tier-2 resolver finding |
| N09-12..15 | see case file | | raise max_output_tokens: the default budget truncates 500-row and 5,000-row pages |
| N09-30..36 | see case file | | new: PROFILE the product's Neo4j query shapes on the 120k-node graph with database-access ceilings |
| N04-07 | see case file | | a Folder node is also named hub; restrict to label Function |
| N06-19 | see case file | | the engine treats -- as a comment, so the hidden pattern is ignored; safe outcome is an error or this project's own count |
| N06-27 | see case file | | a Folder node is also named hub; restrict to label Function |
| N08-07/08 | see case file | | compare content unordered; the tie order of trace callers and snippet suggestions differs between backends and is reported as a finding |
| N09-35 | 60,000 | 80,000 | the project filter adds one property read per row: 60,010 accesses for a 20,000-row page is the intended ~3 per row; the ceiling was 10 accesses too tight |
| S99-04 (assertions.py) | | | an empty table has no column names; 'excludes' now holds on an empty list |
| expected/eval-small.json | orders, db | Service/orders, Service/db | compose services are indexed as Service/<name>; the two tier-2 'misses' in the first coverage report were this naming slip |
| N09-22, run_cases.py | | | fs steps may name their own root (the medium repo); an unexpected exception is now recorded as a failed step instead of ending the suite (N09-22..27 had not run) |
