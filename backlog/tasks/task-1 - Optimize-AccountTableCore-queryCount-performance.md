---
id: TASK-1
title: Optimize AccountTableCore queryCount performance
status: Done
assignee:
  - '@agent'
created_date: '2026-10-21 02:44'
updated_date: '2026-10-21 02:54'
labels: []
dependencies: []
ordinal: 1000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
AccountTableCore currently inherits the default queryCount implementation from CollectionQueryInterface, which executes query_id(whereClause, whereValues) to fetch matching record IDs into a Java memory array before calling .length. As the account table grows large over time, querying count this way leads to significant memory spikes and latency bottlenecks. Overriding queryCount in AccountTableCore to delegate directly to accountDataObjectMap.queryCount enables native database count queries (e.g. SQL SELECT COUNT(*) / Mongo countDocuments), eliminating unnecessary object loading, reducing memory pressure, and dramatically improving performance for large datasets.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 AccountTableCore overrides queryCount(String whereClause, Object[] whereValues) and delegates directly to accountDataObjectMap.queryCount
- [x] #2 Unit tests in AccountTable_queryCount_test verify queryCount return values for both null and filtered clauses
- [x] #3 Existing AccountTable tests pass cleanly
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Override queryCount(String, Object[]) in AccountTableCore to delegate to accountDataObjectMap.queryCount.\n2. Add AccountTable_queryCount_test to test queryCount with null, empty, and filtered where clauses.\n3. Run tests in JavaCommons-module to verify fix.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented AccountTableCore queryCount delegation with empty whereClause normalization to null. Verified with AccountTable_queryCount_test.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Overrode queryCount in AccountTableCore to delegate directly to accountDataObjectMap.queryCount. Added unit tests in AccountTable_queryCount_test and verified all tests pass.
<!-- SECTION:FINAL_SUMMARY:END -->
