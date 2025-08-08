# Changelog

## [0.1.6] - 2025-08-08
### Added
- Pagination for Athena `GetQueryResults` using `NextToken` to remove the 999-row cap
- New option: `Max Rows Returned` with modes `No Limit` and `Limit Applied`
- `Max Rows` numeric field (shown when `Limit Applied`) to stop after the configured number of rows

### Changed
- Result assembly now aggregates all pages; in Raw Format, `rowCount` reflects the number of rows returned

## [0.1.5] - 2025-07-08
### Fixed
- Updated error handling to use NodeOperationError and NodeApiError

## [0.1.2] - 2025-07-08
### Changed
- Modified node to remove AWS SDK Dependency (@aws-sdk/client-athena)

## [0.1.1] - 2025-07-08
### Added
- Allow SQL query field to use n8n expressions (dynamic data from previous nodes)

## [0.1.0] - 2024-06-15
### Added
- Initial release: AWS Athena Query node for n8n