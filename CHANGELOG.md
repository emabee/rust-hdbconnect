# Changelog

## [0.7.3]  2018-09-21

Add implementation for NCLOB.

## [0.7.2]  2018-09-20

Add implementation for PartitionInformation.

Use cesu8 crate rather than internalized outdated clone of its code.

Update dependent libs.

## [0.7.1]  2018-09-13

Add missing HANA data types: SECONDTIME, DAYDATE, SECONDDATE, SMALLDECIMAL.

## [0.7.0]  2018-09-08

Add feature "tls" - implementation pretty complete, but completely untested.

## [0.6.0]  2018-08-11

Minor internal updates.

## [0.5.2 (was meant to be released as 0.6.0)]  2018-08-10

Replace usage of rust_decimal with BigDecimal and fix insufficient decimal handling,
remove HdbDecimal from API (reason for version bump).

Add support for new authentication variant

Add support for client info (= session context)

Fix key words in Cargo.toml

Prepare for ensuring the buffer is empty after parsing a reply

Improve handling of connect_options

Handle clientlocale through conn_params

Introduce SecStr for password handling

## [0.5.1] 2018-07-25

Update to rustc 127.1, and to current versions of used libs

Fix topology part implementation (did not work with sacale-out topology)

## [0.5.0] 2018-07-20

Revise error handling (-> version bump)

Fix issue with large queries

Fix incorrect row order

## [0.4.10] 2018-03-29

Add handling for warnings from the server

Implement Drop for ResultSetCore

## [0.4.9] 2018-02-28

Minor improvements around XA

## [0.4.7] 2018-02-04

Implement some missing option parts

Update to dist_tx 0.2

## [0.4.6] 2018-01-29

Support distributed transactions

## [0.4.5] 2017-12-07

Support  Deserialization for DECIMAL types

## [0.4.4] 2017-12-03

Support "select for update"

## [0.4.3] 2017-12-01

Update to serde_db 0.4

## [0.4.2] 2017-11-28

Added missing docu for new methods that were introduced in 0.4.1

## [0.4.1] 2017-11-28

Correct and extend the evaluation of ColumnOptions in resultset metadata and
ParameterOptions in parameter metadata

## [0.4.0] 2017-11-26

Add access methods for metadata.

## [0.3.2] 2017-11-17

Add support for stringified value representations.

## [0.3.1] 2017-11-17

Add support for HANA's DECIMAL types.

## [0.3.0] 2017-11-12

Revise the LOB handling, add streaming support for CLOBs and BLOBs.
Replace `Row::field_as()` with `Row::field_into()` to allow field-wise access without cloning.

## [0.2.0] 2017-11-01

Extract the serde-usage into a separate crate (serde_db).
