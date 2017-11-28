# Changelog

## [Unreleased] 

## [0.4.2] 
Added missing docu for new methods that were introduced in 0.4.1

## [0.4.1]
Corrected and extended the evaluation of ColumnOptions in resultset metadata and 
ParameterOptions in parameter metadata

## [0.4.0] 
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

