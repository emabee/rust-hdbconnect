# Changelog

## [0.23.1] 2021-10-14

Improve test setup.

Fix authentication two byte length encoding.

Add support for time crate (serde)

- switch to serde_db 0.10
- remove dependency to chrono (chrono support for serde remains)

Implement implicit redirect (the server can trigger a redirect in case that e.g. an SMVR failover
has taken place). IS NOT YET TESTED!

## [0.23.0] 2020-12-29

Add redirect support: target database can be specified by name.

Some minor API smoothing (-> version bump):

- Connection::`get_database_name()`, `get_system_id()`, and `get_full_version_string()`,
  return `HdbResult<String>`, rather than `HdbResult<Option<String>>`
- ConnectParamsBuilder
  - `get_password` returns a `SecUtf8` instead of a `SecStr`
  - `get_options` is removed

## [0.22.2] 2020-11-22

Add support for array-type.

## [0.22.1] 2020-11-13

Minor improvements, code maintenance.

## [0.22.0] 2020-10-16

### API changes

Revise handling of ResultSetMetadata to make it much more ergonomic (and idiomatic).

- ResulSset now provides direct access to a `Vec<FieldMetadata>`, and
`FieldMetadata` exposes the metadata of each field.
- The type `ResultSetMetadata` with its awkward API is gone (-> version bump).

Update dependencies.

## [0.21.4] 2020-08-19

### Fixes and Improvements

Fix issue with empty LOBs being read from the database (introduced with 0.21.3).

Update dependencies.

## [0.21.3] 2020-05-18

### Fixes and Improvements

Implement automatic reconnect (only works within the server's reconnect-wait-timeout).

Make `HdbError` thread-safe (again) - Kudos to Mark Obad!

Minor  performance improvement in LOB implementation.

## [0.21.2] 2020-05-04

### Fixes and Improvements

Add method `NCLob::total_char_length`, improve docu.

## [0.21.1] 2020-04-21

### Fixes and Improvements

Add support for CONNECT (switch user).

## [0.21.0] 2020-04-08

### API changes

Simplify API of `OutputParameters`.

### Fixes and Improvements

Remove direct dependency to `serde_derive`.

Add test for using HANA's management console.

## [0.20.5] 2020-04-02

### Fixes and Improvements

Improve ease-of-use for lob objects by letting lob objects keep their result set core object
and their prepared statement core object alive until they are completely loaded.

## [0.20.4] 2020-03-02

### API extension

Add `Connection::client_info`.

## [0.20.3] 2020-02-28

Add value conversions from Strings to number types (using `std::fmt::Display`)
and from number types to Strings (using `parse()`).

## [0.20.2] 2020-02-25

### Fixes and Improvements

Change serialization format of ConnectParamsBuilder to URL; the serialization to url
includes all information except password.

Some code maintenance.

## [0.20.1] 2020-02-18

Add variant `ServerCerts::None` to use TLS without server validation.

## [0.20.0] 2020-01-31

### API changes

Replace &String with &str in return value types.

### Fixes and Improvements

Make `ResultsSet` thread-safe.

Add `IntoConnectParamsBuilder` and implementations for `&str` and `String` and `Url`.

## [0.19.0] 2020-01-16

### API changes

Make `Connection::new()` and `ConnectionManager::new()` more flexible by
taking a `P: IntoConnectParams` rather than a `ConnectParams`.

Improve `ConnectParamsBuilder`:

- move root-certificates option as an additional variant into ServerCerts enum
- don't erase any information from the `ConnectParamsBuilder` during build()

Make feature "tls" permanent - so it disappears from the list of cargo features!

### Fixes and Improvements

Remove redundant `ConnectParams` in `Connection` (it is in AmConnCore anyway).

Remove wrong debug asserts from lob_writer (adding to the fix for [issue 23]).

Implement `Display`, and derive `Debug`, for `ConnectParams`.

Fix pedantic clippies, and some docu.

## [0.18.0] 2019-12-10

Revise HdbResponse:

- remove public visibility to its elements (-> version bump)
- remove parameter descriptors

Simplify handling of ParameterDescriptors in PreparedStatement.

Make `ParameterDescriptors` public.

Support database procedures with LOB input parameters (fix [issue 23]).

## [0.17.0] 2019-11-21

Bugfix: Let `ResultSet`s that were produced from a `PreparedStatement` keep
the `PreparedStatement`'s Core alive.

Add option to use Mozillas root certificates.

Version bump due to incompatible API changes caused by

- Avoid cloning of metadata
- Use inner mutability to avoid mut for ResultSet iterations
- Revise handling of server usage information

## [0.16.0-actix-compatibility] 2019-10-29

Stick to older version of rustls (0.15.1) and webpki (0.19.1) to stay compatible with actix.

## [0.16.0] 2019-10-28

Add convenience method Connection::prepare_and_execute().

Incompatible: revise API of OutputParameters:
allow iterating and using deserialization with `serde_db`, revise method names.

Incompatible: Avoid cloning in accessors of ServerError.

## [0.15.2] 2019-03-05

Add possibility to set the lob-write-length.

Increase default values for fetch-size and lob-read-length.

## [0.15.1] 2019-02-25

Fix broken TLS feature.

## [0.15.0] 2019-02-25

Change `HdbValue::LOBSTREAM` to `LOBSTREAM(Option<Arc<Mutex<Read + Send>>>)`.

Let PreparedStatement::execute_batch() work with empty batch if no input pars are required.

## [0.14.2] 2019-02-21

Add HdbValue::STR(&str), and allow LOB streaming to the database with HdbValue::LOBSTREAM.

Add support for database columns of types ALPHANUM and BINTEXT.

## [0.14.1] 2019-02-08

Improve HdbValue, TypedId, and handling of parameters in PreparedStatement.
Add method PreparedStatement.execute_row().

## [0.13.0] 2019-01-30

- revise names of some metadata methods
- Reduce complexity of HdbValue: use a single explicit NULL variant
- Also simplify type id handling
- Implement geo-types
- Migrate to serde_db 0.7
- Apply changes to ResultSet
  - remove methods pop_row(), reverse_rows(),
  - add method next_row()
  - make ResultSet an Iterator for item type HdbResult\<Row\>
- Apply changes to Row
  - remove methods iter(), pop(), pop_into(), field_into(), field_into_option(), field_into_nclob(),
    field_into_clob(), field_into_blob(), reverse_values()
  - add method next_value()
  - add internal method number_of_fields()
  - make Row and iterator with item type HdbValue

## [0.12.2]  2019-01-16

Remove unneccessary allocations.
Version sync re-added.

## [0.12.1]  2019-01-14

Code maintenance.

(Non-functional: temporarily remove version-sync.)

## [0.12.0]  2018-12-14

Expose Connection.get_id().

Code maintenance.

Remove usage of #[doc(hidden)] -> version bump.

## [0.11.1]  2018-12-14

Move to edition 2018.
Support serialization of byte arrays containing valid UTF-8 into NCLOB.
Remove unneccessary String allocations and make connection API more flexible.

## [0.11.0]  2018-12-11

Change handling of type ids  (-> version bump).
Fix parameter serialization.
Fix some new clippies.

Code maintenance: make all factory submodules private.

## [0.10.0]  2018-12-07

Changes to PreparedStatement:

- add method add_row_to_batch()
- remove method set_auto_commit() (-> version bump)

HdbValue now implements Serialize.

## [0.9.1]  2018-10-29

Switch from deprecated to new hashing libs.

## [0.9]  2018-10-12

Add a way to provide the server certificate for tls directly, and refactor the respective
API of ConnectParamsBuilder (-> version bump).

## [0.8.1]  2018-10-01

Fix tls and expose as a documented feature.

## [0.8.0]  2018-10-01

Improve client_info handling (-> API change -> Version bump).

Implement ReplyType Explain.

## [0.7.4]  2018-09-28

Some improvements around parsing date and time values.

Update to serde_db 0.5.

## [0.7.3]  2018-09-21

Add implementation for NCLOB.

## [0.7.2]  2018-09-20

Add implementation for PartitionInformation.

Use cesu8 crate rather than internalized outdated clone of its code.

Update dependent libs.

## [0.7.1]  2018-09-13

Add missing HANA data types: SECONDTIME, DAYDATE, SECONDDATE, SMALLDECIMAL.

## [0.7.0]  2018-09-08

Add feature "alpha_tls" - implementation pretty complete, but completely untested.

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

[issue 23]: (https://github.com/emabee/rust-hdbconnect/issues/23)
