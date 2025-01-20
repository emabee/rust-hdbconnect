# How to Contribute to `hdbconnect`

## Tools

### Rust versions

Install 

- the **latest rust stable** (for normal compilation), 
- the **latest rust nightly** (for clippy, doc creation and doc tests),
  and the **minimal supported rust version**.

### cargo-script

Install [cargo-script](https://github.com/DanielKeep/cargo-script). You need this for running the qualification script `./scripts/qualify.rs` before submitting.

### cargo-outdated

Install [cargo-outdated](https://github.com/kbknapp/cargo-outdated). 
See also [installation and usage](https://github.com/kbknapp/cargo-outdated/blob/master/README.md#installation-and-usage).

### Mermaid support

Install a mermaid previewer for your ID, e.g. "Markdown Preview Mermaid Support" from Matt Bierner for Visual Studio Code.

The doc folder contains one and in future maybe more such diagrams.

## HANA

### Install a HANA database for tests

The tests should run ideally against a new HANA Cloud and a reasonably new HANA 2.

### Provide test configurations

Test configurations are expected in folder `./private`. This folder is git-ignored.

Within this folder, create a configuration file for each database.

Choose a name that follows the pattern `test_<discriminant>.db`, as e.g. `test_cloud.db` or `test_onprem.db`.

The file content should be a json document with the structure

```json
{
    "direct_url":"hdbsql://<host_url>:3<instance nr>15",
    "redirect_url":"hdbsql://<host_url>:3<instance nr>13?db=<dbname>",
    "std":{"name":"<USER1>","pw":"<user1_pw>"},
    "um":{"name":"<USER2>","pw":"<user1_pw>"}
}
```

where
- the placeholders with angular brackets must be replaced with correct values
- the direct URL will be used for most of the tests,
- the redirect URL can/should point to the same database, but via the redirect-syntax; it is used only in test_010_connect
- the std-user will be used for most of the tests, 
- the um-user for user-management activities (tests 11, 12, 13).

See https://docs.rs/hdbconnect/latest/hdbconnect/url/index.html for details of the URL format.

### Announce the chosen test configuration using the environment

Announce the test configuration you want to use for the test runs by using the 
environment variable `HDBCONNECT_TEST_DB`, e.g. use
`export HDBCONNECT_TEST_DB='cloud'` to test with the database you configured in 
`.private/test_cloud.db`.

## Dev Process

### Changes

Do changes locally and use a dedicated topic branch.

Follow the versioning rules described in [version policy](./doc/version_policy.md).

Extend `CHANGELOG.md`.

- collect changes in an `[a.b.c-unpublished]` section.
- **New version?** Update homogeneously the version entries in 
  - `./Cargo.toml`
  - `./hdbconnect/Cargo.toml`
  - `./hdbconnect_async/Cargo.toml`

For major or minor version increments, adapt `README.md` (i.e., not necessary for patch increments).

### Update dependencies

Ideally done with reasonable frequency and not together with development increments.

### Update MSVR

Should only be done when needed, and to the smallest possible value, in most cases after a update of the dependencies.

Update the respective version value in 

- ./hdbconnect/Cargo.toml
- ./hdbconnect_async/Cargo.toml
- ./hdbconnect_impl/Cargo.toml
- ./scripts/qualify.rs
- README.md

### Qualify

Run `./scripts/qualify.rs` and make sure that all checks pass. Do not submit if this is not given!

Ideally, do it twice with two different database backends (HANA2 and HANA Cloud).

### Submission

Check in and push.

Create a pull request and get it merged.

If it is a publishing change:
```cmd
cargo publish --package hdbconnect_impl
cargo publish --package hdbconnect
cargo publish --package hdbconnect_async
```