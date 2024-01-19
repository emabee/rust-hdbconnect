#!/usr/bin/env rust-script
//! ```cargo
//! [dependencies]
//! yansi = "0.5"
//! ```
extern crate yansi;
use std::{process::Command, time::Instant};

fn main() {
    let mut simulate = false;
    let mut run_tests = true;

    for arg in std::env::args() {
        if &arg == "--simulate" {
            simulate = true;
        }
        if &arg == "--no-test" {
            run_tests = false;
        }
    }

    macro_rules! run_command {
        ($cmd:expr) => {
            let mut command = command!($cmd);
            if simulate {
            } else {
                let mut child = command.spawn().unwrap();
                let status = child.wait().unwrap();
                if !status.success() {
                    print!("> {}", yansi::Paint::red("qualify terminates due to error"));
                    std::process::exit(-1);
                }
            }
        };
    }

    macro_rules! command {
        ($cmd:expr) => {{
            if simulate {
                println!("{}", yansi::Paint::red($cmd));
            } else {
                print!("\n> {}\n", yansi::Paint::yellow($cmd));
            }
            let mut chips = $cmd.split(' ');
            let mut command = Command::new(chips.next().unwrap());
            for chip in chips {
                command.arg(chip);
            }
            command
        }};
    }

    if simulate {
        println!("Qualify hdbconnect  -- SIMULATION --");
    } else {
        println!("Qualify hdbconnect");
    }

    // Format
    run_command!("cargo fmt");

    // Build in important variants
    run_command!("cargo build --package hdbconnect");
    run_command!("cargo build --package hdbconnect --all-features");
    run_command!("cargo build --package hdbconnect_async");
    run_command!("cargo build --package hdbconnect_async --all-features");

    run_command!("cargo build --package hdbconnect --all-features --release");
    run_command!("cargo build --package hdbconnect_async --all-features --release");

    // Clippy in important variants
    run_command!(
        "cargo +nightly clippy --all-targets --package hdbconnect \
                  --all-features -- -D warnings"
    );
    run_command!(
        "cargo +nightly clippy --all-targets --package hdbconnect_async \
                  --all-features -- -D warnings"
    );

    // doc
    run_command!("cargo +nightly doc --package hdbconnect --all-features --no-deps --open");
    run_command!("cargo +nightly doc --package hdbconnect_async --all-features --no-deps --open");

    if run_tests {
        // doc-tests
        run_command!("cargo +nightly test --doc --all-features --package hdbconnect");
        run_command!("cargo +nightly test --doc --all-features --package hdbconnect_async");

        // Run tests in important variants
        let start = Instant::now();
        run_command!("cargo test --package hdbconnect --release --all-features");
        run_command!("cargo test --package hdbconnect_async --release --all-features");
        run_command!("cargo test --package hdbconnect");
        run_command!("cargo test --package hdbconnect_async");

        if !simulate {
            println!(
                "Four test runs together took {:?}",
                Instant::now().duration_since(start)
            );
        }

        // check version consistency
        run_command!("cargo run --package hdbconnect --example version_numbers");
        run_command!("cargo run --package hdbconnect_async --example version_numbers");

        // check git status
        if !simulate {
            let mut cmd = command!("git status -s");
            let child = cmd.stdout(std::process::Stdio::piped()).spawn().unwrap();
            let output = child.wait_with_output().unwrap();
            if output.stdout.len() > 0 {
                print!("> {}", yansi::Paint::red("there are unsubmitted files"));
                std::process::exit(-1);
            }

            println!(
                "\n> all done 😀  Looks like you're ready to \n\
               cargo publish --package hdbconnect_impl\n\
               cargo publish --package hdbconnect\n\
               cargo publish --package hdbconnect_async\n\
               ?"
            );
        }
    }
}
