setlocal
REM *** XEON cpu
set RUSTFLAGS=-C target-cpu=cascadelake -C target-feature=+crt-static --emit asm
cargo +nightly build --target x86_64-win7-windows-msvc -Z build-std="panic_abort,std" -Z build-std-features="panic_immediate_abort" --release
endlocal