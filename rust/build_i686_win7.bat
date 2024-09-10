cargo +nightly build --target i686-win7-windows-msvc -Z build-std="panic_abort,std" -Z build-std-features="panic_immediate_abort" --release
