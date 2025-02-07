#!/bin/sh

export PATH=~/opt/android-ndk-r27c/toolchains/llvm/prebuilt/linux-x86_64/bin:$PATH
export OPENSSL_STATIC=1
export OPENSSL_LIB_DIR=$(pwd)/build/openssl/
export OPENSSL_INCLUDE_DIR=$(pwd)/build/openssl/include/
export RUSTFLAGS="-L /home/user/opt/android-ndk-r27c/toolchains/llvm/prebuilt/linux-x86_64/lib/clang/17/lib/linux/x86_64/ -L /home/user/opt/android-ndk-r27c/toolchains/llvm/prebuilt/linux-x86_64/sysroot/usr/lib/x86_64-linux-android/24"
exec cargo build --bin android-proot-cri --target x86_64-linux-android
