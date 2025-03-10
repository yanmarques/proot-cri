#!/bin/sh
set -ex

export CC=/home/user/opt/android-ndk-r27c/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android28-clang
export CXX=/home/user/opt/android-ndk-r27c/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android28-clang++
export AR=/home/user/opt/android-ndk-r27c/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar

build_talloc() (
	cd ./build/samba/lib/talloc/
	cat <<EOF > cross-answers.txt
Checking uname sysname type: "Linux"
Checking uname machine type: "dontcare"
Checking uname release type: "dontcare"
Checking uname version type: "dontcare"
Checking simple C program: OK
rpath library support: OK
-Wl,--version-script support: FAIL
Checking getconf LFS_CFLAGS: OK
Checking for large file support without additional flags: OK
Checking for -D_FILE_OFFSET_BITS=64: OK
Checking for -D_LARGE_FILES: OK
Checking correct behavior of strtoll: OK
Checking for working strptime: OK
Checking for C99 vsnprintf: OK
Checking for HAVE_SHARED_MMAP: OK
Checking for HAVE_MREMAP: OK
Checking for HAVE_INCOHERENT_MMAP: OK
Checking for HAVE_SECURE_MKSTEMP: OK
Checking getconf large file support flags work: OK
Checking for HAVE_IFACE_IFCONF: FAIL
EOF

	./configure build --disable-rpath --disable-python --cross-compile --cross-answers ./cross-answers.txt
	mkdir -p lib
	$AR rcs "lib/libtalloc.a" bin/default/lib/talloc/talloc.*.o
)

build_proot() (
	cd ./build/proot/src/
	export CFLAGS="-I../../samba/lib/talloc/include" 
	export LDFLAGS="-L../../samba/lib/talloc/lib -static"
	make
)

build_talloc
build_proot
