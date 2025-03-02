#!/bin/sh
set -ex
IS_AARCH64=false
ndk_ver="r27c"
clang_ver="18"
openssl_ver="3.4.1"

if [ -z $ANDROID_API ]; then
	echo "Missing \$ANDROID_API environment variable" >&2
	exit 2
fi

case ${ARCH} in
	x86_64)
		;;
	aarch64)
		if [ "$ARCH" = aarch64 ]; then
			IS_AARCH64=true
		fi
		;;
	*)
		echo "Unknown \$ARCH '$ARCH'" >&2
		exit 2
esac

mkdir -p ./build/
cd ./build/

# TODO: figure out android ndk license
if [ ! -e android-ndk-"$ndk_ver" ]; then
	# unpack Android NDK
	curl -fsSLO https://dl.google.com/android/repository/android-ndk-"$ndk_ver"-linux.zip
	unzip android-ndk-"$ndk_ver"-linux.zip
	rm android-ndk-"$ndk_ver"-linux.zip
fi

export ANDROID_NDK_ROOT=$(pwd)/android-ndk-"$ndk_ver"

# add android-ndk bin path, required by openssl and rust
export PATH="$ANDROID_NDK_ROOT"/toolchains/llvm/prebuilt/linux-x86_64/bin:$PATH

mkdir -p "$ARCH"
cd "$ARCH"

if [ ! -e openssl-"$openssl_ver" ]; then
	# unpack openssl
	curl -fsSLO https://github.com/openssl/openssl/releases/download/openssl-"$openssl_ver"/openssl-"$openssl_ver".tar.gz
	tar -xf openssl-"$openssl_ver".tar.gz
	rm openssl-"$openssl_ver".tar.gz

	cd openssl-"$openssl_ver"

	# TODO: find robust verification to avoid "unnecessary" rebuilds
	if [ ! -e libssl.so ]; then
		openssl_arch="$ARCH"
		if [ "$ARCH" = aarch64 ]; then
			openssl_arch=arm64
		fi
		./Configure android-"$openssl_arch" -D__ANDROID_API__="$ANDROID_API"
		make
	fi

	cd ..
fi


cd ./../../

# link openssl statically
export OPENSSL_STATIC=1
export OPENSSL_LIB_DIR=$(pwd)/build/openssl-"$openssl_ver"/
export OPENSSL_INCLUDE_DIR=$(pwd)/build/openssl-"$openssl_ver"/include/

# add search paths of android dynamic libaries: log and unwind
export RUSTFLAGS="-L $ANDROID_NDK_ROOT/toolchains/llvm/prebuilt/linux-x86_64/lib/clang/$clang_ver/lib/linux/$ARCH/"
export RUSTFLAGS="$RUSTFLAGS -L $ANDROID_NDK_ROOT/toolchains/llvm/prebuilt/linux-x86_64/sysroot/usr/lib/${ARCH}-linux-android/$ANDROID_API"

# workaround: ensure rust uses the NDK linker
linker="$ANDROID_NDK_ROOT/toolchains/llvm/prebuilt/linux-x86_64/bin/$ARCH-linux-android$ANDROID_API-clang"

exec cargo build \
	--bin proot-cri \
	--target ${ARCH}-linux-android \
	--config target."$ARCH"-linux-android.linker=\""$linker"\" \
	$*

