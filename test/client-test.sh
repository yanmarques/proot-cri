#!/bin/sh
set -uo pipefail

counter=0

testit() {
	local name=$1
	local expected=$2
	local out=$(mktemp)

	shift
	shift

	crictl --runtime-endpoint=$(pwd)/proot.sock $* > "$out" 2>&1
	local retcode=$?

	if ([ $retcode -eq 0 ] && [ $expected -eq 1 ]) || ([ $retcode -ne 0 ] && [ $expected -eq 0 ]); then
		echo "[+] test no. $counter passed: $name"
	else
		echo "------------------------------ TEST NO. $counter OUTPUT   ----------------------------------" >&2
		cat "$out" >&2
		echo "--------------------------------------------------------------------------------------------" >&2
		echo "[-] test no. $counter failed: $name"
	fi

	rm "$out"
	counter=$(( counter + 1 ))
}

testit "pull image alpine from aws" 1 pull public.ecr.aws/docker/library/alpine
testit "pull image alpine from dockerhub" 1 pull alpine
testit "remove alpine image" 1 rmi public.ecr.aws/docker/library/alpine

testit "pull image with unsupported registry" 0 pull google.com/docker/library/alpine
testit "pull non-existant image" 0 pull public.ecr.aws/it/does/not-exists
testit "pull non-existant tag" 0 pull public.ecr.aws/docker/library/alpine:tag-does-not-exists

testit "create container" 1 create 123 ./test/container-config.json ./test/podsandbox-config.json

testit "list containers" 1 ps -a
