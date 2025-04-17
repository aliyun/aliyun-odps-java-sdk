#!/bin/bash

set -eu
base_dir=$(pushd $(dirname $0) >/dev/null && pwd -P && popd >/dev/null)

pb_version=$(basename $0)
pb_version=${pb_version/protoc-/}
pb_version=${pb_version/.sh/}
protoc_dir=$base_dir/$(uname)/$pb_version
protoc=$protoc_dir/protoc
export LD_LIBRARY_PATH=$protoc_dir
export DYLD_LIBRARY_PATH=$protoc_dir

$protoc "$@"
