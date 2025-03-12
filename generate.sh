#!/bin/bash

./jextract-22/bin/jextract --target-package kernel.generated --output /Users/oussama.saoudi/oxidized_java_kernel/src  @includes_filtered.txt ./delta-kernel-rs/target/ffi-headers/delta_kernel_ffi.h --library :./delta-kernel-rs/target/release

