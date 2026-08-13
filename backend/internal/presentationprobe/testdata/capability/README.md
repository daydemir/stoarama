# C2 target-capability evidence

These files record a non-production, fail-closed capability assessment. They
are not probe artifacts, media reports, corpus proofs, or authorization to run
native code.

`darwin-arm64-25F84.json` applies only to macOS 26.5.2 build 25F84, Darwin
kernel 25.5.0, ARM64, and the exact contract digest in the record. The two C
spikes were compiled with Apple clang on that host and run manually. Their
ordered source digests and normalized observation digest are bound by the JSON
record and package tests. They established:

- custom Seatbelt default-deny initialization succeeds before fd handoff;
- an unlinked fd received later over SCM_RIGHTS remains seekable/readable;
- a new outside-root open fails with `EPERM`, but a pre-opened outside-root fd
  remains readable, so the required inherited-fd confinement is not proven;
- `RLIMIT_AS` and `RLIMIT_DATA` return `EINVAL`; immediate 256 MiB malloc and
  mmap bursts can both be touched, so polling cannot replace a hard limit.

The snippets are evidence inputs only. Tests never compile or execute them.
Future native work still requires collision-safe relocation of both received
fds to distinct `CLOEXEC` descriptors numbered at least 8 before installing
the fixed evidence map at fds 3 and 4.
