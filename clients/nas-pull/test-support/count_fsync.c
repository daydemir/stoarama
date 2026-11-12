/* Test-only syscall interposer: counts real SQLite and Python fsync calls.
 * macOS: cc -dynamiclib -o /tmp/count_fsync.dylib count_fsync.c
 * Linux: cc -shared -fPIC -o /tmp/count_fsync.so count_fsync.c -ldl
 * Preload into the benchmark interpreter, never a production client.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdatomic.h>
#include <time.h>
#include <unistd.h>

static _Atomic long count;
static _Atomic long delay_ns;
static _Atomic int active;

void cleanup_sync_begin(long nanoseconds) {
    atomic_store(&count, 0);
    atomic_store(&delay_ns, nanoseconds);
    atomic_store(&active, 1);
}
long cleanup_sync_end(void) {
    atomic_store(&active, 0);
    return atomic_load(&count);
}
static int measured_fsync(int fd) {
#ifndef __APPLE__
    int (*real_sync)(int) = dlsym(RTLD_NEXT, "fsync");
#endif
    if (atomic_load(&active)) {
        atomic_fetch_add(&count, 1);
        struct timespec delay = {.tv_nsec = atomic_load(&delay_ns)};
        nanosleep(&delay, 0);
    }
#ifdef __APPLE__
    return fsync(fd); /* dyld does not interpose calls from this image. */
#else
    return real_sync(fd);
#endif
}
#ifdef __APPLE__
__attribute__((used)) static struct {const void *replacement, *original;} interpose
    __attribute__((section("__DATA,__interpose"))) = {(void *)measured_fsync, (void *)fsync};
#else
int fsync(int fd) { return measured_fsync(fd); }
int fdatasync(int fd) {
    int (*real_sync)(int) = dlsym(RTLD_NEXT, "fdatasync");
    if (atomic_load(&active)) {
        atomic_fetch_add(&count, 1);
        struct timespec delay = {.tv_nsec = atomic_load(&delay_ns)};
        nanosleep(&delay, 0);
    }
    return real_sync(fd);
}
#endif
