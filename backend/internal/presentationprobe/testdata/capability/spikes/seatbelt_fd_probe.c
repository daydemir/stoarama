#define _DARWIN_C_SOURCE
#include <sandbox.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int recvfd(int s) {
  char b=0; struct iovec i={&b,1}; char c[CMSG_SPACE(sizeof(int))];
  struct msghdr m={0}; m.msg_iov=&i; m.msg_iovlen=1; m.msg_control=c; m.msg_controllen=sizeof(c);
  if (recvmsg(s,&m,0)!=1) return -1;
  struct cmsghdr *h=CMSG_FIRSTHDR(&m); if(!h || h->cmsg_level!=SOL_SOCKET || h->cmsg_type!=SCM_RIGHTS) return -1;
  int fd=-1; memcpy(&fd,CMSG_DATA(h),sizeof(fd)); return fd;
}
static int sendfd(int s,int fd) {
  char b='x'; struct iovec i={&b,1}; char c[CMSG_SPACE(sizeof(int))]; memset(c,0,sizeof(c));
  struct msghdr m={0}; m.msg_iov=&i; m.msg_iovlen=1; m.msg_control=c; m.msg_controllen=sizeof(c);
  struct cmsghdr *h=CMSG_FIRSTHDR(&m); h->cmsg_level=SOL_SOCKET; h->cmsg_type=SCM_RIGHTS; h->cmsg_len=CMSG_LEN(sizeof(int));
  memcpy(CMSG_DATA(h),&fd,sizeof(fd)); return sendmsg(s,&m,0);
}
int main(void) {
  char outside[]="/tmp/seatbelt-outside.XXXXXX"; int ofd=mkstemp(outside); write(ofd,"outside",7); lseek(ofd,0,SEEK_SET);
  char media[]="/tmp/seatbelt-media.XXXXXX"; int mfd=mkstemp(media); write(mfd,"media",5); fsync(mfd); close(mfd); mfd=open(media,O_RDONLY); unlink(media);
  int sv[2]; socketpair(AF_UNIX,SOCK_DGRAM,0,sv); pid_t p=fork();
  if(p==0){ close(sv[0]); char *e=0;
    const char *profile="(version 1) (deny default) (allow file-read-metadata) (allow ipc-posix*) (allow mach-lookup)";
    int sr=sandbox_init(profile,0,&e); dprintf(2,"sandbox=%d errno=%d err=%s\n",sr,errno,e?e:"-"); if(e)sandbox_free_error(e);
    int got=recvfd(sv[1]); char b[16]={0}; errno=0; ssize_t rn=pread(got,b,5,0); dprintf(2,"media fd=%d read=%zd errno=%d bytes=%.*s seek=%lld/%d\n",got,rn,errno,(int)(rn>0?rn:0),b,(long long)lseek(got,0,SEEK_SET),errno);
    memset(b,0,sizeof(b)); errno=0; rn=pread(ofd,b,7,0); dprintf(2,"outside-open read=%zd errno=%d bytes=%.*s\n",rn,errno,(int)(rn>0?rn:0),b);
    errno=0; int nf=open(outside,O_RDONLY); dprintf(2,"outside-new fd=%d errno=%d\n",nf,errno);
    _exit(0);
  }
  close(sv[1]); usleep(100000); int se=sendfd(sv[0],mfd); fprintf(stderr,"send=%d errno=%d\n",se,errno); int st=0; waitpid(p,&st,0); close(mfd); close(ofd); unlink(outside); return WIFEXITED(st)?WEXITSTATUS(st):128+WTERMSIG(st);
}
