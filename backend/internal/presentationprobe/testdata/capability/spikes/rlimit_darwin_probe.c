#include <sys/resource.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
int main(void){
 struct rlimit r={128ULL<<20,128ULL<<20}; int a=setrlimit(RLIMIT_AS,&r); fprintf(stderr,"as=%d errno=%d %s\n",a,errno,strerror(errno));
 struct rlimit d={96ULL<<20,96ULL<<20}; errno=0; int x=setrlimit(RLIMIT_DATA,&d); fprintf(stderr,"data=%d errno=%d %s\n",x,errno,strerror(errno));
 errno=0; void *p=malloc(256ULL<<20); fprintf(stderr,"malloc=%p errno=%d %s\n",p,errno,strerror(errno)); if(p){memset(p,1,256ULL<<20); fprintf(stderr,"touch survived\n");}
 errno=0; void *q=mmap(0,256ULL<<20,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANON,-1,0); fprintf(stderr,"mmap=%p errno=%d %s\n",q,errno,strerror(errno)); if(q!=MAP_FAILED){memset(q,2,256ULL<<20); fprintf(stderr,"mmap touch survived\n");}
 return p||q!=MAP_FAILED;
}
