//go:build darwin && cgo

package loadstand

/*
#include <sys/types.h>
#include <sys/acl.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <errno.h>

// Darwin returns 0 for an entry, -1/EINVAL at the end of a valid ACL.
// Validation + a native cursor over this immutable copy prevents confusing an
// invalid ACL with an empty one. The native header bounds the entry count.
static int corsa_final_acl(int fd) {
 // Query explicit descriptor security-property presence: acl_get_fd_np alone
 // reports ENOENT for no ACL, which must not be confused with arbitrary errors.
 filesec_t security=filesec_init();
 if (security==NULL) return errno ? errno : EIO;
 struct stat statbuf;
 if (fstatx_np(fd,&statbuf,security)!=0) { int error=errno ? errno : EIO; filesec_free(security); return error; }
 int present=0;
 if (filesec_query_property(security,FILESEC_ACL,&present)!=0) { int error=errno ? errno : EIO; filesec_free(security); return error; }
 if (!present) { filesec_free(security); return 0; }
 acl_t acl=NULL;
 if (filesec_get_property(security,FILESEC_ACL,&acl)!=0 || acl==NULL) { int error=errno ? errno : EIO; filesec_free(security); return error; }
 filesec_free(security);
 int result = 0;
 if (acl_valid(acl) != 0) { result = errno ? errno : EIO; goto done; }
 int cursor = ACL_FIRST_ENTRY;
 for (int i=0; i<=ACL_MAX_ENTRIES; i++) {
  acl_entry_t entry;
  errno=0;
  if (acl_get_entry(acl,cursor,&entry) != 0) {
   result = errno == EINVAL ? 0 : (errno ? errno : EIO);
   goto done;
  }
  acl_tag_t tag;
  if (acl_get_tag_type(entry,&tag)!=0) { result=errno ? errno : EIO; goto done; }
  if (tag != ACL_EXTENDED_DENY) { result=EACCES; goto done; }
  cursor=ACL_NEXT_ENTRY;
 }
 result=EOVERFLOW;
done:
 if (acl_free(acl)!=0 && result==0) result=EIO;
 return result;
}
*/
import "C"

import "os"

func auditFinalACL(file *os.File) error {
	if nativeFinalACL(file) != 0 {
		return ErrUnsupportedArtifact
	}
	return nil
}

func nativeFinalACL(file *os.File) int { return int(C.corsa_final_acl(C.int(file.Fd()))) }
