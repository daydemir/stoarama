"""Private R2 delivery of precisely the two human-approved derived joins."""
import json,re
from pathlib import Path
import coverage_downloads as delivery
INDEX=Path('/Users/jelly/.stoarama/seam-review/private/jaworzno-reviewed-repairs-20261009.json')
BASE='/api/reviewed-repairs/file/'
def route(h,u,q,head=False):
 ticket_base=BASE.replace('/file/','/ticket/')
 is_ticket=u.path.startswith(ticket_base)
 if not u.path.startswith(BASE) and not is_ticket:return False
 key=u.path[len(ticket_base if is_ticket else BASE):]
 if not re.fullmatch('[0-9a-f]{64}',key):return delivery.send(h,404,{'error':'Unknown reviewed repair'},head=head)
 try:
  o=next((o for o in json.loads(INDEX.read_text()) if o['sha256']==key),None)
  if not o:return delivery.send(h,404,{'error':'Unknown reviewed repair'},head=head)
  return delivery.ticket(h,o,q,head) if is_ticket else delivery.file(h,o,q,head)
 except Exception:return delivery.send(h,503,{'error':'Reviewed repair unavailable; originals preserved'},head=head)
def get(h,u,q):return route(h,u,q)
def head(h,u,q):return route(h,u,q,True)
