"""Freeze exact local test inputs and retained logs as data; no command execution."""
from pathlib import Path
import hashlib,io,json,tarfile
HERE=Path(__file__).resolve().parent
REPO=HERE.parents[3]
SUBJECT=REPO/'docs/_internal/d1-artifacts/2026-11-20/d1-o1a-outbound-admission-fix-v1'
RED=Path('/private/tmp/corsa-d1-o1a-outbound-admission-unit-2de968d3c4c5429f82895d71ef9a31ec')
GREEN=Path('/private/tmp/corsa-d1-o1a-outbound-admission-green-race-6830e1fcbb964718b41e8fe2af844864')
RAW,SOURCE,ORIGINS={},{},{}
def sha(b):return hashlib.sha256(b).hexdigest()
def add(table,name,p):
 p=Path(p);assert p.is_file() and not p.is_symlink() and p.stat().st_size<8*1024**2
 b=p.read_bytes();table[name]=b;ORIGINS[('RAW/' if table is RAW else 'SOURCE/')+name]={'path':str(p),'bytes':len(b),'sha256':sha(b)}
def collect():
 for label,w in [('original-red',RED),('corrected-green-race',GREEN)]:
  r=json.loads((w/'RESULT.json').read_bytes());add(SOURCE,label+'/RESULT.json',w/'RESULT.json')
  for s in r['steps']:
   for stream in ['stdout','stderr']:add(RAW,label+'/'+Path(s[stream]).name,s[stream])
  for name in ['SOURCE-BEFORE.json','SOURCE-AFTER.json','overlay.json','red-overlay.json','ROOT-DATA.json','REVIEWER-DATA.json','ARCHITECT-DATA.json']:
   if (w/name).exists():add(SOURCE,label+'/'+name,w/name)
 for q in sorted(SUBJECT.rglob('*')):
  if q.is_file() and '__pycache__' not in q.parts:add(SOURCE,'subject/'+q.relative_to(SUBJECT).as_posix(),q)
 for name in ['corsa-outbound-admission-unit.py','corsa-outbound-admission-green-race.py']:add(SOURCE,'controllers/'+name,Path('/private/tmp')/name)
 add(SOURCE,'architect/REVIEW.json',REPO/'docs/_internal/d1-artifacts/2026-11-20/d1-o1a-outbound-admission-architect-data-v1/REVIEW.json')
def write(name,obj):(HERE/name).write_text(json.dumps(obj,sort_keys=True,indent=2)+'\n')
def pack(name,rows):
 b=io.BytesIO()
 with tarfile.open(fileobj=b,mode='w:xz',format=tarfile.USTAR_FORMAT) as t:
  for n,v in sorted(rows.items()):
   m=tarfile.TarInfo(n);m.size=len(v);m.mode=0o644;m.uid=m.gid=m.mtime=0;m.uname=m.gname='';t.addfile(m,io.BytesIO(v))
 (HERE/(name+'.tar.xz')).write_bytes(b.getvalue());write(name+'-MEMBERS.json',{n:{'bytes':len(v),'sha256':sha(v)} for n,v in sorted(rows.items())})
def main():
 collect();r=json.loads((GREEN/'RESULT.json').read_bytes());assert r['status']=='OUTBOUND_ADMISSION_RED_GREEN_RACE_PASS' and r['error'] is None and r['unknown'] is False
 write('SUMMARY.json',{'scope':'OUTBOUND_PRE_ADMISSION_ACCOUNTING_ONLY_NOT_NATIVE_CAUSE_PROOF','tested_base_commit':json.loads((SUBJECT/'MANIFEST.json').read_bytes())['base_commit'],'manifest_sha256':sha((SUBJECT/'MANIFEST.json').read_bytes()),'ordinary':r['tests']['ordinary'],'race':r['tests']['race'],'original_RED':'Five assertion failures on old code; wrapper originally expected four and stopped. Its failure is preserved; only expectation diagnosis changed before two green runs. No identical RED repeat.','guards':'Inbound sync refusal, partial admission, admitted queue drain, clock/window guards, actual byte/digest and reciprocal proof remain strict.','native_2c751_sender_return_site':'UNKNOWN','Linux_ELF_or_app_executed':False,'push':False})
 write('ORIGINS.json',ORIGINS);pack('RAW',RAW);pack('SOURCE',SOURCE)
 files=sorted(p for p in HERE.iterdir() if p.is_file() and p.name!='SHA256SUMS');(HERE/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+p.name+'\n' for p in files));print(json.dumps({'files':len(files)+1,'RAW':len(RAW),'SOURCE':len(SOURCE),'SUMS':sha((HERE/'SHA256SUMS').read_bytes())}))
if __name__=='__main__':main()
