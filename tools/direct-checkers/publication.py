#!/usr/bin/env python3
"""Safe parented connector publication of the three verified public snapshot feeds.

No network or remote write occurs without an explicitly supplied connector backend.
GitHub's available connector cannot create orphan commits or enforce force-with-lease.
A parent bound to the observed head plus force=False supplies non-fast-forward race
protection while preserving immutable snapshot trees and existing consumer branches.
"""
from __future__ import annotations
import argparse,datetime as dt,hashlib,json,pathlib,re
from dataclasses import dataclass
from runner import content_hash,now,parse,read,source_health

REPOSITORY='Happypig375/actions-experiment'
FEEDS={
 'github':{'branch':'chatgpt-important-update-feed','paths':('index.json','views/nu.json','views/angourimath.json')},
 'tibo':{'branch':'chatgpt-important-update-tibo-feed','paths':('index.json','views/tibo.json')},
 'media':{'branch':'chatgpt-important-update-reddit-media-feed','paths':('index.json','views/reddit_media.json')},
}
SHA=re.compile(r'^[0-9a-f]{40}$')
FORBIDDEN_KEYS={'access_token','refresh_token','authorization','cookie','cookies','password','api_key','apikey','credentials','oauth_state','account_id','fivehour','weekly','ratelimits'}
class PublishBlocked(RuntimeError):pass
class RefRejected(RuntimeError):pass
class RefUncertain(RuntimeError):pass

def _scan(value):
 if isinstance(value,dict):
  for k,v in value.items():
   if k.lower()=='private' and v is True:raise PublishBlocked('Explicit private content forbidden in public snapshot')
   if k.lower() in FORBIDDEN_KEYS:raise PublishBlocked('Private/authentication field forbidden in public snapshot: '+k)
   _scan(v)
 elif isinstance(value,list):
  for item in value:_scan(item)

def _sha(value):
 if not isinstance(value,str) or not SHA.fullmatch(value):raise PublishBlocked('Invalid immutable Git SHA')
 return value

@dataclass(frozen=True)
class Candidate:
 key:str
 branch:str
 generated_at:str
 snapshot_sha256:str
 files:dict[str,str]
 def tree_elements(self):return [{'path':p,'mode':'100644','type':'blob','content':self.files[p]} for p in sorted(self.files)]

def validate_candidate(candidate:Candidate):
 if candidate.key not in FEEDS or candidate.branch!=FEEDS[candidate.key]['branch']:raise PublishBlocked('Unapproved feed or branch')
 if set(candidate.files)!=set(FEEDS[candidate.key]['paths']):raise PublishBlocked('Unexpected or missing public snapshot file')
 stamp=parse(candidate.generated_at)
 if stamp>now()+dt.timedelta(minutes=5):raise PublishBlocked('Future snapshot')
 if (now()-stamp).total_seconds()>4500:raise PublishBlocked('Stale snapshot; reacquire before publication')
 docs={}
 for path,content in candidate.files.items():
  if not isinstance(content,str) or len(content.encode())>1024*1024:raise PublishBlocked('Invalid or oversized JSON view')
  doc=json.loads(content);_scan(doc);docs[path]=doc
  if doc.get('generated_at')!=candidate.generated_at:raise PublishBlocked('Mixed snapshot timestamps')
 if docs['index.json'].get('fresh_for_seconds')!=4500:raise PublishBlocked('Unexpected freshness contract')
 if candidate.key=='github':
  for key,repo in [('nu','bryanedds/Nu'),('angourimath','asc-community/AngouriMath')]:
   doc=docs['views/'+key+'.json']
   if doc.get('repo')!=repo or not doc.get('head_sha') or doc.get('errors'):raise PublishBlocked('GitHub source is incomplete or outside approved public scope')
  rows=docs['index.json'].get('repos',[])
  expected={('nu','bryanedds/Nu','views/nu.json'),('angourimath','asc-community/AngouriMath','views/angourimath.json')}
  if len(rows)!=2 or {(r.get('key'),r.get('full_name'),r.get('view_path')) for r in rows}!=expected:raise PublishBlocked('GitHub source index mismatch')
  if any(r.get('errors') or r.get('head_sha')!=docs[r['view_path']].get('head_sha') for r in rows):raise PublishBlocked('GitHub source index incomplete or inconsistent')
 elif candidate.key=='tibo':
  doc=docs['views/tibo.json']
  if doc.get('source')!='@thsottiaux' or not doc.get('candidate_discovery_only'):raise PublishBlocked('Tibo source identity/privacy boundary mismatch')
  source=docs['index.json'].get('source',{})
  if (source.get('key'),source.get('name'),source.get('view_path'))!=('tibo','@thsottiaux','views/tibo.json'):raise PublishBlocked('Tibo source index mismatch')
  if source.get('status')!='ok':raise PublishBlocked('Unhealthy Tibo source')
 elif candidate.key=='media':
  doc=docs['views/reddit_media.json']
  if doc.get('source')!='r/codex media' or not doc.get('candidate_discovery_only') or not doc.get('ocr_is_untrusted'):raise PublishBlocked('Reddit OCR source identity/privacy boundary mismatch')
  index=docs['index.json']
  if index.get('view_path')!='views/reddit_media.json' or index.get('status') not in ('ok','empty') or index.get('errors'):raise PublishBlocked('Reddit OCR source index mismatch or unhealthy')
  if doc.get('errors') or any(i.get('ocr_errors') for i in doc.get('items',[])):raise PublishBlocked('Incomplete Reddit OCR source')
 return candidate

def prepare_candidate(state:pathlib.Path,key:str):
 if key not in FEEDS:raise PublishBlocked('Only verified GitHub, Tibo and media outputs may be published')
 pointer=read(state/'pointers'/f'{key}.json')
 if not pointer or pointer.get('mode') not in ('live-anonymous','live-connector-and-public'):raise PublishBlocked('No live verified snapshot; fixtures and imported snapshots cannot be published')
 snapshot=(state/pointer['snapshot']).resolve()
 if not snapshot.is_relative_to(state.resolve()/'snapshots'/key):raise PublishBlocked('Snapshot outside expected state scope')
 if any(p.is_symlink() for p in snapshot.rglob('*')):raise PublishBlocked('Symlinks are forbidden in public snapshot trees')
 if content_hash(snapshot)!=pointer['sha256']:raise PublishBlocked('Snapshot integrity mismatch')
 files={str(p.relative_to(snapshot)):p.read_bytes().decode('utf-8') for p in snapshot.rglob('*') if p.is_file()}
 rows=source_health(read(snapshot/'index.json'),snapshot)
 if not rows or any(s['status']!='ok' for s in rows):raise PublishBlocked('Partial/unavailable source cannot be published during cutover')
 return validate_candidate(Candidate(key,FEEDS[key]['branch'],pointer['generated_at'],pointer['sha256'],files))

class Publisher:
 """Backend: head(branch), file(commit,path), tree(elements), commit(tree,parent,message), update(branch,sha,force=False)."""
 def __init__(self,backend,repository=REPOSITORY):
  if repository!=REPOSITORY:raise PublishBlocked('Unapproved public repository')
  self.backend=backend
 def current(self,branch):
  try:
   head=_sha(self.backend.head(branch));document=json.loads(self.backend.file(head,'index.json'))
   current=parse(document['generated_at'])
  except (OSError,ValueError,KeyError,TypeError) as exc:raise PublishBlocked('Remote immutable snapshot metadata unreadable or malformed') from exc
  if current>now()+dt.timedelta(minutes=5):raise PublishBlocked('Remote snapshot is future-dated')
  return head,current
 def verify(self,candidate,commit):
  for path,content in candidate.files.items():
   if self.backend.file(commit,path)!=content:raise PublishBlocked('Published bytes do not match immutable local candidate')
 def publish(self,candidate,max_attempts=4):
  validate_candidate(candidate)
  if not 1<=max_attempts<=4:raise PublishBlocked('Publication retry budget must be 1..4')
  tree=None
  for attempt in range(1,max_attempts+1):
   head,current=self.current(candidate.branch)
   if parse(candidate.generated_at)<=current:return {'action':'skipped-not-newer','branch':candidate.branch,'current_commit':head,'attempts':attempt}
   if tree is None:tree=_sha(self.backend.tree(candidate.tree_elements()))
   commit=_sha(self.backend.commit(tree,head,'Refresh public '+candidate.key+' snapshot from direct acquisition'))
   validate_candidate(candidate)
   try:self.backend.update(candidate.branch,commit,force=False)
   except RefRejected:continue
   except RefUncertain:
    seen,remote_stamp=self.current(candidate.branch)
    if seen==commit:
     self.verify(candidate,commit)
     return {'action':'published-verified-after-uncertain-update','branch':candidate.branch,'commit':commit,'parent':head,'attempts':attempt}
    if remote_stamp>=parse(candidate.generated_at):return {'action':'superseded-after-uncertain-update','branch':candidate.branch,'current_commit':seen,'attempts':attempt}
    raise PublishBlocked('Ref update outcome remains uncertain; stop without retry or cutover')
   seen,remote_stamp=self.current(candidate.branch)
   self.verify(candidate,commit)
   if seen!=commit and remote_stamp<parse(candidate.generated_at):raise PublishBlocked('Post-publication timestamp moved backward; stop cutover')
   return {'action':'published' if seen==commit else 'published-then-superseded','branch':candidate.branch,'commit':commit,'parent':head,'attempts':attempt,'current_commit':seen}
  raise PublishBlocked('Concurrent writer prevented safe publication within retry budget')

def main():
 p=argparse.ArgumentParser();p.add_argument('key',choices=FEEDS);p.add_argument('--state',default=str(pathlib.Path(__file__).parent/'runtime'));p.add_argument('--output');a=p.parse_args()
 c=prepare_candidate(pathlib.Path(a.state).resolve(),a.key)
 plan={'repository':REPOSITORY,'key':c.key,'branch':c.branch,'generated_at':c.generated_at,'snapshot_sha256':c.snapshot_sha256,'files':c.files,'tree_elements':c.tree_elements(),'publication_mode':'parented-fast-forward-only','remote_writes_performed':False}
 text=json.dumps(plan,indent=2)+'\n'
 if a.output:pathlib.Path(a.output).write_text(text)
 else:print(text,end='')
if __name__=='__main__':main()
