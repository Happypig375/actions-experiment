#!/usr/bin/env python3
"""One-time continuity import from connector-materialized, immutable public snapshots."""
import argparse,json,pathlib,shutil
from runner import atomic,content_hash,locked,parse,read,source_health,now,stamp
NAMES={'github':'chatgpt-important-update-feed','public':'chatgpt-important-update-public-feed','tibo':'chatgpt-important-update-tibo-feed','media':'chatgpt-important-update-reddit-media-feed','recovery':'chatgpt-important-update-backlog-feed'}
def main():
 p=argparse.ArgumentParser();p.add_argument('source');p.add_argument('state');a=p.parse_args();source=pathlib.Path(a.source).resolve();state=pathlib.Path(a.state).resolve()
 with locked(state):
  if (state/'pointers').exists():raise SystemExit('Refuse to seed an already initialized state directory')
  provenance=read(source/'provenance.json'); records={d['branch']:d for d in provenance['immutable_snapshots']}
  for key,branch in NAMES.items():
   src=source/branch;stage=state/'imports'/key;shutil.copytree(src,stage)
   index=read(stage/'index.json')
   for s in index.get('repos',[])+index.get('sources',[])+([index['source']] if isinstance(index.get('source'),dict) else []):
    if s.get('view_path') and not (stage/s['view_path']).is_file():raise ValueError('seed view missing')
   if key=='recovery':
    shutil.copy2(source/'chatgpt-important-update-recovery-state/state.json',stage/'state.json')
   digest=content_hash(stage);target=state/'snapshots'/key/digest;target.parent.mkdir(parents=True,exist_ok=True);shutil.copytree(stage,target)
   pointer={'snapshot':str(target.relative_to(state)),'sha256':digest,'generated_at':index['generated_at'],'fresh_for_seconds':index.get('fresh_for_seconds',4500),'mode':'imported-github-snapshot','origin':records[branch],'health':{'status':'imported-not-live-verified','sources':source_health(index,target)}}
   atomic(state/'pointers'/f'{key}.json',pointer)
   if all(x['status']=='ok' for x in pointer['health']['sources']):atomic(state/'pointers'/f'{key}.last-good.json',pointer)
  branch='chatgpt-important-update-backlog-retained';src=source/branch;index=read(src/'index.json')
  if index and index.get('retain_until') and parse(index['retain_until'])>now():
   digest=content_hash(src);target=state/'snapshots/retained'/digest;target.parent.mkdir(parents=True,exist_ok=True);shutil.copytree(src,target)
   atomic(state/'retained.json',{'generated_at':stamp(),'batches':[{'snapshot':str(target.relative_to(state)),'sha256':digest,'generated_at':index['generated_at'],'retained_at':index['retained_at'],'retain_until':index['retain_until'],'origin':records[branch]}]})
  atomic(state/'seed-provenance.json',provenance)
 print('Imported old source timestamps, heads, OCR cache, cursors, and unexpired retained backlog without claiming live verification.')
if __name__=='__main__':main()
