#!/usr/bin/env python3
"""Direct-invocation personal acquisition runner. No scheduler, publishing, credentials, or notifications."""
from __future__ import annotations
import argparse, contextlib, datetime as dt, fcntl, hashlib, json, os, pathlib, shutil, subprocess, sys, tempfile, uuid
ROOT=pathlib.Path(__file__).resolve().parent
COLLECTORS=('github','public','tibo','media','recovery')
FRESH=4500

def now(): return dt.datetime.now(dt.timezone.utc)
def stamp(t=None): return (t or now()).isoformat().replace('+00:00','Z')
def parse(t):
    value=dt.datetime.fromisoformat(t.replace('Z','+00:00'))
    if value.tzinfo is None: raise ValueError('timestamp must include timezone')
    return value.astimezone(dt.timezone.utc)
def read(path,default=None):
    try:return json.loads(path.read_text())
    except FileNotFoundError:return default

def atomic(path,doc):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name('.'+path.name+'.'+uuid.uuid4().hex)
    with open(temp,'w') as f:json.dump(doc,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    os.replace(temp,path)

@contextlib.contextmanager
def locked(state):
    state.mkdir(parents=True,exist_ok=True)
    with open(state/'runner.lock','a') as f:
        fcntl.flock(f,fcntl.LOCK_EX);yield

def source_health(index, hot):
    sources=index.get('repos') or index.get('sources') or ([index['source']] if isinstance(index.get('source'),dict) else [index])
    out=[]
    for s in sources:
        key=s.get('key') or ('reddit_media' if 'ocr_candidate_count' in index else 'unknown')
        errors=list(s.get('errors',[]))
        available=s.get('status') in ('ok','empty') if 'status' in s else bool(s.get('head_sha'))
        if key=='reddit_media' and errors:available=False
        incomplete=s.get('coverage_complete') is False or (key in ('nu','angourimath') and bool(errors))
        if key=='reddit' and errors:
            incomplete=incomplete or (any('RSS:' in e for e in errors) if s.get('acquisition_mode')=='rss' else True)
        view=read(hot/s.get('view_path','index.json'),{})
        ocr_errors=sum(bool(item.get('ocr_errors')) for item in view.get('items',[]))
        out.append({'key':key,'available':available,'coverage_complete':not incomplete,'errors':errors,'ocr_failed_candidates':ocr_errors,'status':'unavailable' if not available else 'partial' if incomplete or ocr_errors else 'ok'})
    return out

def content_hash(path):
    h=hashlib.sha256()
    for file in sorted(path.rglob('*')):
        if file.is_file():h.update(str(file.relative_to(path)).encode()+b'\0'+file.read_bytes()+b'\0')
    return h.hexdigest()

def event_rows(snapshot):
    rows=[]
    kinds={'commits':'commit','issues':'issue','pulls':'pull_request','releases':'release','workflow_runs':'workflow_run'}
    for path in sorted((snapshot/'views').glob('*.json')):
        doc=read(path,{})
        for group in ('items','commits','issues','pulls','releases','workflow_runs','events'):
            for item in doc.get(group,[]) or []:
                kind=item.get('type') if group=='events' else kinds.get(group,group)
                if group=='issues' and item.get('kind')=='pull_request':kind='pull_request'
                identity=item.get('sha') if kind=='commit' else item.get('url') or item.get('id') or item.get('number')
                if identity is None: identity=hashlib.sha256(json.dumps(item,sort_keys=True).encode()).hexdigest()
                # A commit SHA is immutable. Other GitHub source versions share one event timestamp
                # across hot/recovery schemas; full source views preserve all discussion details.
                version_value=identity if kind=='commit' else (item.get('updated_at') or item.get('timestamp') or item.get('published_at')) if kind in kinds.values() else None
                if version_value is None:version_value=item
                version=hashlib.sha256(json.dumps(version_value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                rows.append({'key':f'{path.stem}:{kind}:{identity}','version':version,'source':path.stem,'kind':kind,'item':item})
    return rows

def publish(state,key,hot,acquisition):
    index=read(hot/'index.json');generated=parse(index['generated_at'])
    if generated>now()+dt.timedelta(minutes=5):raise ValueError('future-dated snapshot rejected')
    pointers=state/'pointers';previous=read(pointers/f'{key}.json')
    if previous and generated<=parse(previous['generated_at']):return {'accepted':False,'reason':'older-or-equal','pointer':previous}
    digest=content_hash(hot);target=state/'snapshots'/key/digest
    target.parent.mkdir(parents=True,exist_ok=True)
    if not target.exists():shutil.copytree(hot,target)
    if content_hash(target)!=digest:raise RuntimeError('immutable snapshot hash mismatch')
    sources=source_health(index,target)
    health={'generated_at':index['generated_at'],'sources':sources,'status':'ok' if all(s['status']=='ok' for s in sources) else 'degraded'}
    pointer={'snapshot':str(target.relative_to(state)),'sha256':digest,'generated_at':index['generated_at'],'fresh_for_seconds':index.get('fresh_for_seconds',FRESH),'mode':acquisition.get('mode'),'health':health}
    # Queue/dedup and the immutable snapshot are durable before recovery cursor advances.
    seen=read(state/'dedup.json',{});new=[];at=stamp()
    for row in event_rows(target):
        if seen.get(row['key'],{}).get('version')!=row['version']:new.append(row)
        seen[row['key']]={'version':row['version'],'seen_at':at}
    cutoff=now()-dt.timedelta(days=8)
    seen={k:v for k,v in seen.items() if parse(v['seen_at'])>=cutoff}
    queued=read(state/'pending'/f'{key}.json',{}).get('new_or_changed_candidates',[])
    all_new={(r['key'],r['version']):r for r in queued+new}
    atomic(state/'pending'/f'{key}.json',{'generated_at':at,'snapshot_sha256':digest,'source_health':health,'new_or_changed_candidates':list(all_new.values()),'delivered':False})
    atomic(state/'dedup.json',seen)
    atomic(pointers/f'{key}.json',pointer)
    if health['status']=='ok':atomic(pointers/f'{key}.last-good.json',pointer)
    return {'accepted':True,'pointer':pointer,'new_or_changed_candidates':len(new)}

def retain_backlog(state):
    current=read(state/'pointers/recovery.json');retained=read(state/'retained.json',{'batches':[]})
    batches=[b for b in retained['batches'] if parse(b['retain_until'])>now()]
    if current:
        doc=read(state/current['snapshot']/'index.json',{})
        if doc.get('has_backlog') and parse(doc['generated_at'])+dt.timedelta(hours=48)>now() and not any(b['sha256']==current['sha256'] for b in batches):
            batches.append({**current,'retained_at':stamp(),'retain_until':stamp(parse(doc['generated_at'])+dt.timedelta(hours=48))})
    # No-backlog runs preserve all still-live recovery batches, not just the latest.
    atomic(state/'retained.json',{'generated_at':stamp(),'batches':batches})
    return {'retained_batches':len(batches)}

def safe_env():
    # Preserve approved network/proxy transport configuration, never token/cookie/auth variables.
    allowed={'PATH','LANG','LC_ALL','TZ','SSL_CERT_FILE','SSL_CERT_DIR','CODEX_PROXY_CERT','HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','NO_PROXY','http_proxy','https_proxy','all_proxy','no_proxy'}
    env={k:v for k,v in os.environ.items() if k in allowed}
    env.update(PYTHONUNBUFFERED='1',PYTHONDONTWRITEBYTECODE='1',HOME='/nonexistent')
    return env

def run_one(state,key,timeout=240,http_timeout=12,max_requests=80,fixture_dir=None,bridge_dir=None):
    provenance=read(ROOT/'provenance.json')
    actual=hashlib.sha256((ROOT/'collectors'/f'{key}.py').read_bytes()).hexdigest()
    if actual!=provenance['collectors'][key]['collector_sha256']:raise RuntimeError('collector differs from reviewed extraction; rerun extractor only after review')
    workspace=state/'runs'/f'{now().strftime("%Y%m%dT%H%M%S.%fZ")}-{key}'
    (workspace/'hot/views').mkdir(parents=True);(workspace/'raw').mkdir()
    previous=read(state/'pointers'/f'{key}.last-good.json') or read(state/'pointers'/f'{key}.json')
    # Recovery state may contain independent cursors even when some sources are degraded.
    if key=='recovery':previous=read(state/'pointers/recovery.json') or previous
    if previous:
        source=state/previous['snapshot']
        if content_hash(source)!=previous['sha256']:raise RuntimeError('previous snapshot integrity failed')
        shutil.copytree(source,workspace/'previous')
    else:(workspace/'previous').mkdir()
    cmd=[sys.executable,str(ROOT/'worker.py'),str(ROOT/'collectors'/f'{key}.py'),'--timeout',str(http_timeout),'--max-requests',str(max_requests),'--cooldown-file',str(state/'cooldowns.json')]
    if bridge_dir:cmd+=['--bridge-dir',str(pathlib.Path(bridge_dir).resolve())]
    if fixture_dir:cmd+=['--fixture-dir',str(pathlib.Path(fixture_dir).resolve())]
    start=stamp()
    try:
        p=subprocess.run(cmd,cwd=workspace,env=safe_env(),capture_output=True,text=True,timeout=timeout)
        result={'collector':key,'started_at':start,'finished_at':stamp(),'exit_code':p.returncode,'errors':p.stderr[-4000:]}
        acquisition=read(workspace/'acquisition.json',{'mode':'fixture' if fixture_dir else 'live-anonymous','requests':[]})
        if not fixture_dir:atomic(state/'cooldowns.json',acquisition.get('cooldowns',{}))
        result['acquisition']=acquisition
        if p.returncode==0 and (workspace/'hot/index.json').exists():result.update(publish(state,key,workspace/'hot',acquisition))
        else:result.update(accepted=False,reason='collector-failed')
    except subprocess.TimeoutExpired:
        result={'collector':key,'started_at':start,'finished_at':stamp(),'accepted':False,'reason':'run-time-budget-exhausted'}
    atomic(workspace/'receipt.json',result);atomic(state/'attempts'/f'{key}.json',result)
    return result

def status(state):
    result={'generated_at':stamp(),'feeds':{}}
    for key in COLLECTORS:
        pointer=read(state/'pointers'/f'{key}.json')
        latest=read(state/'attempts'/f'{key}.json',{})
        if pointer:
            snapshot=state/pointer['snapshot']
            source_rows=source_health(read(snapshot/'index.json'),snapshot)
            pointer['health']={'generated_at':pointer['generated_at'],'sources':source_rows,'status':'ok' if all(s['status']=='ok' for s in source_rows) else 'degraded'}
            age=max(0,(now()-parse(pointer['generated_at'])).total_seconds())
            result['feeds'][key]={**pointer,'age_seconds':int(age),'fresh':age<=pointer['fresh_for_seconds'],'last_attempt_reason':latest.get('reason')}
        else:result['feeds'][key]={'fresh':False,'status':'not-collected','last_attempt_reason':latest.get('reason')}
    return result

def main():
    os.umask(0o077)
    p=argparse.ArgumentParser();p.add_argument('--state',default=str(ROOT/'runtime'));sp=p.add_subparsers(dest='command',required=True)
    r=sp.add_parser('run');r.add_argument('collectors',nargs='*',choices=COLLECTORS);r.add_argument('--due',action='store_true');r.add_argument('--timeout',type=int,default=240);r.add_argument('--http-timeout',type=float,default=12);r.add_argument('--max-requests',type=int,default=80);r.add_argument('--fixture-dir');r.add_argument('--bridge-dir')
    sp.add_parser('status');sp.add_parser('retain');sp.add_parser('verify')
    a=p.parse_args();state=pathlib.Path(a.state).resolve()
    if a.command=='status':print(json.dumps(status(state),indent=2));return
    with locked(state):
        if a.command=='run':
            results=[]
            for key in a.collectors or COLLECTORS:
                attempt=read(state/'attempts'/f'{key}.json',{})
                if a.due and attempt and (now()-parse(attempt['finished_at'])).total_seconds()<3600:continue
                results.append(run_one(state,key,a.timeout,a.http_timeout,a.max_requests,a.fixture_dir,a.bridge_dir))
            retained=retain_backlog(state);atomic(state/'health.json',status(state));print(json.dumps({'results':results,**retained},indent=2))
        elif a.command=='retain':print(json.dumps(retain_backlog(state)))
        elif a.command=='verify':
            checks=[]
            for path in (state/'pointers').glob('*.json'):
                pointer=read(path);checks.append({'pointer':path.name,'valid':content_hash(state/pointer['snapshot'])==pointer['sha256']})
            print(json.dumps(checks,indent=2));sys.exit(0 if all(x['valid'] for x in checks) else 1)
if __name__=='__main__':main()
