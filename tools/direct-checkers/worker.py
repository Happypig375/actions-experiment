#!/usr/bin/env python3
"""Token-free, finite HTTP boundary for the preserved collectors."""
import argparse, datetime as dt, email.message, hashlib, io, json, pathlib, runpy, time, urllib.error, urllib.parse, urllib.request

ALLOWED_HOSTS = {'api.github.com','www.reddit.com','t.me','fxtwitter.com','rsshub.chyi.org','rsshub.olivetint.com','i.redd.it','preview.redd.it','external-preview.redd.it','redditmedia.com','www.redditmedia.com'}

def validate_url(url):
    p = urllib.parse.urlsplit(url)
    if p.scheme != 'https' or p.hostname not in ALLOWED_HOSTS or p.username or p.password or p.port not in (None,443):
        raise ValueError('URL outside public acquisition allowlist')
    if p.hostname == 'api.github.com' and not any(p.path.startswith('/repos/'+repo+'/') or p.path == '/repos/'+repo for repo in ('bryanedds/Nu','asc-community/AngouriMath')):
        raise ValueError('GitHub repository outside public watch scope')
    if p.hostname == 'www.reddit.com' and not p.path.startswith('/r/codex/'):
        raise ValueError('Reddit path outside public watch scope')
    if p.hostname == 't.me' and p.path != '/s/codex_resets':
        raise ValueError('Telegram path outside public watch scope')
    if any(any(word in key.lower() for word in ('token','secret','password','api_key','apikey','credential','authorization')) for key,value in urllib.parse.parse_qsl(p.query)):
        raise ValueError('Authentication data is forbidden in public acquisition URLs')
    return p

class Redirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_url(newurl)
        return super().redirect_request(req,fp,code,msg,headers,newurl)

class Response(io.BytesIO):
    def __init__(self, data, headers):
        super().__init__(data)
        self.headers = email.message.Message()
        for k,v in headers.items(): self.headers[k] = v
        self.status=200


def main():
    p=argparse.ArgumentParser();p.add_argument('collector');p.add_argument('--timeout',type=float,default=12);p.add_argument('--max-requests',type=int,default=80);p.add_argument('--fixture-dir');p.add_argument('--cooldown-file');p.add_argument('--bridge-dir');a=p.parse_args()
    opener=urllib.request.build_opener(Redirects())
    log=[]; stopped_hosts={}; started=time.monotonic(); requests=0
    cooldowns={}
    if a.cooldown_file and pathlib.Path(a.cooldown_file).exists():
        cooldowns=json.loads(pathlib.Path(a.cooldown_file).read_text())
    fixture=json.loads((pathlib.Path(a.fixture_dir)/'responses.json').read_text()) if a.fixture_dir else None
    def get(req, timeout=None, *args, **kwargs):
        nonlocal requests
        url=req.full_url if isinstance(req,urllib.request.Request) else req
        parsed=validate_url(url);host=parsed.hostname
        if isinstance(req,urllib.request.Request):
            if req.get_method() != 'GET' or any(k.lower() in ('authorization','cookie','proxy-authorization') for k,v in req.header_items()):
                raise ValueError('Only anonymous public GETs are permitted')
        if host in stopped_hosts: raise RuntimeError(stopped_hosts[host])
        if not (a.bridge_dir and host=='api.github.com') and cooldowns.get(host,0) > time.time(): raise RuntimeError('source rate-limit cooldown still active')
        if requests >= a.max_requests: raise RuntimeError('request budget exhausted; coverage incomplete')
        requests+=1
        rec={'url':url,'started_at':dt.datetime.now(dt.timezone.utc).isoformat()}
        try:
            if a.bridge_dir and host=='api.github.com':
                directory=pathlib.Path(a.bridge_dir);directory.mkdir(parents=True,exist_ok=True)
                rid=hashlib.sha256(url.encode()).hexdigest()
                request_path=directory/(rid+'.request.json'); response_path=directory/(rid+'.response.json')
                request_path.write_text(json.dumps({'url':url,'requested_at':rec['started_at']}))
                deadline=time.monotonic()+120
                while not response_path.exists():
                    if time.monotonic()>deadline:raise RuntimeError('connector bridge response timed out')
                    time.sleep(0.1)
                entry=json.loads(response_path.read_text())
                if entry.get('url')!=url:raise ValueError('connector response URL mismatch')
                if 'error' in entry:raise RuntimeError(entry['error'])
                fetched=dt.datetime.fromisoformat(entry['fetched_at'].replace('Z','+00:00'))
                if abs((dt.datetime.now(dt.timezone.utc)-fetched).total_seconds())>300:raise ValueError('connector response is stale')
                data=entry['content'].encode();headers={'Content-Type':'application/json'}
                json.loads(data)
                rec['transport']='connected-github-read'
            elif fixture is not None:
                entry=fixture[url]
                if 'error' in entry: raise RuntimeError(entry['error'])
                data=(pathlib.Path(a.fixture_dir)/entry['body']).read_bytes();headers=entry.get('headers',{})
            else:
                with opener.open(req,timeout=min(timeout or a.timeout,a.timeout)) as response:
                    data=response.read(16*1024*1024+1);headers=dict(response.headers.items())
                    if len(data)>16*1024*1024:raise RuntimeError('response exceeds 16 MiB byte budget')
            if host=='t.me' and b'data-post="codex_resets/' not in data:
                raise RuntimeError('Telegram response has no recognizable public channel messages; unavailable or layout changed')
            rec.update(status='ok',bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
            return Response(data,headers)
        except urllib.error.HTTPError as exc:
            rec.update(status='error',http_status=exc.code,error=str(exc))
            if exc.code==429 or (exc.code==403 and (exc.headers.get('X-RateLimit-Remaining')=='0' or 'rate limit' in str(exc).lower())):
                seconds=3600
                try: seconds=max(60,min(86400,int(exc.headers.get('Retry-After','3600'))))
                except ValueError: pass
                try: seconds=max(seconds,int(exc.headers.get('X-RateLimit-Reset','0'))-int(time.time()))
                except ValueError: pass
                cooldowns[host]=time.time()+min(seconds,86400)
                stopped_hosts[host]=f'HTTP {exc.code}; source cooldown active'
            raise
        except Exception as exc:
            rec.update(status='error',error=str(exc)[:800]);raise
        finally:
            log.append(rec)
    urllib.request.urlopen=get
    try:runpy.run_path(a.collector,run_name='__main__')
    finally:
        pathlib.Path('acquisition.json').write_text(json.dumps({'mode':'fixture' if fixture is not None else 'live-connector-and-public' if a.bridge_dir else 'live-anonymous','requests':log,'request_count':requests,'duration_seconds':time.monotonic()-started,'cooldowns':cooldowns},indent=2)+'\n')
if __name__ == '__main__': main()
