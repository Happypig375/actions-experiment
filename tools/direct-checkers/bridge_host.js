// Run this JavaScript with functions.exec, not node. It uses the existing GitHub connector.
// Change collector to "recovery" for recovery; that additionally needs approved anonymous network access.
const collector = "github";
const root = "REPLACE_WITH_INSTALLED_CHECKER_DIRECTORY";
const bridge = root + "/runtime/bridge-" + collector + "-" + Date.now();
const command = "cd " + root + " && python runner.py run " + collector + " --bridge-dir " + bridge + " --timeout 300 > evidence/latest-" + collector + "-bridge.json";
const launched = await tools.exec_command({cmd:command,yield_time_ms:1000,max_output_tokens:1000});
if (launched.exit_code !== undefined) { text(launched); exit(); }
const deadline=Date.now()+290000;
while (Date.now()<deadline) {
 const q=await tools.exec_command({cmd:"python - <<'PY'\nimport pathlib,json\np=pathlib.Path("+JSON.stringify(bridge)+")\nprint(json.dumps([dict(json.loads(f.read_text()),response_file=str(f).replace('.request.json','.response.json')) for f in p.glob('*.request.json') if not f.with_name(f.name.replace('.request.json','.response.json')).exists()]))\nPY",max_output_tokens:3000});
 const pending=JSON.parse(q.output.trim()||"[]");
 if (!pending.length) {
  const status=await tools.write_stdin({session_id:launched.session_id,chars:"",yield_time_ms:1000,max_output_tokens:1000});
  if (status.exit_code!==undefined) {text(status);break;}
  continue;
 }
 for (const request of pending) {
  if (!/^https:\/\/api\.github\.com\/repos\/(bryanedds\/Nu|asc-community\/AngouriMath)(\/|\?|$)/.test(request.url)) throw Error("Out-of-scope bridge request");
  let result=await tools.mcp__codex_apps__github_fetch({url:request.url});
  // One safe retry only for connector Internal error, never permissions/auth/rate limits.
  if (result.isError && JSON.stringify(result).includes("Mcp error: -32603: Internal error")) result=await tools.mcp__codex_apps__github_fetch({url:request.url});
  const content=result.structuredContent?.content;
  const response={url:request.url,fetched_at:new Date().toISOString(),...(typeof content==="string"&&!result.isError?{content}:{error:JSON.stringify(result)})};
  await tools.apply_patch("*** Begin Patch\n*** Add File: "+request.response_file+"\n+"+JSON.stringify(response)+"\n*** End Patch");
  text({url:request.url,status:response.error?"error":"ok"});
 }
}
text(await tools.exec_command({cmd:"cd "+root+" && python runner.py status",max_output_tokens:7000}));
