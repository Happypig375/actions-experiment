// Run in functions.exec. Read-only by default. Parent must authorize this exact public release first.
// Set root to the installed checker directory and key to github, tibo, or media.
const root = "REPLACE_WITH_INSTALLED_CHECKER_DIRECTORY";
const key = "github";
const approveRemoteWrite = false;
const q=await tools.exec_command({cmd:"cd "+root+" && python publication.py "+key,max_output_tokens:20000});
if(q.exit_code!==0)throw Error("Candidate validation failed: "+q.output);
const plan=JSON.parse(q.output);
const repository="Happypig375/actions-experiment";
const branches={github:"chatgpt-important-update-feed",tibo:"chatgpt-important-update-tibo-feed",media:"chatgpt-important-update-reddit-media-feed"};
if(plan.repository!==repository||plan.branch!==branches[key]||plan.publication_mode!=="parented-fast-forward-only")throw Error("Unapproved publication scope");
function unwrap(result){if(result.isError)throw Error(JSON.stringify(result));return result.structuredContent;}
function sha(result){const d=unwrap(result);const s=d?.sha||d?.commit?.sha||d?.structuredContent?.sha; if(!/^[0-9a-f]{40}$/.test(s||""))throw Error("Unexpected connector object result; stop before ref update");return s;}
async function readJSON(url){const d=unwrap(await tools.mcp__codex_apps__github_fetch({url}));if(typeof d?.content!=="string")throw Error("Unexpected immutable read result");return JSON.parse(d.content);}
async function readText(commit,path){const url="https://raw.githubusercontent.com/"+repository+"/"+commit+"/"+path;const d=unwrap(await tools.mcp__codex_apps__github_fetch({url}));if(typeof d?.content!=="string")throw Error("Unexpected immutable file read result");return d.content;}
async function current(){const branch=await readJSON("https://api.github.com/repos/"+repository+"/branches/"+encodeURIComponent(plan.branch));const head=branch.commit?.sha;if(!/^[0-9a-f]{40}$/.test(head||""))throw Error("Invalid branch head");const index=JSON.parse(await readText(head,"index.json"));const when=Date.parse(index.generated_at);if(!Number.isFinite(when)||when>Date.now()+300000)throw Error("Invalid remote timestamp");return {head,when};}
function stillFresh(){const generated=Date.parse(plan.generated_at);if(!Number.isFinite(generated)||generated>Date.now()+300000||Date.now()-generated>4500000)throw Error("Candidate is no longer fresh");}
async function verify(commit){for(const [path,content] of Object.entries(plan.files)){if(await readText(commit,path)!==content)throw Error("Published bytes differ: "+path);}}
let tree;
for(let attempt=1;attempt<=4;attempt++){
 stillFresh();const previous=await current();
 if(Date.parse(plan.generated_at)<=previous.when){text({action:"skipped-not-newer",branch:plan.branch,current_commit:previous.head});break;}
 if(!approveRemoteWrite){text({action:"would-publish",repository,branch:plan.branch,parent:previous.head,files:Object.keys(plan.files),generated_at:plan.generated_at,mode:plan.publication_mode,remote_writes_performed:false});break;}
 if(!tree)tree=sha(await tools.mcp__codex_apps__github_create_tree({repository_full_name:repository,base_tree_sha:null,tree_elements:plan.tree_elements}));
 const commit=sha(await tools.mcp__codex_apps__github_create_commit({repository_full_name:repository,tree_sha:tree,parent_sha:previous.head,message:"Refresh public "+key+" snapshot from direct acquisition"}));
 stillFresh();
 text({stage:"prepared-ref-update",branch:plan.branch,commit,parent:previous.head});
 const update=await tools.mcp__codex_apps__github_update_ref({repository_full_name:repository,branch_name:plan.branch,sha:commit,force:false});
 if(update.isError){
  text({stage:"ref-update-error",branch:plan.branch,commit,result:update});
  const detail=JSON.stringify(update);
  if(/not a fast.forward|non.fast.forward/i.test(detail)){if(attempt===4)throw Error("Concurrent writer exhausted publication budget");continue;}
  if(/denied|rejected due to unacceptable risk|not authorized|permission|forbidden/i.test(detail))throw Error("Publication denied; stop without retry");
  const after=await current();
  if(after.head===commit){await verify(commit);text({action:"published-verified-after-uncertain-update",branch:plan.branch,commit});break;}
  if(after.when>=Date.parse(plan.generated_at)){text({action:"superseded-after-uncertain-update",branch:plan.branch,current_commit:after.head});break;}
  throw Error("Uncertain ref update; verified head differs and is older. Stop without blind retry");
 }
 const after=await current();await verify(commit);
 if(after.head!==commit&&after.when<Date.parse(plan.generated_at))throw Error("Post-publication rollback detected; stop cutover");
 text({action:after.head===commit?"published":"published-then-superseded",repository,branch:plan.branch,commit,parent:previous.head,current_commit:after.head,verified_paths:Object.keys(plan.files)});break;
}
