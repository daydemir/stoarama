package api

import "strings"

// SupportEmail is the single source of truth for the address users contact for
// help. Surfaced site-wide in the shared footer (below) and on the account/docs
// pages. Do not hardcode the address anywhere else.
const SupportEmail = "deniz@aydemir.us"

// Canonical topbar CSS. Injected at <!--SHELL_HEAD--> (inside each page's <head>,
// before </style> is fine — these are standalone rules). Single source of truth.
const shellHeadCSS = `
<style id="shell-css">
.topbar{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;column-gap:16px;margin-bottom:16px;min-height:48px;}
.topbar-left{display:flex;align-items:center;gap:10px;min-width:0;overflow:hidden;justify-self:start;}
.topbar-center{display:flex;align-items:center;justify-content:center;min-width:0;justify-self:center;}
.topbar-right{display:flex;align-items:center;gap:10px;justify-self:end;min-width:0;}
.site-title{font-family:var(--mono);font-size:15px;font-weight:600;letter-spacing:0.05em;color:var(--accent);text-decoration:none;white-space:nowrap;}
.global-nav{display:inline-flex;align-items:center;gap:6px;padding:4px;border:1px solid var(--border);border-radius:999px;background:color-mix(in srgb,var(--panel) 90%,#000);}
.global-nav a{display:inline-flex;align-items:center;gap:6px;padding:7px 14px;border-radius:999px;border:1px solid transparent;font-family:var(--mono);font-size:11px;letter-spacing:0.04em;text-transform:uppercase;color:var(--muted);text-decoration:none;line-height:1;transition:background 120ms ease,color 120ms ease,border-color 120ms ease;}
.global-nav a:hover{color:var(--text);border-color:color-mix(in srgb,var(--accent) 35%,var(--border));background:color-mix(in srgb,var(--panel2) 78%,transparent);text-decoration:none;}
.global-nav a.active{color:#fff;background:var(--accent);border-color:var(--accent);}
.topbar-docs-link{font-family:var(--mono);font-size:11px;letter-spacing:0.04em;text-transform:uppercase;color:var(--muted);text-decoration:none;white-space:nowrap;padding:0 4px;}
.topbar-docs-link:hover{color:var(--text);text-decoration:none;}
.account-chip{position:relative;display:inline-flex;align-items:center;gap:7px;height:34px;padding:0 12px;border-radius:999px;border:1px solid var(--border);background:color-mix(in srgb,var(--panel) 92%,#000);color:var(--muted);font-family:var(--mono);font-size:11px;white-space:nowrap;}
.account-chip a{color:var(--text);text-decoration:none;}
.account-chip a:hover{text-decoration:underline;}
.account-chip .caret{font-size:9px;opacity:0.85;}
/* Current-org indicator: a building glyph + the selected org name + a caret,
   reading as "you are viewing: <Org>". Framed distinctly (accent-tinted border)
   so it never reads like a personal user menu. Billing + clips are org-scoped, so
   the org must always be unmistakable. */
.org-switch{display:inline-flex;align-items:center;gap:7px;cursor:pointer;color:var(--text);height:28px;padding:0 4px;border-radius:999px;}
.org-switch .org-glyph{font-size:12px;color:var(--accent);line-height:1;}
.org-switch .org-name{font-weight:600;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.org-switch:hover .org-name{color:var(--accent);}
.org-switch .org-label{color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:0.05em;}
.org-menu{position:absolute;top:40px;right:0;min-width:220px;padding:6px;border:1px solid var(--border);border-radius:10px;background:color-mix(in srgb,var(--panel) 96%,#000);box-shadow:0 8px 24px rgba(0,0,0,0.35);z-index:50;display:none;flex-direction:column;gap:2px;}
.org-menu.open{display:flex;}
.org-menu .org-menu-head{padding:4px 10px 6px;color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:0.05em;}
.org-menu .org-item{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:7px;color:var(--text);cursor:pointer;font-size:11px;text-align:left;background:none;border:none;font-family:var(--mono);width:100%;text-decoration:none;}
.org-menu .org-item:hover{background:color-mix(in srgb,var(--panel2) 80%,transparent);text-decoration:none;color:var(--text);}
.org-menu .org-item.current{color:var(--accent);}
/* Org row grid: [name (grows) | role (auto) | check slot (fixed)]. The check slot
   is present-but-empty on non-current rows so the role label lines up across all rows. */
.org-menu .org-item .org-item-name{flex:1 1 auto;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.org-menu .org-item .org-check{flex:0 0 12px;width:12px;text-align:right;color:var(--accent);font-size:10px;}
.org-menu .org-role{flex:0 0 auto;color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:0.04em;}
.org-menu .org-sep{height:1px;margin:4px 2px;background:var(--border);}
.org-menu .org-muted{color:var(--muted);}
.org-menu .org-muted:hover{color:var(--text);}
/* New-org modal: reuses the panel look; overlay dims the page, panel is centered. */
.org-modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.55);display:none;align-items:center;justify-content:center;z-index:100;}
.org-modal-overlay.open{display:flex;}
.org-modal{width:320px;max-width:calc(100vw - 32px);padding:20px;border:1px solid var(--border);border-radius:12px;background:var(--panel);box-shadow:0 12px 40px rgba(0,0,0,0.5);font-family:var(--mono);}
.org-modal h3{margin:0 0 4px;font-size:13px;letter-spacing:0.04em;color:var(--text);}
.org-modal p{margin:0 0 14px;font-size:11px;color:var(--muted);line-height:1.4;}
.org-modal label{display:block;font-size:9px;text-transform:uppercase;letter-spacing:0.05em;color:var(--muted);margin-bottom:6px;}
.org-modal input{width:100%;box-sizing:border-box;height:36px;padding:0 10px;border:1px solid var(--border);border-radius:8px;background:color-mix(in srgb,var(--panel2) 80%,#000);color:var(--text);font-family:var(--mono);font-size:12px;}
.org-modal input:focus{outline:none;border-color:var(--accent);}
.org-modal .org-modal-err{min-height:14px;margin:8px 0 0;font-size:10px;color:#e06b6b;}
.org-modal .org-modal-actions{display:flex;justify-content:flex-end;gap:8px;margin-top:14px;}
.org-modal button{height:32px;padding:0 14px;border-radius:8px;font-family:var(--mono);font-size:11px;letter-spacing:0.04em;cursor:pointer;border:1px solid var(--border);}
.org-modal button.primary{background:var(--accent);border-color:var(--accent);color:#fff;}
.org-modal button.primary[disabled]{opacity:0.6;cursor:default;}
.org-modal button.ghost{background:none;color:var(--muted);}
.org-modal button.ghost:hover{color:var(--text);}
@media (max-width:720px){.topbar{grid-template-columns:auto minmax(0,1fr);grid-template-areas:"brand utils" "nav nav";row-gap:10px;column-gap:10px;align-items:center;}.topbar-left{grid-area:brand;}.topbar-right{grid-area:utils;justify-self:stretch;justify-content:flex-end;flex-wrap:wrap;row-gap:8px;}.topbar-center{grid-area:nav;justify-self:stretch;}.global-nav{width:100%;justify-content:center;}.topbar-docs-link{display:inline-flex;align-items:center;min-height:40px;padding:0 8px;}.org-switch{height:40px;}.org-switch .org-name{max-width:40vw;}}
.site-footer{margin:40px auto 24px;max-width:1200px;padding:16px 4px 0;border-top:1px solid var(--border);display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:10px;font-family:var(--mono);font-size:11px;letter-spacing:0.03em;color:var(--muted);}
.site-footer a{color:var(--muted);text-decoration:none;}
.site-footer a:hover{color:var(--text);text-decoration:underline;}
/* Authoritative button system, injected into every page head (after each page's
   own <style>) so the whole app reads the same and these rules win over page-local
   button styles. --accent-strong is the saturated brand teal the active nav tab and
   active filter chips use; primary buttons must fill with it, never a washed tint. */
:root{--accent-strong:#1288a8;}
button.primary,button[type=submit].primary,.btn.primary{background:var(--accent-strong);border-color:var(--accent-strong);color:#fff;font-weight:600;box-shadow:0 1px 2px rgba(0,0,0,0.25);}
button.primary:hover,button[type=submit].primary:hover,.btn.primary:hover{background:color-mix(in srgb,var(--accent-strong) 88%,#000);border-color:color-mix(in srgb,var(--accent-strong) 88%,#000);color:#fff;}
/* Secondary/utility: clearly clickable outline, dark text, no faint accent-tint fill. */
button.danger,.btn.danger{background:color-mix(in srgb,var(--err) 12%,transparent);border-color:color-mix(in srgb,var(--err) 45%,var(--border));color:var(--err);}
button.danger:hover,.btn.danger:hover{background:color-mix(in srgb,var(--err) 18%,transparent);color:var(--err);}
/* Disabled is the ONLY faint/inert look: dimmed, not-allowed, no pointer events. */
button:disabled,button[disabled],.btn:disabled,.btn[disabled]{opacity:0.45;cursor:not-allowed;pointer-events:none;}
</style>`

// Canonical topbar markup. Injected at <!--SHELL_TOPBAR--> (first child of <body>).
// %ACTIVE_STREAMS% / %ACTIVE_RECORDING% become " active" or "" per page.
const shellTopbarTmpl = `<div class="topbar">
  <div class="topbar-left">
    <a href="/" class="site-title">STO-A-RAMA</a>
  </div>
  <div class="topbar-center">
    <nav class="global-nav" aria-label="Global">
      <a href="/" class="%ACTIVE_STREAMS%">Streams</a>
      <a href="/recordings" class="%ACTIVE_RECORDING%">Recordings</a>
    </nav>
  </div>
  <div class="topbar-right" id="accountArea">
    <a class="topbar-docs-link" href="/pricing">Pricing</a>
    <a class="topbar-docs-link" href="/docs/getting-started">Docs</a>
    <div class="account-chip" id="topbarAccountStatus">Checking session...</div>
  </div>
</div>` + shellTopbarJS

// Canonical topbar BEHAVIOR. One definition for every page: fetch the session,
// render the account chip identically, so the account control works
// on every tab including Recording. Wrapped in an IIFE that runs from its own
// script element, independent of any page-body script, so a page-body JS error
// cannot break the topbar. References only the shared #topbar* ids from the markup
// above. Do not duplicate this logic in any page.
const shellTopbarJS = `
<script>
(function(){
  function esc(v){return String(v==null?"":v).replace(/[&<>"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c];});}
  var chip=document.getElementById("topbarAccountStatus");
  function redirectPath(){
    try{return location.pathname+(location.search||"");}catch(_){return "/";}
  }
  function authCompleted(){
    try{return String(new URLSearchParams(location.search).get("auth")||"").toLowerCase()==="complete";}catch(_){return false;}
  }
  function clearAuthCompleteParam(){
    try{
      var next=new URL(location.href);
      next.searchParams.delete("auth");
      history.replaceState(null,"",next.pathname+(next.searchParams.toString()?("?"+next.searchParams.toString()):"")+(next.hash||""));
    }catch(_){}
  }
  function sleep(ms){return new Promise(function(resolve){setTimeout(resolve,ms);});}
  function signInHref(){
    return "/account?redirect_path="+encodeURIComponent(redirectPath());
  }
  function renderAnon(label){
    if(!chip)return;
    chip.innerHTML='<a href="'+signInHref()+'">'+(label||"Log in")+'</a>';
  }
  function switchOrg(id){
    return fetch("/api/v1/account/orgs/"+encodeURIComponent(id)+"/switch",{method:"POST",credentials:"same-origin",headers:{Accept:"application/json"}})
      .then(function(res){if(!res.ok){throw new Error("status "+res.status);}location.reload();});
  }
  function logout(){
    fetch("/api/v1/account/logout",{method:"POST",credentials:"same-origin",headers:{Accept:"application/json"}})
      .then(function(){location.href="/";})
      .catch(function(){location.href="/";});
  }
  // In-page create-org modal (replaces window.prompt). Built once, reused; posts to
  // POST /api/v1/account/orgs {name}, then switches to the new org and reloads.
  function openCreateOrgModal(){
    var overlay=document.getElementById("orgCreateModal");
    if(!overlay){
      overlay=document.createElement("div");
      overlay.className="org-modal-overlay";
      overlay.id="orgCreateModal";
      overlay.innerHTML='<div class="org-modal" role="dialog" aria-modal="true" aria-labelledby="orgModalTitle">'
        +'<h3 id="orgModalTitle">New org</h3>'
        +'<p>Billing and recorded clips are scoped to the selected org.</p>'
        +'<label for="orgModalName">Org name</label>'
        +'<input id="orgModalName" type="text" autocomplete="off" placeholder="e.g. Acme Field Team" />'
        +'<div class="org-modal-err" id="orgModalErr"></div>'
        +'<div class="org-modal-actions">'
        +'<button type="button" class="ghost" id="orgModalCancel">Cancel</button>'
        +'<button type="button" class="primary" id="orgModalCreate">Create</button>'
        +'</div></div>';
      document.body.appendChild(overlay);
      var input=overlay.querySelector("#orgModalName");
      var errEl=overlay.querySelector("#orgModalErr");
      var createBtn=overlay.querySelector("#orgModalCreate");
      function close(){overlay.classList.remove("open");}
      function submit(){
        var name=String(input.value||"").trim();
        errEl.textContent="";
        if(!name){errEl.textContent="Enter an org name.";input.focus();return;}
        createBtn.disabled=true;
        fetch("/api/v1/account/orgs",{method:"POST",credentials:"same-origin",headers:{Accept:"application/json","Content-Type":"application/json"},body:JSON.stringify({name:name})})
          .then(function(res){return res.json().then(function(d){if(!res.ok){throw new Error((d&&d.error)||("status "+res.status));}return d;});})
          .then(function(d){return switchOrg(d.id);})
          .catch(function(err){createBtn.disabled=false;errEl.textContent=(err&&err.message)||"Could not create org.";});
      }
      overlay.addEventListener("click",function(e){if(e.target===overlay){close();}});
      overlay.querySelector("#orgModalCancel").addEventListener("click",close);
      createBtn.addEventListener("click",submit);
      input.addEventListener("keydown",function(e){if(e.key==="Enter"){e.preventDefault();submit();}else if(e.key==="Escape"){close();}});
    }
    var nameInput=overlay.querySelector("#orgModalName");
    var errBox=overlay.querySelector("#orgModalErr");
    var createButton=overlay.querySelector("#orgModalCreate");
    nameInput.value="";errBox.textContent="";createButton.disabled=false;
    overlay.classList.add("open");
    nameInput.focus();
  }
  function renderSignedIn(payload){
    if(!chip)return;
    var acct=payload.account||{};
    var email=String(acct.email||"").trim()||"Account";
    var currentOrg=payload.current_org||{};
    var orgs=Array.isArray(payload.orgs)?payload.orgs:[];
    var currentId=Number(currentOrg.id||acct.id||0);
    var orgName=String(currentOrg.name||"").trim()||email;
    // Current-org indicator: a building glyph + the selected org name + a caret.
    // Always shown for a signed-in user so the org (which scopes billing + clips) is
    // never ambiguous. The dropdown carries the org list + Account + New org + Log out.
    var orgItems=orgs.map(function(o){
      var oid=Number(o.id);
      var isCur=oid===currentId;
      var cur=isCur?' current':'';
      return '<button type="button" class="org-item'+cur+'" data-org="'+oid+'">'
        +'<span class="org-item-name">'+esc(o.name)+'</span>'
        +'<span class="org-role">'+esc(o.role||"")+'</span>'
        +'<span class="org-check" aria-hidden="true">'+(isCur?'✓':'')+'</span></button>';
    }).join('');
    chip.innerHTML='<span class="org-switch" id="orgSwitch" title="Current org">'
        +'<span class="org-glyph" aria-hidden="true">&#127970;</span>'
        +'<span class="org-name">'+esc(orgName)+'</span>'
        +'<span class="caret" aria-hidden="true">&#9662;</span></span>'
      +'<div class="org-menu" id="orgMenu">'
        +(orgItems?'<div class="org-menu-head">Your orgs</div>'+orgItems+'<div class="org-sep"></div>':'')
        +'<a class="org-item org-muted" id="orgAccount" href="/account">Account</a>'
        +'<a class="org-item org-muted" id="orgSettings" href="/org-settings">Org settings</a>'
        +'<button type="button" class="org-item org-muted" id="orgCreate">New org</button>'
        +'<div class="org-sep"></div>'
        +'<button type="button" class="org-item org-muted" id="orgLogout">Log out</button>'
      +'</div>';
    var sw=document.getElementById("orgSwitch");
    var menu=document.getElementById("orgMenu");
    if(sw&&menu){
      sw.addEventListener("click",function(e){e.stopPropagation();menu.classList.toggle("open");});
      document.addEventListener("click",function(){menu.classList.remove("open");});
      menu.addEventListener("click",function(e){e.stopPropagation();});
      menu.querySelectorAll(".org-item[data-org]").forEach(function(btn){
        btn.addEventListener("click",function(){var id=Number(btn.getAttribute("data-org"));if(id&&id!==currentId){switchOrg(id).catch(function(){});}else{menu.classList.remove("open");}});
      });
      var createBtn=document.getElementById("orgCreate");
      if(createBtn){createBtn.addEventListener("click",function(){menu.classList.remove("open");openCreateOrgModal();});}
      var logoutBtn=document.getElementById("orgLogout");
      if(logoutBtn){logoutBtn.addEventListener("click",function(){menu.classList.remove("open");logout();});}
    }
  }
  function fetchAccountMe(){
    var ctrl=new AbortController();
    var to=setTimeout(function(){ctrl.abort();},8000);
    return fetch("/api/v1/account/me",{credentials:"same-origin",headers:{Accept:"application/json"},signal:ctrl.signal})
      .then(function(res){
        clearTimeout(to);
        if(res.status===401){return null;}
        if(!res.ok){throw new Error("status "+res.status);}
        return res.json();
      })
      .catch(function(err){clearTimeout(to);throw err;});
  }
  async function load(){
    if(chip){chip.textContent="Checking session...";}
    var attempts=authCompleted()?6:1;
    try{
      var payload=null;
      for(var i=0;i<attempts;i++){
        payload=await fetchAccountMe();
        if(payload){break;}
        if(i<attempts-1){await sleep(250);}
      }
      if(!payload){renderAnon("Log in");return;}
      var acct=payload.account||{};
      if(payload.authenticated&&acct&&acct.email){
        renderSignedIn(payload);
        if(authCompleted()){clearAuthCompleteParam();}
      }else{
        renderAnon("Log in");
      }
    }catch(_){
      if(!chip)return;
      chip.innerHTML='<a href="'+signInHref()+'">Log in</a> <span class="muted">&middot; session unavailable</span>';
    }
  }
  if(document.readyState==="loading"){document.addEventListener("DOMContentLoaded",load);}else{load();}
})();
</script>`

// Canonical site-wide footer. Injected just before </body> on every page. The
// address is the single SupportEmail constant; do not duplicate it.
const shellFooterTmpl = `<footer class="site-footer">
  <span>Need help? <a href="mailto:%SUPPORT_EMAIL%">Email support</a></span>
</footer>`

func shellFooter() string {
	return strings.ReplaceAll(shellFooterTmpl, "%SUPPORT_EMAIL%", SupportEmail)
}

// active = "streams" | "recording" | "" (none).
func injectShell(page []byte, active string) []byte {
	topbar := shellTopbarTmpl
	topbar = strings.Replace(topbar, "%ACTIVE_STREAMS%", boolActive(active == "streams"), 1)
	topbar = strings.Replace(topbar, "%ACTIVE_RECORDING%", boolActive(active == "recording"), 1)
	out := strings.Replace(string(page), "<!--SHELL_HEAD-->", shellHeadCSS, 1)
	out = strings.Replace(out, "<!--SHELL_TOPBAR-->", topbar, 1)
	out = strings.Replace(out, "</body>", shellFooter()+"\n</body>", 1)
	return []byte(out)
}

func boolActive(on bool) string {
	if on {
		return "active"
	}
	return ""
}
