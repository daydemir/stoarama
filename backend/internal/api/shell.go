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
.account-chip .caret{font-size:9px;opacity:0.8;cursor:pointer;}
.org-switch{display:inline-flex;align-items:center;gap:5px;cursor:pointer;color:var(--text);}
.org-menu{position:absolute;top:40px;right:0;min-width:200px;padding:6px;border:1px solid var(--border);border-radius:10px;background:color-mix(in srgb,var(--panel) 96%,#000);box-shadow:0 8px 24px rgba(0,0,0,0.35);z-index:50;display:none;flex-direction:column;gap:2px;}
.org-menu.open{display:flex;}
.org-menu .org-item{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:7px 10px;border-radius:7px;color:var(--text);cursor:pointer;font-size:11px;text-align:left;background:none;border:none;font-family:var(--mono);width:100%;}
.org-menu .org-item:hover{background:color-mix(in srgb,var(--panel2) 80%,transparent);}
.org-menu .org-item.current{color:var(--accent);}
.org-menu .org-role{color:var(--muted);font-size:9px;text-transform:uppercase;letter-spacing:0.04em;}
.org-menu .org-sep{height:1px;margin:4px 2px;background:var(--border);}
.org-menu .org-create{color:var(--muted);}
.org-menu .org-create:hover{color:var(--text);}
.admin-chip{display:none;align-items:center;justify-content:center;width:34px;height:34px;border-radius:999px;border:1px solid var(--border);background:color-mix(in srgb,var(--panel) 92%,#000);color:var(--text);font-family:var(--mono);font-size:13px;text-decoration:none;flex:0 0 auto;}
.admin-chip:hover{text-decoration:none;border-color:color-mix(in srgb,var(--accent) 35%,var(--border));color:var(--accent);}
@media (max-width:720px){.topbar{grid-template-columns:auto 1fr;grid-template-areas:"brand utils" "nav nav";row-gap:10px;column-gap:10px;align-items:center;}.topbar-left{grid-area:brand;}.topbar-right{grid-area:utils;justify-self:end;}.topbar-center{grid-area:nav;justify-self:stretch;}.global-nav{width:100%;justify-content:center;}}
.site-footer{margin:40px auto 24px;max-width:1200px;padding:16px 4px 0;border-top:1px solid var(--border);display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:10px;font-family:var(--mono);font-size:11px;letter-spacing:0.03em;color:var(--muted);}
.site-footer a{color:var(--muted);text-decoration:none;}
.site-footer a:hover{color:var(--text);text-decoration:underline;}
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
      <a href="/recordings" class="%ACTIVE_RECORDING%">Recording</a>
    </nav>
  </div>
  <div class="topbar-right" id="accountArea">
    <a class="topbar-docs-link" href="/docs/getting-started">Docs</a>
    <a id="topbarAdminLink" class="admin-chip" href="/admin" aria-label="Admin">&#9881;</a>
    <div class="account-chip" id="topbarAccountStatus">Checking session...</div>
  </div>
</div>` + shellTopbarJS

// Canonical topbar BEHAVIOR. One definition for every page: fetch the session,
// render the account chip + admin gear identically, so the account control works
// on every tab including Recording. Wrapped in an IIFE that runs from its own
// script element, independent of any page-body script, so a page-body JS error
// cannot break the topbar. References only the shared #topbar* ids from the markup
// above. Do not duplicate this logic in any page.
const shellTopbarJS = `
<script>
(function(){
  function esc(v){return String(v==null?"":v).replace(/[&<>"']/g,function(c){return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c];});}
  var chip=document.getElementById("topbarAccountStatus");
  var gear=document.getElementById("topbarAdminLink");
  if(gear){gear.style.display="none";}
  function redirectPath(){
    try{return location.pathname+(location.search||"");}catch(_){return "/";}
  }
  function signInHref(){
    return "/account?redirect_path="+encodeURIComponent(redirectPath());
  }
  function renderAnon(label){
    if(!chip)return;
    chip.innerHTML='<a href="'+signInHref()+'">'+(label||"Log in")+'</a>';
  }
  function switchOrg(id){
    fetch("/api/v1/account/orgs/"+encodeURIComponent(id)+"/switch",{method:"POST",credentials:"same-origin",headers:{Accept:"application/json"}})
      .then(function(res){if(!res.ok){throw new Error("status "+res.status);}location.reload();})
      .catch(function(){});
  }
  function createOrg(){
    var name=window.prompt("Name your new org:");
    if(name==null)return;
    name=String(name).trim();
    if(!name)return;
    fetch("/api/v1/account/orgs",{method:"POST",credentials:"same-origin",headers:{Accept:"application/json","Content-Type":"application/json"},body:JSON.stringify({name:name})})
      .then(function(res){return res.json().then(function(d){if(!res.ok){throw new Error(d.error||("status "+res.status));}return d;});})
      .then(function(d){switchOrg(d.id);})
      .catch(function(){});
  }
  function renderSignedIn(payload){
    if(!chip)return;
    var acct=payload.account||{};
    var authType=String(payload.session&&payload.session.auth_type||acct.auth_type||"").trim();
    var email=String(acct.email||"").trim()||"Account";
    var currentOrg=payload.current_org||{};
    var orgs=Array.isArray(payload.orgs)?payload.orgs:[];
    var currentId=Number(currentOrg.id||acct.id||0);
    var orgName=String(currentOrg.name||"").trim();
    // The switcher shows only when the user actually belongs to >1 org, or when
    // an org name is present (so a single-org user still sees which org they are in).
    var hasSwitcher=orgs.length>0;
    var label='<a href="/account">'+esc(email)+'</a>';
    if(hasSwitcher){
      var items=orgs.map(function(o){
        var oid=Number(o.id);
        var cur=oid===currentId?' current':'';
        return '<button type="button" class="org-item'+cur+'" data-org="'+oid+'"><span>'+esc(o.name)+'</span><span class="org-role">'+esc(o.role||"")+'</span></button>';
      }).join('');
      label='<span class="org-switch" id="orgSwitch">'+esc(orgName||email)+' <span class="caret">&#9662;</span></span>'
        +'<div class="org-menu" id="orgMenu">'+items
        +'<div class="org-sep"></div>'
        +'<button type="button" class="org-item org-create" id="orgCreate">+ New org</button>'
        +'</div>';
    }
    chip.innerHTML=label+(authType?' <span class="muted">&middot; '+esc(authType)+'</span>':'');
    if(hasSwitcher){
      var sw=document.getElementById("orgSwitch");
      var menu=document.getElementById("orgMenu");
      if(sw&&menu){
        sw.addEventListener("click",function(e){e.stopPropagation();menu.classList.toggle("open");});
        document.addEventListener("click",function(){menu.classList.remove("open");});
        menu.addEventListener("click",function(e){e.stopPropagation();});
        menu.querySelectorAll(".org-item[data-org]").forEach(function(btn){
          btn.addEventListener("click",function(){var id=Number(btn.getAttribute("data-org"));if(id&&id!==currentId){switchOrg(id);}else{menu.classList.remove("open");}});
        });
        var createBtn=document.getElementById("orgCreate");
        if(createBtn){createBtn.addEventListener("click",createOrg);}
      }
    }
    if(gear&&String(acct.role||"").trim().toLowerCase()==="admin"){gear.style.display="inline-flex";}
  }
  function load(){
    if(chip){chip.textContent="Checking session...";}
    var ctrl=new AbortController();
    var to=setTimeout(function(){ctrl.abort();},8000);
    fetch("/api/v1/account/me",{credentials:"same-origin",headers:{Accept:"application/json"},signal:ctrl.signal})
      .then(function(res){
        clearTimeout(to);
        if(res.status===401){renderAnon("Log in");return null;}
        if(!res.ok){throw new Error("status "+res.status);}
        return res.json();
      })
      .then(function(payload){
        if(!payload)return;
        var acct=payload.account||{};
        if(payload.authenticated&&acct&&acct.email){
          renderSignedIn(payload);
        }else{
          renderAnon("Log in");
        }
      })
      .catch(function(){
        clearTimeout(to);
        if(!chip)return;
        chip.innerHTML='<a href="'+signInHref()+'">Log in</a> <span class="muted">&middot; session unavailable</span>';
      });
  }
  if(document.readyState==="loading"){document.addEventListener("DOMContentLoaded",load);}else{load();}
})();
</script>`

// Canonical site-wide footer. Injected just before </body> on every page. The
// address is the single SupportEmail constant; do not duplicate it.
const shellFooterTmpl = `<footer class="site-footer">
  <span>Need help? <a href="mailto:%SUPPORT_EMAIL%">%SUPPORT_EMAIL%</a></span>
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
