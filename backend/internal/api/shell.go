package api

import "strings"

// Canonical topbar CSS. Injected at <!--SHELL_HEAD--> (inside each page's <head>,
// before </style> is fine — these are standalone rules). Single source of truth.
const shellHeadCSS = `
<style id="shell-css">
.topbar{display:grid;grid-template-columns:minmax(0,1fr) auto auto;align-items:center;gap:12px;margin-bottom:16px;min-height:40px;}
.topbar-left{display:flex;align-items:center;gap:10px;min-width:0;overflow:hidden;}
.topbar-center{display:flex;align-items:center;justify-content:center;min-width:0;}
.topbar-right{display:flex;align-items:center;gap:10px;justify-content:flex-end;position:relative;}
.site-title{font-family:var(--mono);font-size:15px;font-weight:600;letter-spacing:0.05em;color:var(--accent);text-decoration:none;white-space:nowrap;}
.metrics-row{display:flex;flex-wrap:nowrap;gap:6px;min-width:0;overflow:hidden;}
.global-nav{display:inline-flex;align-items:center;gap:8px;padding:4px;border:1px solid var(--border);border-radius:999px;background:color-mix(in srgb,var(--panel) 90%,#000);}
.global-nav a{display:inline-flex;align-items:center;gap:6px;padding:7px 12px;border-radius:999px;border:1px solid transparent;font-family:var(--mono);font-size:11px;letter-spacing:0.04em;text-transform:uppercase;color:var(--muted);text-decoration:none;transition:background 120ms ease,color 120ms ease,border-color 120ms ease;}
.global-nav a:hover{color:var(--text);border-color:color-mix(in srgb,var(--accent) 35%,var(--border));background:color-mix(in srgb,var(--panel2) 78%,transparent);text-decoration:none;}
.global-nav a.active{color:#fff;background:var(--accent);border-color:var(--accent);}
.topbar-docs-link{font-family:var(--mono);font-size:11px;letter-spacing:0.04em;text-transform:uppercase;color:var(--muted);text-decoration:none;white-space:nowrap;padding:0 4px;}
.topbar-docs-link:hover{color:var(--text);text-decoration:none;}
.account-chip{display:inline-flex;align-items:center;gap:7px;min-height:36px;padding:0 12px;border-radius:999px;border:1px solid var(--border);background:color-mix(in srgb,var(--panel) 92%,#000);color:var(--muted);font-family:var(--mono);font-size:11px;white-space:nowrap;}
.account-chip a{color:var(--text);text-decoration:none;}
.account-chip a:hover{text-decoration:underline;}
.account-chip .caret{font-size:9px;opacity:0.8;}
.admin-chip{display:none;align-items:center;justify-content:center;width:34px;height:34px;border-radius:999px;border:1px solid var(--border);background:color-mix(in srgb,var(--panel) 92%,#000);color:var(--text);font-family:var(--mono);font-size:13px;text-decoration:none;flex:0 0 auto;}
.admin-chip:hover{text-decoration:none;border-color:color-mix(in srgb,var(--accent) 35%,var(--border));color:var(--accent);}
@media (max-width:720px){.topbar{grid-template-columns:minmax(0,1fr);overflow-x:auto;}.topbar-center{justify-content:flex-start;}.topbar-right{justify-content:flex-start;}}
</style>`

// Canonical topbar markup. Injected at <!--SHELL_TOPBAR--> (first child of <body>).
// %ACTIVE_STREAMS% / %ACTIVE_RECORDING% become " active" or "" per page.
const shellTopbarTmpl = `<div class="topbar">
  <div class="topbar-left">
    <a href="/" class="site-title">STO-A-RAMA</a>
    <div class="metrics-row" id="metricsRow"></div>
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
</div>`

// active = "streams" | "recording" | "" (none).
func injectShell(page []byte, active string) []byte {
	topbar := shellTopbarTmpl
	topbar = strings.Replace(topbar, "%ACTIVE_STREAMS%", boolActive(active == "streams"), 1)
	topbar = strings.Replace(topbar, "%ACTIVE_RECORDING%", boolActive(active == "recording"), 1)
	out := strings.Replace(string(page), "<!--SHELL_HEAD-->", shellHeadCSS, 1)
	out = strings.Replace(out, "<!--SHELL_TOPBAR-->", topbar, 1)
	return []byte(out)
}

func boolActive(on bool) string {
	if on {
		return "active"
	}
	return ""
}
