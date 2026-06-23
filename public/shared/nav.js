// ─────────────────────────────────────────────────────────────────────────
// NITM Ops Hub — shared top navigation.
//
// SINGLE SOURCE OF TRUTH for the nav across every module page. To add, rename,
// remove, or reorder a module link, edit MODULES below — every page updates.
//
// Each page just needs:  <script defer src="/shared/nav.js"></script>
// This script removes any old hard-coded top nav and injects the canonical one,
// highlighting the active module based on the current URL.
// ─────────────────────────────────────────────────────────────────────────
(function () {
  var MODULES = [
    { href: "/", label: "Hub" },
    { href: "/calendar/", label: "Calendar" },
    { href: "/inventory/", label: "Inventory" },
    { href: "/3pl/", label: "3PL" },
    { href: "/tracker/", label: "Tracker" },
    { href: "/social/", label: "Social" },
    { href: "/ambassadors/", label: "Ambassadors" },
    { href: "/growth/", label: "Growth" },
    { href: "/icp/", label: "ICP" },
    { href: "/cx-agent/", label: "CX Agent" },
    { href: "/stage/", label: "Stage Timing" },
    { href: "/campaign-router/", label: "Campaign Router" },
  ];

  var path = location.pathname;
  function isActive(href) {
    if (href === "/") return path === "/" || path === "";
    return path === href || path.indexOf(href) === 0;
  }

  var css = "" +
    ".nitm-nav{background:linear-gradient(180deg,#1a2332 0%,#141c28 100%);" +
      "padding:11px 24px;display:flex;align-items:center;gap:22px;" +
      "border-bottom:1px solid #1e293b;font-family:'DM Sans',system-ui,-apple-system,sans-serif;" +
      "overflow-x:auto;white-space:nowrap;position:sticky;top:0;z-index:1000;}" +
    ".nitm-nav::-webkit-scrollbar{height:0;}" +
    ".nitm-nav .nitm-brand{color:#f1f5f9;font-weight:700;font-size:14px;text-decoration:none;letter-spacing:.5px;flex:0 0 auto;}" +
    ".nitm-nav a:not(.nitm-brand){color:#64748b;text-decoration:none;font-size:13px;font-weight:500;transition:color .15s;flex:0 0 auto;}" +
    ".nitm-nav a:not(.nitm-brand):hover{color:#cbd5e1;}" +
    ".nitm-nav a.nitm-active{color:#f1f5f9;}";

  var style = document.createElement("style");
  style.textContent = css;
  document.head.appendChild(style);

  function build() {
    // Remove any pre-existing top nav (old hard-coded ones, or a prior inject).
    var moduleLink = /href="\/(calendar|inventory|3pl|tracker|social|ambassadors|growth|icp|cx-agent|stage|campaign-router)\/?"/;
    Array.prototype.slice.call(document.querySelectorAll("nav")).forEach(function (n) {
      if (n.classList.contains("nitm-nav")) { n.remove(); return; }
      if (n.classList.contains("top-nav") || moduleLink.test(n.innerHTML)) n.remove();
    });

    var nav = document.createElement("nav");
    nav.className = "nitm-nav";
    var html = '<a class="nitm-brand" href="/">NITM Ops</a>';
    MODULES.forEach(function (m) {
      if (m.href === "/") return; // brand already links to the hub
      html += '<a href="' + m.href + '"' + (isActive(m.href) ? ' class="nitm-active"' : "") + ">" + m.label + "</a>";
    });
    nav.innerHTML = html;
    document.body.insertBefore(nav, document.body.firstChild);
  }

  if (document.body) build();
  else document.addEventListener("DOMContentLoaded", build);
})();
