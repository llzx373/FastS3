/* Sidebar language switch: English is default, Chinese under /zh/. */
(function () {
  var search = document.querySelector(".wy-side-nav-search");
  if (!search || search.querySelector(".lang-switch")) return;
  var p = document.createElement("p");
  p.className = "lang-switch";
  p.style.cssText = "margin:8px 0 0;font-size:13px";
  p.innerHTML = '<a href="/">English</a> · <a href="/zh/">中文</a>';
  search.appendChild(p);
})();
