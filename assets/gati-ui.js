/* GATI shared UI: mobile nav toggle + per-chart download menu (PNG or CSV).
 *
 * Mark any card with  data-chart="slug"  and register a CSV provider:
 *   GATI_UI.registerChart('slug', () => ({ header:[...], rows:[[...]], suffix:'90d' }));
 * The provider runs at click time, so the file always matches what is on screen
 * (current filters, range and sort).
 */
(function () {
  'use strict';

  /* ---------- mobile nav ---------- */
  document.addEventListener('DOMContentLoaded', function () {
    var btn = document.querySelector('.nav-toggle'), nav = document.getElementById('siteNav');
    if (btn && nav) {
      btn.addEventListener('click', function () {
        var open = nav.classList.toggle('open');
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      });
    }
    initDownloads();
  });

  /* ---------- helpers ---------- */
  var providers = {};
  var H2C_SRC = 'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js';
  var h2cPromise = null;

  function loadHtml2Canvas() {
    if (window.html2canvas) return Promise.resolve(window.html2canvas);
    if (!h2cPromise) {
      h2cPromise = new Promise(function (resolve, reject) {
        var s = document.createElement('script');
        s.src = H2C_SRC; s.async = true;
        s.onload = function () { resolve(window.html2canvas); };
        s.onerror = function () { h2cPromise = null; reject(new Error('Could not load the image exporter')); };
        document.head.appendChild(s);
      });
    }
    return h2cPromise;
  }

  function stamp() { return new Date().toISOString().slice(0, 10); }

  function fileName(slug, suffix, ext) {
    return ['gsm', slug, suffix, stamp()].filter(Boolean).join('-')
      .toLowerCase().replace(/[^a-z0-9._-]+/g, '-') + '.' + ext;
  }

  function saveBlob(blob, name) {
    var url = URL.createObjectURL(blob), a = document.createElement('a');
    a.href = url; a.download = name; document.body.appendChild(a); a.click(); a.remove();
    setTimeout(function () { URL.revokeObjectURL(url); }, 1500);
  }

  function csvCell(v) {
    if (v == null) return '';
    var s = String(v);
    // neutralise spreadsheet formulas in text cells (numbers pass through untouched)
    if (typeof v === 'string' && /^[=+\-@\t\r]/.test(s) && isNaN(Number(s))) s = "'" + s;
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }

  function toCSV(header, rows) {
    return [header].concat(rows).map(function (r) { return r.map(csvCell).join(','); }).join('\r\n');
  }

  /* ---------- exporters ---------- */
  function downloadCSV(slug) {
    var p = providers[slug]; if (!p) return false;
    var d = p();
    if (!d || !d.rows || !d.rows.length) return false;
    // BOM so Excel reads UTF-8 (en dashes, accented names) correctly
    saveBlob(new Blob(['\uFEFF' + toCSV(d.header, d.rows)], { type: 'text/csv;charset=utf-8' }),
      fileName(slug, d.suffix, 'csv'));
    return true;
  }

  function downloadPNG(card, slug) {
    var suffix = (providers[slug] && (providers[slug]() || {}).suffix) || '';
    return loadHtml2Canvas().then(function (h2c) {
      return h2c(card, {
        scale: Math.max(2, Math.min(3, window.devicePixelRatio || 2)),
        backgroundColor: '#ffffff',
        useCORS: true,
        logging: false,
        ignoreElements: function (el) { return el.hasAttribute && el.hasAttribute('data-export-ignore'); },
        onclone: function (doc, clone) {
          // show the whole heatmap, not just its scrolled viewport
          clone.querySelectorAll('.hm-wrap').forEach(function (el) { el.style.overflow = 'visible'; });
          // small credit line so a shared image keeps its source
          var cr = doc.createElement('div');
          cr.textContent = 'GATI Foundation · Global Skills Mobility News Tracker · ' + stamp();
          cr.style.cssText = 'margin-top:12px;padding-top:8px;border-top:1px solid #dfe9e8;font:11px/1.4 Inter,system-ui,sans-serif;color:#5f7574;';
          clone.appendChild(cr);
        }
      });
    }).then(function (canvas) {
      return new Promise(function (resolve, reject) {
        canvas.toBlob(function (b) { b ? resolve(b) : reject(new Error('Image export failed')); }, 'image/png');
      });
    }).then(function (blob) { saveBlob(blob, fileName(slug, suffix, 'png')); });
  }

  /* ---------- menu UI ---------- */
  var ICON = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4v11M7 11l5 5 5-5M5 20h14"/></svg>';
  var openMenu = null;

  function closeMenu(returnFocus) {
    if (!openMenu) return;
    openMenu.menu.hidden = true;
    openMenu.btn.setAttribute('aria-expanded', 'false');
    if (returnFocus) openMenu.btn.focus();
    openMenu = null;
  }

  function build(card) {
    if (card.querySelector('.dl-wrap')) return;
    var slug = card.getAttribute('data-chart');
    var title = (card.querySelector('h2') || {}).textContent || 'chart';

    var wrap = document.createElement('div');
    wrap.className = 'dl-wrap';
    wrap.setAttribute('data-export-ignore', '');

    var btn = document.createElement('button');
    btn.type = 'button'; btn.className = 'dl-btn';
    btn.setAttribute('aria-haspopup', 'menu'); btn.setAttribute('aria-expanded', 'false');
    btn.setAttribute('aria-label', 'Download chart: ' + title.trim());
    btn.title = 'Download chart';
    btn.innerHTML = ICON;

    var menu = document.createElement('div');
    menu.className = 'dl-menu'; menu.setAttribute('role', 'menu'); menu.hidden = true;
    menu.innerHTML =
      '<button type="button" role="menuitem" data-fmt="png">' + ICON.replace('<svg ', '<svg width="15" height="15" style="fill:none;stroke:currentColor;stroke-width:2" ') + 'Download as PNG</button>' +
      '<button type="button" role="menuitem" data-fmt="csv">' + ICON.replace('<svg ', '<svg width="15" height="15" style="fill:none;stroke:currentColor;stroke-width:2" ') + 'Download as CSV</button>';

    wrap.appendChild(btn); wrap.appendChild(menu);

    var tools = card.querySelector('.card-tools');
    if (tools) tools.appendChild(wrap);
    else { wrap.classList.add('dl-abs'); card.appendChild(wrap); }

    btn.addEventListener('click', function (e) {
      e.stopPropagation();
      if (openMenu && openMenu.btn === btn) { closeMenu(false); return; }
      closeMenu(false);
      var old = menu.querySelector('.dl-note'); if (old) old.remove();
      menu.hidden = false; btn.setAttribute('aria-expanded', 'true');
      openMenu = { btn: btn, menu: menu };
      menu.querySelector('button').focus();
    });

    menu.addEventListener('click', function (e) {
      var item = e.target.closest('button[data-fmt]'); if (!item) return;
      e.stopPropagation();
      var fmt = item.getAttribute('data-fmt');
      if (fmt === 'csv') {
        if (downloadCSV(slug)) closeMenu(true);
        else note(menu, 'Nothing to export yet.');
      } else {
        closeMenu(false);
        btn.classList.add('busy');
        downloadPNG(card, slug)
          .catch(function (err) { console.error(err); alert('Could not create the image. Check your connection and try again.'); })
          .then(function () { btn.classList.remove('busy'); btn.focus(); });
      }
    });

    menu.addEventListener('keydown', function (e) {
      var items = Array.prototype.slice.call(menu.querySelectorAll('button[data-fmt]'));
      var i = items.indexOf(document.activeElement);
      if (e.key === 'ArrowDown') { e.preventDefault(); items[(i + 1) % items.length].focus(); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); items[(i - 1 + items.length) % items.length].focus(); }
    });
  }

  function note(menu, text) {
    var n = menu.querySelector('.dl-note');
    if (!n) { n = document.createElement('div'); n.className = 'dl-note'; n.setAttribute('role', 'status'); menu.appendChild(n); }
    n.textContent = text;
  }

  function initDownloads() {
    document.querySelectorAll('.card[data-chart]').forEach(build);
    document.addEventListener('click', function (e) {
      if (openMenu && !openMenu.menu.contains(e.target)) closeMenu(false);
    });
    document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeMenu(true); });
  }

  window.GATI_UI = { registerChart: function (slug, fn) { providers[slug] = fn; } };
})();
