/* GSM News Tracker — shared data + filter module
 * Used by feed.html and analysis.html. Reads ./data/items.csv, normalises it,
 * and drives the filter sidebar (state is mirrored in the URL so a filtered
 * view can be shared or carried between pages).
 */
window.GATI = (function () {
  'use strict';

  /* ------------------------------------------------------------ constants */
  const SENT = {
    positive: { label: 'Positive Developments', color: '#2f9e6e', arrow: '▲' },
    neutral:  { label: 'Neutral Developments',  color: '#f2a93c', arrow: '●' },
    negative: { label: 'Negative Developments', color: '#e05353', arrow: '▼' }
  };
  const SENT_ORDER = ['positive', 'neutral', 'negative'];

  const THEMES = [
    { name: 'Visas and Work Permits',     short: 'Visas' },
    { name: 'Skills and Talent',          short: 'Skills' },
    { name: 'Labour and Workers',         short: 'Labour' },
    { name: 'Students and Education',     short: 'Students' },
    { name: 'Bilateral Deals and Trade',  short: 'Bilateral' },
    { name: 'Enforcement and Crisis',     short: 'Enforcement' },
    { name: 'Remittances and Diaspora',   short: 'Diaspora' },
    { name: 'Demographics and Workforce', short: 'Demographics' },
    { name: 'Other',                      short: 'Other' }
  ];
  const THEME_SHORT = Object.fromEntries(THEMES.map(t => [t.name, t.short]));

  const BUCKETS = ['India Specific', 'Destination Countries', 'Competitor Countries',
                   'Demographics & Fertility', 'Global & Multilateral'];

  // Places that are not "destination countries" in the country charts
  const NON_DEST = new Set(['India', 'Global', 'Unspecified']);

  const COUNTRY_ALIAS = {
    'usa': 'United States', 'us': 'United States', 'u.s.': 'United States', 'united states of america': 'United States',
    'uk': 'United Kingdom', 'u.k.': 'United Kingdom', 'britain': 'United Kingdom',
    'eu': 'European Union', 'europe': 'European Union',
    'gulf': 'Gulf Region', 'gcc': 'Gulf Region', 'persian gulf': 'Gulf Region', 'gulf region': 'Gulf Region',
    'united arab emirates': 'UAE', 'korea': 'South Korea', 'republic of korea': 'South Korea',
    'czechia': 'Czech Republic', 'nz': 'New Zealand', 'global': 'Global'
  };

  // Sources the current prompt explicitly EXCLUDES (aggregators / SEO / lead-gen / exam-coaching)
  const EXCLUDED_TOKENS = ['visahq', 'visaverge', 'travelandtourworld', 'visasupdate', 'worldvisaacademy',
    'y-axis', 'yaxis', 'terratern', 'radvisionworld', 'immigrationnewscanada', 'thevisaexperts',
    'migrantimes', 'indianeagle', 'civilsdaily', 'sanskritiias'];

  /* -------------------------------------------------------------- helpers */
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  function decode(s) {
    return String(s || '')
      .replace(/<(https?:[^|>]+)\|([^>]+)>/g, '$2')   // Slack <url|label>
      .replace(/<(https?:[^>]+)>/g, '$1')              // Slack <url>
      .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
      .replace(/&#0?39;/g, "'").replace(/&amp;/g, '&')
      .trim();
  }
  const cleanUrl = u => decode(u).replace(/[<>]/g, '').split('|')[0].trim();

  // Mirrors fetch_and_process.py's normalize_vibe(): strips invisible
  // Unicode format characters (zero-width space/joiner, BOM, soft hyphen...)
  // that can ride along in an LLM- or Slack-produced cell and defeat a
  // plain string match, then matches on a 3-letter prefix. Belt-and-braces
  // alongside the same fix server-side, in case a row was written before
  // that fix landed, or by a source other than the pipeline.
  function normVibe(raw) {
    const cleaned = String(raw || '').replace(/[\u200B-\u200F\u00AD\uFEFF\u2060]/g, '').trim().toLowerCase();
    for (const canon of ['positive', 'negative', 'neutral']) { if (cleaned.startsWith(canon.slice(0, 3))) return canon; }
    return '';
  }

  const hostOf = u => { try { return new URL(u).hostname.replace(/^www\./, ''); } catch (e) { return ''; } };
  const normKey = s => String(s || '').toLowerCase().replace(/^update:\s*/, '').replace(/[^a-z0-9]/g, '');

  function fmtDate(d) {
    return new Date(d + 'T00:00:00').toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
  }
  function fmtDateYear(d) {
    return new Date(d + 'T00:00:00').toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  }
  function fmtDateLong(d) {
    return new Date(d + 'T00:00:00').toLocaleDateString('en-GB', { weekday: 'long', day: 'numeric', month: 'short', year: 'numeric' });
  }
  function addDays(iso, n) {
    const d = new Date(iso + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10);
  }
  function weekStart(iso) {
    const d = new Date(iso + 'T00:00:00Z'); const day = (d.getUTCDay() + 6) % 7;
    d.setUTCDate(d.getUTCDate() - day); return d.toISOString().slice(0, 10);
  }
  const fmtPP = v => (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(Math.round(v)) + ' pp';

  function tally(items) {
    const t = { positive: 0, neutral: 0, negative: 0, n: items.length };
    items.forEach(i => { t[i.vibe]++; });
    return t;
  }
  const net = t => t.n ? ((t.positive - t.negative) / t.n) * 100 : 0;

  /* ------------------------------------------------------------ CSV parse */
  function parseCSV(text) {
    if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
    const rows = []; let row = [], f = '', q = false;
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (q) {
        if (c === '"') { if (text[i + 1] === '"') { f += '"'; i++; } else q = false; } else f += c;
      } else if (c === '"') q = true;
      else if (c === ',') { row.push(f); f = ''; }
      else if (c === '\n') { row.push(f); rows.push(row); row = []; f = ''; }
      else if (c === '\r') { /* skip */ }
      else f += c;
    }
    if (f !== '' || row.length) { row.push(f); rows.push(row); }
    const head = rows.shift().map(h => h.trim().toLowerCase());
    return rows.filter(r => r.length > 1).map(r => Object.fromEntries(head.map((h, i) => [h, r[i] || ''])));
  }

  function normTheme(raw) {
    const k = decode(raw).toLowerCase().replace(/&/g, 'and').replace(/\s+/g, ' ').trim();
    const hit = THEMES.find(t => t.name.toLowerCase() === k);
    return hit ? hit.name : (k ? 'Other' : 'Untagged');
  }
  function normBucket(tok) {
    const k = decode(tok).toLowerCase();
    if (k.includes('india')) return 'India Specific';
    if (k.includes('destination')) return 'Destination Countries';
    if (k.includes('competitor')) return 'Competitor Countries';
    if (k.includes('demograph') || k.includes('fertil')) return 'Demographics & Fertility';
    if (k.includes('global') || k.includes('multilateral')) return 'Global & Multilateral';
    return null;
  }
  function normCountry(tok) {
    const t = decode(tok).trim(); if (!t) return null;
    return COUNTRY_ALIAS[t.toLowerCase()] || t;
  }

  function normalise(rows) {
    const items = []; let ignored = 0;
    rows.forEach((r, idx) => {
      const headline = decode(r.headline);
      const url = cleanUrl(r.url);
      const date = (r.date || '').trim();
      if (!headline || /^quiet news day/i.test(headline) || !/^https?:/i.test(url) || !/^\d{4}-\d{2}-\d{2}$/.test(date)) { ignored++; return; }
      const host = hostOf(url);
      const source = decode(r.source) || host;
      const vibe = normVibe(r.vibe) || 'neutral';
      const buckets = [...new Set(decode(r.categories).split('|').map(normBucket).filter(Boolean))];
      const countries = [...new Set(decode(r.countries).split('|').map(normCountry).filter(Boolean))];
      const exKey = host + ' ' + source.toLowerCase().replace(/[\s.\-]/g, '');
      items.push({
        id: idx, date, headline, url, host, source, vibe,
        summary: decode(r.summary),
        top: /^(true|t|1|yes)$/i.test((r.in_top7 || '').trim()),
        theme: normTheme(r.theme),
        countries: countries.length ? countries : ['Unspecified'],
        buckets: buckets.length ? buckets : [],
        excluded: EXCLUDED_TOKENS.some(t => exKey.includes(t)),
        repeat: false
      });
    });
    // flag later repeats of an identical headline
    const seen = new Set();
    [...items].sort((a, b) => a.date.localeCompare(b.date) || a.id - b.id).forEach(i => {
      const k = normKey(i.headline); if (seen.has(k)) i.repeat = true; seen.add(k);
    });
    items.sort((a, b) => b.date.localeCompare(a.date) || a.id - b.id);
    return { items, ignored };
  }

  async function loadItems() {
    const res = await fetch('./data/items.csv', { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const out = normalise(parseCSV(await res.text()));
    out.latest = out.items.length ? out.items[0].date : null;
    return out;
  }

  /* --------------------------------------------------------------- filters */
  const FACETS = {
    sent:    i => [i.vibe],
    theme:   i => [i.theme],
    bucket:  i => i.buckets,
    country: i => i.countries,
    source:  i => [i.source]
  };
  const URLKEY = { q: 'q', from: 'from', to: 'to', sent: 's', theme: 't', bucket: 'b', country: 'c',
                   source: 'src', top: 'top', dedupe: 'nodup', noexcl: 'noexcl' };

  const blankState = () => ({ q: '', from: '', to: '', sent: new Set(), theme: new Set(), bucket: new Set(),
    country: new Set(), source: new Set(), top: false, dedupe: false, noexcl: false });

  function readState() {
    const st = blankState(); const p = new URLSearchParams(location.search);
    st.q = p.get(URLKEY.q) || ''; st.from = p.get(URLKEY.from) || ''; st.to = p.get(URLKEY.to) || '';
    Object.keys(FACETS).forEach(f => { (p.get(URLKEY[f]) || '').split('|').filter(Boolean).forEach(v => st[f].add(v)); });
    st.top = p.get(URLKEY.top) === '1'; st.dedupe = p.get(URLKEY.dedupe) === '1'; st.noexcl = p.get(URLKEY.noexcl) === '1';
    return st;
  }
  function writeState(st, extra) {
    const p = new URLSearchParams();
    if (st.q) p.set(URLKEY.q, st.q);
    if (st.from) p.set(URLKEY.from, st.from);
    if (st.to) p.set(URLKEY.to, st.to);
    Object.keys(FACETS).forEach(f => { if (st[f].size) p.set(URLKEY[f], [...st[f]].join('|')); });
    if (st.top) p.set(URLKEY.top, '1'); if (st.dedupe) p.set(URLKEY.dedupe, '1'); if (st.noexcl) p.set(URLKEY.noexcl, '1');
    Object.entries(extra || {}).forEach(([k, v]) => { if (v) p.set(k, v); });
    const qs = p.toString();
    history.replaceState(null, '', location.pathname + (qs ? '?' + qs : ''));
    document.querySelectorAll('a[data-carry]').forEach(a => { a.href = a.dataset.carry + (qs ? '?' + qs : ''); });
  }

  function passes(i, st, skip) {
    if (st.q) {
      const hay = (i.headline + ' ' + i.summary + ' ' + i.source + ' ' + i.countries.join(' ')).toLowerCase();
      if (!st.q.toLowerCase().split(/\s+/).filter(Boolean).every(t => hay.includes(t))) return false;
    }
    if (st.from && i.date < st.from) return false;
    if (st.to && i.date > st.to) return false;
    for (const f in FACETS) {
      if (f === skip || !st[f].size) continue;
      if (!FACETS[f](i).some(v => st[f].has(v))) return false;
    }
    if (st.top && !i.top) return false;
    if (st.dedupe && i.repeat) return false;
    if (st.noexcl && i.excluded) return false;
    return true;
  }

  function createFilters(opts) {
    const items = opts.items, latest = opts.latest, mount = opts.mount;
    const st = readState();
    const ui = { moreCountries: false, moreSources: false, cq: '' };
    const totals = {};
    Object.keys(FACETS).forEach(f => {
      const m = new Map(); items.forEach(i => new Set(FACETS[f](i)).forEach(v => m.set(v, (m.get(v) || 0) + 1)));
      totals[f] = m;
    });
    const nRepeat = items.filter(i => i.repeat).length, nExcl = items.filter(i => i.excluded).length;

    mount.innerHTML = `
      <div class="f-head"><h3>Filters</h3><button type="button" class="f-reset" id="f-reset">Reset all</button></div>
      <div class="f-count" id="f-count"></div>
      <div class="f-sec"><input type="search" id="f-q" placeholder="Search headlines &amp; summaries" autocomplete="off"></div>
      <div class="f-sec"><h4>Period</h4>
        <div class="seg" id="f-range"><button type="button" data-r="7">7D</button><button type="button" data-r="30">30D</button><button type="button" data-r="90">90D</button><button type="button" data-r="all">All</button></div>
        <div class="f-dates"><label>From<input type="date" id="f-from"></label><label>To<input type="date" id="f-to"></label></div>
      </div>
      <div class="f-sec"><h4>Sentiment</h4><div id="f-sent"></div></div>
      <div class="f-sec"><h4>Theme</h4><div id="f-theme"></div></div>
      <div class="f-sec"><h4>Focus <span class="hint" title="The four buckets from the daily prompt, plus Global &amp; Multilateral">ⓘ</span></h4><div id="f-bucket"></div></div>
      <div class="f-sec"><h4>Country <span class="hint" title="'India' = India-only developments. India-linked stories abroad sit under the other country plus the India Specific focus.">ⓘ</span></h4>
        <input type="search" id="f-cq" placeholder="Find a country" autocomplete="off">
        <div id="f-country"></div><button type="button" class="f-more" id="f-cmore"></button></div>
      <div class="f-sec"><h4>Source</h4><div id="f-source"></div><button type="button" class="f-more" id="f-smore"></button></div>
      <div class="f-sec"><h4>Briefing &amp; data quality</h4>
        <label class="opt"><input type="checkbox" id="f-top"><span class="name">Main bulletin only (top 5–7)</span></label>
        <label class="opt"><input type="checkbox" id="f-dedupe"><span class="name">Hide repeated headlines</span><span class="n">${nRepeat}</span></label>
        <label class="opt"><input type="checkbox" id="f-noexcl"><span class="name">Hide sources on the exclude list</span><span class="n">${nExcl}</span></label>
      </div>`;
    const $ = s => mount.querySelector(s);

    function counts(f) {
      const m = new Map();
      items.forEach(i => { if (passes(i, st, f)) new Set(FACETS[f](i)).forEach(v => m.set(v, (m.get(v) || 0) + 1)); });
      return m;
    }
    function optRow(f, value, label, n, checked, dot) {
      return `<label class="opt${n === 0 && !checked ? ' zero' : ''}"><input type="checkbox" data-f="${f}" value="${esc(value)}"${checked ? ' checked' : ''}>` +
        (dot ? `<i class="dot" style="background:${dot}"></i>` : '') + `<span class="name">${esc(label)}</span><span class="n">${n}</span></label>`;
    }
    function orderedValues(f) { return [...totals[f].keys()].sort((a, b) => totals[f].get(b) - totals[f].get(a) || a.localeCompare(b)); }

    function renderLists() {
      const cs = counts('sent'), ct = counts('theme'), cb = counts('bucket'), cc = counts('country'), cso = counts('source');
      $('#f-sent').innerHTML = SENT_ORDER.map(k => optRow('sent', k, SENT[k].label, cs.get(k) || 0, st.sent.has(k), SENT[k].color)).join('');
      const themeVals = [...THEMES.map(t => t.name), ...(totals.theme.has('Untagged') ? ['Untagged'] : [])].filter(v => totals.theme.has(v) || st.theme.has(v));
      $('#f-theme').innerHTML = themeVals.map(v => optRow('theme', v, v, ct.get(v) || 0, st.theme.has(v))).join('');
      const bVals = BUCKETS.filter(v => totals.bucket.has(v) || st.bucket.has(v));
      $('#f-bucket').innerHTML = bVals.map(v => optRow('bucket', v, v, cb.get(v) || 0, st.bucket.has(v))).join('');

      let cVals = orderedValues('country'); const q = ui.cq.trim().toLowerCase();
      if (q) cVals = cVals.filter(v => v.toLowerCase().includes(q));
      const cLimit = (ui.moreCountries || q) ? Infinity : 10;
      const cShown = cVals.filter((v, i) => i < cLimit || st.country.has(v));
      $('#f-country').innerHTML = cShown.map(v => optRow('country', v, v, cc.get(v) || 0, st.country.has(v))).join('') || '<div class="f-empty">No match</div>';
      const cm = $('#f-cmore'); cm.style.display = (!q && cVals.length > 10) ? '' : 'none';
      cm.textContent = ui.moreCountries ? 'Show fewer' : `Show all ${cVals.length} countries`;

      const sVals = orderedValues('source'); const sLimit = ui.moreSources ? Infinity : 8;
      const sShown = sVals.filter((v, i) => i < sLimit || st.source.has(v));
      $('#f-source').innerHTML = sShown.map(v => optRow('source', v, v, cso.get(v) || 0, st.source.has(v))).join('');
      const sm = $('#f-smore'); sm.style.display = sVals.length > 8 ? '' : 'none';
      sm.textContent = ui.moreSources ? 'Show fewer' : `Show all ${sVals.length} sources`;
    }

    function syncInputs() {
      $('#f-q').value = st.q; $('#f-from').value = st.from; $('#f-to').value = st.to;
      $('#f-top').checked = st.top; $('#f-dedupe').checked = st.dedupe; $('#f-noexcl').checked = st.noexcl;
      const act = (!st.to && !st.from) ? 'all' : (!st.to ? ['7', '30', '90'].find(n => st.from === addDays(latest, -(Number(n) - 1))) : null);
      mount.querySelectorAll('#f-range button').forEach(b => b.classList.toggle('active', b.dataset.r === act));
    }

    const api = {
      state: st,
      filtered() { return items.filter(i => passes(i, st)); },
      isFiltered() { return !!(st.q || st.from || st.to || st.top || st.dedupe || st.noexcl || Object.keys(FACETS).some(f => st[f].size)); },
      toggle(f, v) { st[f].has(v) ? st[f].delete(v) : st[f].add(v); api.changed(); },
      setOnly(obj) { Object.keys(obj).forEach(f => { st[f].clear(); st[f].add(obj[f]); }); api.changed(); },
      remove(f, v) { if (f in FACETS) st[f].delete(v); else if (f === 'range') { st.from = ''; st.to = ''; } else st[f] = (typeof st[f] === 'boolean') ? false : ''; api.changed(); },
      chips() {
        const c = [];
        if (st.q) c.push({ label: `“${st.q}”`, f: 'q' });
        if (st.from || st.to) c.push({ label: `${st.from ? fmtDate(st.from) : 'start'} → ${st.to ? fmtDate(st.to) : 'latest'}`, f: 'range' });
        SENT_ORDER.forEach(k => { if (st.sent.has(k)) c.push({ label: SENT[k].label, f: 'sent', v: k }); });
        ['theme', 'bucket', 'country', 'source'].forEach(f => st[f].forEach(v => c.push({ label: v, f, v })));
        if (st.top) c.push({ label: 'Main bulletin only', f: 'top' });
        if (st.dedupe) c.push({ label: 'Repeats hidden', f: 'dedupe' });
        if (st.noexcl) c.push({ label: 'Excluded sources hidden', f: 'noexcl' });
        return c;
      },
      reset() { Object.assign(st, blankState()); api.changed(); },
      changed(fromInput) {
        renderLists(); if (!fromInput) syncInputs();
        const n = api.filtered().length;
        $('#f-count').innerHTML = `<b>${n}</b> of ${items.length} items` + (api.isFiltered() ? '' : ' · no filters');
        writeState(st, opts.extraUrl ? opts.extraUrl() : null);
        if (opts.onChange) opts.onChange(api);
      }
    };

    // ---- events
    mount.addEventListener('change', e => {
      const t = e.target;
      if (t.dataset && t.dataset.f) { api.toggle(t.dataset.f, t.value); return; }
      if (t.id === 'f-top') { st.top = t.checked; api.changed(); }
      else if (t.id === 'f-dedupe') { st.dedupe = t.checked; api.changed(); }
      else if (t.id === 'f-noexcl') { st.noexcl = t.checked; api.changed(); }
      else if (t.id === 'f-from') { st.from = t.value; api.changed(); }
      else if (t.id === 'f-to') { st.to = t.value; api.changed(); }
    });
    let timer;
    $('#f-q').addEventListener('input', e => { clearTimeout(timer); timer = setTimeout(() => { st.q = e.target.value.trim(); api.changed(true); }, 180); });
    $('#f-cq').addEventListener('input', e => { ui.cq = e.target.value; renderLists(); });
    $('#f-cmore').addEventListener('click', () => { ui.moreCountries = !ui.moreCountries; renderLists(); });
    $('#f-smore').addEventListener('click', () => { ui.moreSources = !ui.moreSources; renderLists(); });
    $('#f-reset').addEventListener('click', () => { ui.cq = ''; $('#f-cq').value = ''; api.reset(); });
    $('#f-range').addEventListener('click', e => {
      const r = e.target.dataset && e.target.dataset.r; if (!r) return;
      st.to = ''; st.from = r === 'all' ? '' : addDays(latest, -(Number(r) - 1)); api.changed();
    });
    return api;
  }

  /* active-filter chips (click × to remove) */
  function renderChips(container, api) {
    const chips = api.chips();
    container.innerHTML = chips.map((c, i) => `<button type="button" class="chip-x" data-i="${i}">${esc(c.label)} <span aria-hidden="true">×</span></button>`).join('');
    container.style.display = chips.length ? '' : 'none';
    container.onclick = e => {
      const b = e.target.closest('.chip-x'); if (!b) return;
      const c = chips[Number(b.dataset.i)]; api.remove(c.f, c.v);
    };
  }

  function favicon(host) { return host ? `https://www.google.com/s2/favicons?domain=${encodeURIComponent(host)}&sz=32` : ''; }

  return { SENT, SENT_ORDER, THEMES, THEME_SHORT, BUCKETS, NON_DEST, esc, fmtDate, fmtDateYear, fmtDateLong, addDays, weekStart,
           fmtPP, tally, net, loadItems, createFilters, renderChips, favicon, parseCSV, normalise };
})();
