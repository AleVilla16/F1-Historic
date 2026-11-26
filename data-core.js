// Copia del motore dati di F1 History (generata da build). Non modificare a mano.
'use strict';
// ===================== DATI: API, CACHE, MODELLO =====================
const API = 'https://api.jolpi.ca/ergast/f1';
const FIRST_YEAR = 1950;
const CUR_YEAR = new Date().getFullYear();
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---------- IndexedDB ----------
const Store = {
  db: null,
  open() {
    return new Promise(res => {
      try {
        const rq = indexedDB.open('f1-historic', 1);
        rq.onupgradeneeded = () => rq.result.createObjectStore('kv');
        rq.onsuccess = () => { this.db = rq.result; res(true); };
        rq.onerror = () => res(false);
      } catch (e) { res(false); }
    });
  },
  _tx(mode, fn) {
    return new Promise((res, rej) => {
      if (!this.db) return res(undefined);
      const t = this.db.transaction('kv', mode), rq = fn(t.objectStore('kv'));
      t.oncomplete = () => res(rq ? rq.result : undefined);
      t.onerror = () => rej(t.error);
    });
  },
  get(k) { return this._tx('readonly', s => s.get(k)); },
  set(k, v) { return this._tx('readwrite', s => s.put(v, k)); },
  del(k) { return this._tx('readwrite', s => s.delete(k)); },
  clear() { return this._tx('readwrite', s => s.clear()); },
  entries() {
    return new Promise((res, rej) => {
      if (!this.db) return res([]);
      const t = this.db.transaction('kv', 'readonly'), st = t.objectStore('kv');
      const a = st.getAllKeys(), b = st.getAll();
      t.oncomplete = () => res(a.result.map((k, i) => [k, b.result[i]]));
      t.onerror = () => rej(t.error);
    });
  }
};

// ---------- stato grezzo ----------
const D = { seasons: {}, drivers: {}, cons: {}, circuits: {}, enrich: null, meta: { lastSync: 0 } };

async function loadAll() {
  for (const [k, v] of await Store.entries()) {
    if (k.startsWith('season:')) D.seasons[v.year] = v;
    else if (k === 'drivers') D.drivers = v;
    else if (k === 'cons') D.cons = v;
    else if (k === 'circuits') D.circuits = v;
    else if (k === 'enrich') D.enrich = v;
    else if (k === 'meta') D.meta = v;
  }
}
async function saveMasters() {
  await Store.set('drivers', D.drivers); await Store.set('cons', D.cons);
  await Store.set('circuits', D.circuits); await Store.set('meta', D.meta);
}

// ---------- rete con limiti di frequenza ----------
const Sync = { running: false, cancel: false, msg: '', wait: '', done: 0, total: 0, req: 0, err: null, onProgress: () => {}, onSeason: () => {} };
const Net = { last: 0, gap: (typeof process !== 'undefined' && process.env && +process.env.F1_GAP) || 900 };

async function waitMsg(ms, txt) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (Sync.cancel) throw new Error('Download interrotto');
    Sync.wait = `${txt} tra ${Math.ceil((end - Date.now()) / 1000)} s`;
    Sync.onProgress(); await sleep(1000);
  }
  Sync.wait = ''; Sync.onProgress();
}
// registro delle richieste a Jolpica dell'ultima ora (per restare sotto 500/ora)
const JOL_BUDGET = 440;
const ReqLog = {
  list: (() => { try { return JSON.parse(localStorage.getItem('f1h-reqlog') || '[]'); } catch (e) { return []; } })(),
  prune() { const lim = Date.now() - 3600e3; this.list = this.list.filter(t => t > lim); },
  add() { this.list.push(Date.now()); this.prune(); try { localStorage.setItem('f1h-reqlog', JSON.stringify(this.list)); } catch (e) {} },
  count() { this.prune(); return this.list.length; },
};
async function apiGet(path, opt = {}) {
  const full = /^https?:/.test(path) ? path : API + path;
  const isJol = full.startsWith(API);
  let attempt = 0;
  for (;;) {
    if (Sync.cancel) throw new Error('Download interrotto');
    if (isJol && ReqLog.count() >= JOL_BUDGET) {
      const ms = ReqLog.list[0] + 3600e3 - Date.now() + 2000;
      await waitMsg(ms, `Pausa per rispettare il limite di Jolpica (500 richieste/ora, ${ReqLog.count()} usate): riprendo`);
      continue;
    }
    const w = Net.last + Net.gap - Date.now(); if (w > 0) await sleep(w);
    Net.last = Date.now();
    if (isJol) ReqLog.add();
    let r;
    try { r = await fetch(full, { headers: { Accept: 'application/json' } }); }
    catch (e) {
      attempt++; if (attempt > 5) throw new Error('API non raggiungibile (controlla la connessione)');
      await waitMsg(4000 * attempt, 'Errore di rete, nuovo tentativo'); continue;
    }
    Sync.req++;
    if (r.status === 429) {
      attempt++; Sync.throttled = (Sync.throttled || 0) + 1;
      if (isJol && opt.fallback) { Net.jolBlockedUntil = Date.now() + 20 * 60e3; const e = new Error('Jolpica ha bloccato le richieste'); e.code = 'RATE'; throw e; }
      const ra = +(r.headers.get('Retry-After') || 0) * 1000;
      const ms = Math.max(ra, [120e3, 300e3, 600e3][Math.min(attempt - 1, 2)]);
      await waitMsg(ms, `${isJol ? 'Jolpica' : 'L\'API'} ha bloccato temporaneamente le richieste da questa connessione (limite per indirizzo IP)${isJol ? ', spesso condiviso sulle reti mobili' : ''}. Riprovo`);
      continue;
    }
    if (r.status >= 500) {
      attempt++; if (attempt > 5) throw new Error('Server API non disponibile (' + r.status + ')');
      await waitMsg(5000 * attempt, 'Server non disponibile, riprovo'); continue;
    }
    if (opt.allow404 && (r.status === 404 || r.status === 400)) return null;
    if (!r.ok) throw new Error('Errore HTTP ' + r.status + ' su ' + full);
    const j = await r.json();
    return opt.raw ? j : j.MRData;
  }
}
async function pagedAll(path, extract) {
  let off = 0, total = 1; const out = [];
  while (off < total) {
    const m = await apiGet(`${path}${path.includes('?') ? '&' : '?'}limit=100&offset=${off}`, { fallback: Sync.fallback });
    total = +m.total || 0; out.push(...(extract(m) || [])); off += 100;
  }
  return out;
}
function mergeRaces(list, key) {
  const by = {};
  for (const R of list) {
    if (!by[R.round]) by[R.round] = Object.assign({}, R, { [key]: [] });
    by[R.round][key].push(...(R[key] || []));
  }
  return Object.values(by);
}

// ---------- conversione ----------
function ingDriver(x) {
  if (!x) return;
  D.drivers[x.driverId] = { id: x.driverId, gn: x.givenName, fn: x.familyName, dob: x.dateOfBirth || '', nat: x.nationality || '', code: x.code || '', pn: x.permanentNumber || '', url: x.url || '' };
}
function ingCons(x) { if (x) D.cons[x.constructorId] = { id: x.constructorId, name: x.name, nat: x.nationality || '', url: x.url || '' }; }
function ingRace(s, R) {
  const r = s.races[R.round] || (s.races[R.round] = { round: +R.round });
  r.name = R.raceName; r.date = R.date; r.time = R.time || ''; r.url = R.url || '';
  if (R.Sprint) r.sprintDate = R.Sprint.date;
  const C = R.Circuit;
  if (C) {
    r.circ = C.circuitId;
    D.circuits[C.circuitId] = { id: C.circuitId, name: C.circuitName, loc: C.Location?.locality || '', country: C.Location?.country || '', lat: C.Location?.lat, long: C.Location?.long, url: C.url || '' };
  }
}
const num = v => (v === undefined || v === null || v === '') ? null : +v;
function convRes(x) {
  ingDriver(x.Driver); ingCons(x.Constructor);
  const f = x.FastestLap;
  return { d: x.Driver.driverId, c: x.Constructor.constructorId, n: x.number || '', p: num(x.position), pt: x.positionText || '', g: num(x.grid),
    l: +x.laps || 0, s: x.status || '', pts: +x.points || 0, t: x.Time?.time || '',
    fl: f ? { r: num(f.rank), lap: num(f.lap), t: f.Time?.time || '' } : null };
}
function convQ(x) {
  ingDriver(x.Driver); ingCons(x.Constructor);
  return { d: x.Driver.driverId, c: x.Constructor.constructorId, n: x.number || '', p: num(x.position), q1: x.Q1 || '', q2: x.Q2 || '', q3: x.Q3 || '' };
}
const newSeason = y => ({ year: y, races: {}, res: {}, spr: {}, q: {}, dst: [], cst: [], done: {}, fetchedAt: 0, final: false });

async function syncSeason(y, what) {
  const s = D.seasons[y] || (D.seasons[y] = newSeason(y));
  if (what.sched) {
    const races = await pagedAll(`/${y}.json`, m => m.RaceTable?.Races);
    races.forEach(R => ingRace(s, R)); s.done.sched = true;
  }
  if (what.res) {
    const races = mergeRaces(await pagedAll(`/${y}/results.json`, m => m.RaceTable?.Races), 'Results');
    s.res = {};
    for (const R of races) { ingRace(s, R); s.res[R.round] = R.Results.map(convRes); }
    s.done.res = true;
  }
  if (what.spr && y >= 2021) {
    const races = mergeRaces(await pagedAll(`/${y}/sprint.json`, m => m.RaceTable?.Races), 'SprintResults');
    s.spr = {};
    for (const R of races) { ingRace(s, R); s.spr[R.round] = R.SprintResults.map(convRes); }
    s.done.spr = true;
  }
  if (what.st) {
    const L = await pagedAll(`/${y}/driverstandings.json`, m => m.StandingsTable?.StandingsLists);
    s.dst = L.flatMap(l => l.DriverStandings || []).map(x => {
      ingDriver(x.Driver);
      return { d: x.Driver.driverId, p: num(x.position), pt: x.positionText || '', pts: +x.points || 0, w: +x.wins || 0, cs: (x.Constructors || []).map(c => { ingCons(c); return c.constructorId; }) };
    });
    if (y >= 1958) {
      const C = await pagedAll(`/${y}/constructorstandings.json`, m => m.StandingsTable?.StandingsLists);
      s.cst = C.flatMap(l => l.ConstructorStandings || []).map(x => {
        ingCons(x.Constructor);
        return { c: x.Constructor.constructorId, p: num(x.position), pt: x.positionText || '', pts: +x.points || 0, w: +x.wins || 0 };
      });
    }
    s.done.st = true;
  }
  if (what.q && y >= 1994) {
    const races = mergeRaces(await pagedAll(`/${y}/qualifying.json`, m => m.RaceTable?.Races), 'QualifyingResults');
    s.q = {};
    for (const R of races) { ingRace(s, R); s.q[R.round] = R.QualifyingResults.map(convQ); }
    s.done.q = true;
  }
  s.fetchedAt = Date.now();
  s.final = y < CUR_YEAR && s.done.res && s.done.st && (y < 2021 || s.done.spr) && (y < 1994 || s.done.q) && s.fetchedAt > Date.UTC(y + 1, 0, 1);
  await Store.set('season:' + y, s);
  await saveMasters();
}

// ---------- fonte alternativa: f1api.dev ----------
const F1A = 'https://f1api.dev/api';
const F1A_NAT = { 'Great Britain': 'British', 'United Kingdom': 'British', England: 'British', Scotland: 'British', Wales: 'British', Netherlands: 'Dutch', Germany: 'German', Spain: 'Spanish', Italy: 'Italian', France: 'French', Monaco: 'Monegasque', Australia: 'Australian', Mexico: 'Mexican', Canada: 'Canadian', Finland: 'Finnish', Japan: 'Japanese', Thailand: 'Thai', Denmark: 'Danish', China: 'Chinese', 'United States': 'American', USA: 'American', 'New Zealand': 'New Zealander', Brazil: 'Brazilian', Argentina: 'Argentine', Austria: 'Austrian', Belgium: 'Belgian', Switzerland: 'Swiss', Sweden: 'Swedish', Poland: 'Polish', Russia: 'Russian', Venezuela: 'Venezuelan', Colombia: 'Colombian', Portugal: 'Portuguese', Ireland: 'Irish', Hungary: 'Hungarian', India: 'Indian', Indonesia: 'Indonesian', Malaysia: 'Malaysian', 'South Africa': 'South African', Chile: 'Chilean', Uruguay: 'Uruguayan', Czechia: 'Czech', 'Czech Republic': 'Czech', Liechtenstein: 'Liechtensteiner', Rhodesia: 'Rhodesian' };
const asArr = o => Array.isArray(o) ? o : (o && typeof o === 'object' ? [o] : []);
const isoDate = s => { if (!s) return ''; const m = String(s).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/); return m ? `${m[3]}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}` : String(s).slice(0, 10); };
const teamKey = s => normName(String(s || '').replace(/formula\s*(one|1)|f1|team|racing|scuderia|grand prix|motorsport/gi, ''));
function f1aMaps() {
  const byDob = {}, byName = {}, cons = {}, circ = {};
  for (const x of Object.values(D.drivers)) { byDob[normName(x.fn) + '|' + x.dob] = x.id; byName[normName(x.gn + x.fn)] = x.id; }
  for (const x of Object.values(D.cons)) cons[teamKey(x.name)] = x.id;
  for (const x of Object.values(D.circuits)) { circ[normName(x.loc)] = x.id; circ[normName(x.name)] = x.id; }
  return { byDob, byName, cons, circ };
}
function f1aDriver(o, maps) {
  const dv = o.driver || {}; const id0 = dv.driverId || o.driverId; if (!id0) return null;
  const dob = isoDate(dv.birthday);
  let id = maps.byDob[normName(dv.surname) + '|' + dob] || maps.byName[normName((dv.name || '') + (dv.surname || ''))] || (D.drivers[id0] && (!dob || D.drivers[id0].dob === dob) ? id0 : null);
  if (!id) { id = id0; D.drivers[id] = { id, gn: dv.name || '', fn: dv.surname || id0, dob, nat: F1A_NAT[dv.nationality] || dv.nationality || '', code: dv.shortName || '', pn: dv.number ? String(dv.number) : '', url: dv.url || '' }; maps.byDob[normName(dv.surname) + '|' + dob] = id; }
  else if (D.drivers[id] && !D.drivers[id].code && dv.shortName) D.drivers[id].code = dv.shortName;
  return id;
}
function f1aTeam(o, maps) {
  const tm = o.team || {}; const id0 = tm.teamId || o.teamId; if (!id0) return null;
  let id = D.cons[id0] ? id0 : maps.cons[teamKey(tm.teamName)];
  if (!id) { id = id0; D.cons[id] = { id, name: tm.teamName || id0, nat: F1A_NAT[tm.nationality || tm.country] || tm.nationality || '', url: tm.url || '' }; maps.cons[teamKey(tm.teamName)] = id; }
  return id;
}
function f1aCircuit(c, maps) {
  if (!c) return null;
  let id = D.circuits[c.circuitId] ? c.circuitId : (maps.circ[normName(c.city)] || maps.circ[normName(c.circuitName)]);
  if (!id) { id = c.circuitId; D.circuits[id] = { id, name: c.circuitName || id, loc: c.city || '', country: c.country || '', url: c.url || '' }; maps.circ[normName(c.city)] = id; }
  return id;
}
function f1aRow(x, maps) {
  const d = f1aDriver(x, maps), c = f1aTeam(x, maps); if (!d || !c) return null;
  const pos = typeof x.position === 'number' ? x.position : (/^\d+$/.test(String(x.position)) ? +x.position : null);
  const ret = x.retired && String(x.retired).toLowerCase() !== 'finished';
  const dq = /disq|squal|dsq|^dq$/i.test(String(x.position) + ' ' + (x.retired || ''));
  return { d, c, n: x.driver && x.driver.number ? String(x.driver.number) : '', p: pos, pt: dq ? 'D' : (ret || pos == null) ? 'R' : String(pos), g: x.grid != null && x.grid !== '-' ? +x.grid || 0 : null,
    l: +x.laps || 0, s: ret ? String(x.retired) : (x.time ? 'Finished' : ''), pts: +x.points || 0, t: x.time || '', fl: x.fastLap ? { r: null, lap: null, t: String(x.fastLap) } : null };
}
function f1aRaceObj(j) { if (!j) return null; const r = j.races || j.race; return Array.isArray(r) ? r[0] : r; }
async function syncSeasonF1A(y) {
  const s = D.seasons[y] || (D.seasons[y] = newSeason(y));
  const maps = f1aMaps();
  const get = p => apiGet(F1A + p, { raw: true, allow404: true });
  let rounds = [];
  const sch = await get(`/${y}?limit=100`);
  const list = sch ? asArr(sch.races || sch.race || sch.schedule) : [];
  for (const R of list) {
    const rd = +(R.round || 0); if (!rd) continue; rounds.push(rd);
    const r = s.races[rd] || (s.races[rd] = { round: rd });
    r.name = R.raceName || r.name || ''; r.date = isoDate((R.schedule && R.schedule.race && R.schedule.race.date) || R.date || R.raceDate) || r.date;
    const sd = R.schedule && (R.schedule.sprintRace || R.schedule.sprint); if (sd && sd.date) r.sprintDate = isoDate(sd.date);
    const ci = f1aCircuit(R.circuit && (Array.isArray(R.circuit) ? R.circuit[0] : R.circuit), maps); if (ci) r.circ = ci;
    if (R.url) r.url = R.url;
  }
  if (!rounds.length) for (let rd = 1; rd <= 25; rd++) rounds.push(rd);
  s.res = {}; s.q = {}; s.spr = {};
  let misses = 0;
  for (const rd of rounds) {
    const R = f1aRaceObj(await get(`/${y}/${rd}/race?limit=100`));
    if (!R || !asArr(R.results).length) { if (!list.length && ++misses >= 2) break; continue; }
    misses = 0;
    const r = s.races[rd] || (s.races[rd] = { round: rd });
    r.name = r.name || R.raceName || ''; r.date = r.date || isoDate(R.date); if (!r.circ) r.circ = f1aCircuit(R.circuit && (Array.isArray(R.circuit) ? R.circuit[0] : R.circuit), maps); if (!r.url && R.url) r.url = R.url;
    s.res[rd] = asArr(R.results).map(x => f1aRow(x, maps)).filter(Boolean);
    const Q = f1aRaceObj(await get(`/${y}/${rd}/qualy?limit=100`));
    const qr = Q ? asArr(Q.qualyResults || Q.results) : [];
    if (qr.length) s.q[rd] = qr.map(x => { const d = f1aDriver(x, maps), c = f1aTeam(x, maps); return d ? { d, c, n: x.driver && x.driver.number ? String(x.driver.number) : '', p: +(x.gridPosition || x.classificationId || x.position) || null, q1: x.q1 || '', q2: x.q2 || '', q3: x.q3 || '' } : null; }).filter(Boolean);
    if (y >= 2021) {
      const S = f1aRaceObj(await get(`/${y}/${rd}/sprint/race?limit=100`));
      const sr = S ? asArr(S.sprintRaceResults || S.sprintResults || S.results) : [];
      if (sr.length) { s.spr[rd] = sr.map(x => f1aRow(x, maps)).filter(Boolean); s.races[rd].sprintDate = s.races[rd].sprintDate || isoDate(S.date); }
    }
  }
  const DS = await get(`/${y}/drivers-championship?limit=100`);
  s.dst = DS ? asArr(DS.drivers_championship).map(x => { const d = f1aDriver(x, maps), c = f1aTeam(x, maps); return d ? { d, p: +x.position || null, pt: String(x.position ?? ''), pts: +x.points || 0, w: +x.wins || 0, cs: c ? [c] : [] } : null; }).filter(Boolean) : [];
  if (y >= 1958) {
    const CS = await get(`/${y}/constructors-championship?limit=100`);
    s.cst = CS ? asArr(CS.constructors_championship).map(x => { const c = f1aTeam(x, maps); return c ? { c, p: +x.position || null, pt: String(x.position ?? ''), pts: +x.points || 0, w: +x.wins || 0 } : null; }).filter(Boolean) : [];
  }
  s.done = { sched: true, res: Object.keys(s.res).length > 0, st: s.dst.length > 0, spr: true, q: y < 1994 || Object.keys(s.q).length > 0 };
  s.src = 'f1api'; s.manual = false; s.fetchedAt = Date.now();
  s.final = y < CUR_YEAR && s.done.res && s.done.st && s.fetchedAt > Date.UTC(y + 1, 0, 1);
  await Store.set('season:' + y, s); await saveMasters();
}

async function runSync(opts = {}) {
  if (Sync.running) return;
  Object.assign(Sync, { running: true, cancel: false, err: null, done: 0, req: 0, wait: '' });
  Sync.onProgress();
  try {
    const tasks = [];
    if (opts.years) {
      const ys = [...new Set(opts.years)].sort((x, z) => z - x);
      for (const y of ys) { tasks.push({ y, ph: 1 }); if (y >= 1994) tasks.push({ y, ph: 2 }); }
    } else for (let y = CUR_YEAR; y >= FIRST_YEAR; y--) {
      const s = D.seasons[y];
      if (opts.full || !s || !s.final || !(s.done && s.done.res && s.done.st)) tasks.push({ y, ph: 1 });
    }
    if (!opts.years) for (let y = CUR_YEAR; y >= 1994; y--) {
      const s = D.seasons[y];
      if (opts.full || !s || !s.final || !(s.done && s.done.q)) tasks.push({ y, ph: 2 });
    }
    let src = opts.src || 'jolpica';
    if (src === 'jolpica' && opts.fallback && Date.now() < (Net.jolBlockedUntil || 0)) { src = 'f1api'; Sync.switched = true; }
    Sync.fallback = !!opts.fallback && src === 'jolpica'; Sync.switched = Sync.switched && src === 'f1api' && opts.src !== 'f1api'; Sync.throttled = 0;
    if (src === 'f1api') { for (let i = tasks.length - 1; i >= 0; i--) if (tasks[i].ph === 2) tasks.splice(i, 1); }
    Sync.total = tasks.length; Sync.years = opts.years ? tasks.map(t => t.y) : null; Sync.src = opts.src || 'jolpica';
    for (const t of tasks) {
      Sync.msg = src === 'f1api' ? `Stagione ${t.y} da f1api.dev` : t.ph === 1 ? `Stagione ${t.y}: risultati e classifiche` : `Stagione ${t.y}: qualifiche`;
      Sync.cur = t.y; Sync.onProgress();
      try {
        if (src === 'f1api') { if (t.ph === 1) await syncSeasonF1A(t.y); }
        else if (t.ph === 1) { await syncSeason(t.y, { sched: t.y >= CUR_YEAR, res: 1, spr: 1, st: 1 }); D.seasons[t.y].src = 'jolpica'; D.seasons[t.y].manual = false; }
        else await syncSeason(t.y, { q: 1 });
      } catch (e) {
        if (e.code !== 'RATE') throw e;
        // Jolpica bloccata: passa a f1api.dev per il resto di questo download
        src = 'f1api'; Sync.switched = true;
        Sync.msg = `Jolpica bloccata: continuo con f1api.dev (stagione ${t.y})`; Sync.onProgress();
        const s = D.seasons[t.y];
        if (t.ph === 1 || !(s && s.done && s.done.res)) await syncSeasonF1A(t.y);
      }
      Sync.done++; Sync.onProgress(); Sync.onSeason(t.y);
    }
    if (!opts.years) { D.meta.lastSync = Date.now(); await Store.set('meta', D.meta); }
    Sync.msg = 'Dati aggiornati';
  } catch (e) { Sync.err = e.message; Sync.msg = ''; }
  finally { Sync.running = false; Sync.wait = ''; Sync.cur = null; Sync.onProgress(); }
}


// ---------- dati inseriti a mano (letti da formula1.com) ----------
const SEED_DRV = {
  norris: ['Lando', 'Norris', '1999-11-13', 'British', 'NOR'], max_verstappen: ['Max', 'Verstappen', '1997-09-30', 'Dutch', 'VER'],
  russell: ['George', 'Russell', '1998-02-15', 'British', 'RUS'], antonelli: ['Andrea Kimi', 'Antonelli', '2006-08-25', 'Italian', 'ANT'],
  albon: ['Alexander', 'Albon', '1996-03-23', 'Thai', 'ALB'], stroll: ['Lance', 'Stroll', '1998-10-29', 'Canadian', 'STR'],
  hulkenberg: ['Nico', 'Hülkenberg', '1987-08-19', 'German', 'HUL'], leclerc: ['Charles', 'Leclerc', '1997-10-16', 'Monegasque', 'LEC'],
  piastri: ['Oscar', 'Piastri', '2001-04-06', 'Australian', 'PIA'], hamilton: ['Lewis', 'Hamilton', '1985-01-07', 'British', 'HAM'],
  gasly: ['Pierre', 'Gasly', '1996-02-07', 'French', 'GAS'], tsunoda: ['Yuki', 'Tsunoda', '2000-05-11', 'Japanese', 'TSU'],
  ocon: ['Esteban', 'Ocon', '1996-09-17', 'French', 'OCO'], bearman: ['Oliver', 'Bearman', '2005-05-08', 'British', 'BEA'],
  lawson: ['Liam', 'Lawson', '2002-02-11', 'New Zealander', 'LAW'], bortoleto: ['Gabriel', 'Bortoleto', '2004-10-14', 'Brazilian', 'BOR'],
  alonso: ['Fernando', 'Alonso', '1981-07-29', 'Spanish', 'ALO'], sainz: ['Carlos', 'Sainz', '1994-09-01', 'Spanish', 'SAI'],
  doohan: ['Jack', 'Doohan', '2003-01-20', 'Australian', 'DOO'], hadjar: ['Isack', 'Hadjar', '2004-09-28', 'French', 'HAD'],
};
const SEED_CON = { mclaren: ['McLaren', 'British'], red_bull: ['Red Bull Racing', 'Austrian'], mercedes: ['Mercedes', 'German'], williams: ['Williams', 'British'],
  aston_martin: ['Aston Martin', 'British'], sauber: ['Kick Sauber', 'Swiss'], ferrari: ['Ferrari', 'Italian'], alpine: ['Alpine', 'French'], rb: ['Racing Bulls', 'Italian'], haas: ['Haas F1 Team', 'American'] };
const SEEDS = [
  { y: 2025, r: 1, name: 'Australian Grand Prix', date: '2025-03-16', circ: ['albert_park', 'Albert Park Grand Prix Circuit', 'Melbourne', 'Australia', '-37.8497', '144.968'],
    src: 'formula1.com (schermata)', rows: [
      // pos, num, driver, team, laps, tempo/stato, punti
      ['1', 4, 'norris', 'mclaren', 57, '1:42:06.304', 25], ['2', 1, 'max_verstappen', 'red_bull', 57, '+0.895s', 18],
      ['3', 63, 'russell', 'mercedes', 57, '+8.481s', 15], ['4', 12, 'antonelli', 'mercedes', 57, '+10.135s', 12],
      ['5', 23, 'albon', 'williams', 57, '+12.773s', 10], ['6', 18, 'stroll', 'aston_martin', 57, '+17.413s', 8],
      ['7', 27, 'hulkenberg', 'sauber', 57, '+18.423s', 6], ['8', 16, 'leclerc', 'ferrari', 57, '+19.826s', 4],
      ['9', 81, 'piastri', 'mclaren', 57, '+20.448s', 2], ['10', 44, 'hamilton', 'ferrari', 57, '+22.473s', 1],
      ['11', 10, 'gasly', 'alpine', 57, '+26.502s', 0], ['12', 22, 'tsunoda', 'rb', 57, '+29.884s', 0],
      ['13', 31, 'ocon', 'haas', 57, '+33.161s', 0], ['14', 87, 'bearman', 'haas', 57, '+40.351s', 0],
      ['NC', 30, 'lawson', 'red_bull', 46, 'DNF', 0], ['NC', 5, 'bortoleto', 'sauber', 45, 'DNF', 0],
      ['NC', 14, 'alonso', 'aston_martin', 32, 'DNF', 0], ['NC', 55, 'sainz', 'williams', 0, 'DNF', 0],
      ['NC', 7, 'doohan', 'alpine', 0, 'DNF', 0], ['NC', 6, 'hadjar', 'rb', 0, 'DNS', 0],
    ] },
];
// inserisce le gare manuali solo dove non ci sono già risultati scaricati
async function applySeeds() {
  let changed = false;
  for (const S of SEEDS) {
    const s = D.seasons[S.y] || (D.seasons[S.y] = newSeason(S.y));
    if (s.res && s.res[S.r] && s.res[S.r].length) continue;
    for (const [id, v] of Object.entries(SEED_DRV)) if (!D.drivers[id]) D.drivers[id] = { id, gn: v[0], fn: v[1], dob: v[2], nat: v[3], code: v[4], pn: '', url: '' };
    for (const [id, v] of Object.entries(SEED_CON)) if (!D.cons[id]) D.cons[id] = { id, name: v[0], nat: v[1], url: '' };
    const [cid, cname, loc, country, lat, long] = S.circ;
    if (!D.circuits[cid]) D.circuits[cid] = { id: cid, name: cname, loc, country, lat, long, url: '' };
    s.races[S.r] = Object.assign(s.races[S.r] || {}, { round: S.r, name: S.name, date: S.date, circ: cid });
    s.res = s.res || {};
    s.res[S.r] = S.rows.map((x, i) => {
      const num = /^\d+$/.test(x[0]), dns = x[5] === 'DNS';
      return { d: x[2], c: x[3], n: String(x[1]), p: num ? +x[0] : i + 1, pt: num ? x[0] : (dns ? 'W' : 'R'), g: null, l: x[4],
        s: num ? 'Finished' : (dns ? 'Did not start' : 'Ritirato'), pts: x[6], t: num ? x[5] : '', fl: null };
    });
    s.done = Object.assign(s.done || {}, { res: true });
    s.manual = true; s.src = s.src || 'manual'; s.final = false;
    await Store.set('season:' + S.y, s); changed = true;
  }
  if (changed) await saveMasters();
  return changed;
}

// ---------- snapshot (esporta / importa / URL) ----------
function makeSnapshot() {
  return { format: 'f1-historic', version: 1, exportedAt: new Date().toISOString(), seasons: D.seasons, drivers: D.drivers, cons: D.cons, circuits: D.circuits, enrich: D.enrich, photos: typeof PHOTOS !== 'undefined' ? PHOTOS : undefined, cardov: typeof CARDOV !== 'undefined' ? CARDOV : undefined, cardph: typeof CARDPH !== 'undefined' ? CARDPH : undefined, circinfo: typeof CIRCINFO !== 'undefined' ? CIRCINFO : undefined, circph: typeof CIRCPH !== 'undefined' ? CIRCPH : undefined, teamimg: typeof TEAMIMG !== 'undefined' ? TEAMIMG : undefined, uc: typeof UC !== 'undefined' ? UC : undefined, evlinks: typeof EVL !== 'undefined' ? EVL : undefined, teamlogo: typeof TEAMLOGO !== 'undefined' ? TEAMLOGO : undefined, fc: typeof FC !== 'undefined' ? FC : undefined };
}
async function applySnapshot(o) {
  if (!o || !['f1-historic', 'f1-storico'].includes(o.format)) throw new Error('File non riconosciuto: serve un export di questa app');
  for (const [y, s] of Object.entries(o.seasons || {})) {
    const cur = D.seasons[y];
    if (!cur || (s.fetchedAt || 0) >= (cur.fetchedAt || 0)) { D.seasons[y] = s; await Store.set('season:' + y, s); }
  }
  Object.assign(D.drivers, o.drivers || {}); Object.assign(D.cons, o.cons || {}); Object.assign(D.circuits, o.circuits || {});
  if (o.circinfo && typeof CIRCINFO !== 'undefined') { Object.assign(CIRCINFO, o.circinfo); await Store.set('circinfo', CIRCINFO); }
  if (o.circph && typeof CIRCPH !== 'undefined') { Object.assign(CIRCPH, o.circph); await Store.set('circph', CIRCPH); }
  if (o.cardph && typeof CARDPH !== 'undefined') { Object.assign(CARDPH, o.cardph); await Store.set('cardph', CARDPH); }
  if (o.photos && typeof PHOTOS !== 'undefined') { Object.assign(PHOTOS, o.photos); await Store.set('photos', PHOTOS); }
  if (o.cardov && typeof CARDOV !== 'undefined') { Object.assign(CARDOV, o.cardov); try { localStorage.setItem('f1h-cardov', JSON.stringify(CARDOV)); } catch (e) {} }
  if (o.fc && typeof FC !== 'undefined') { Object.assign(FC, o.fc); try { localStorage.setItem('f1h-fc', JSON.stringify(FC)); } catch (e) {} }
  if (o.teamlogo && typeof TEAMLOGO !== 'undefined') { Object.assign(TEAMLOGO, o.teamlogo); await Store.set('teamlogo', TEAMLOGO); }
  if (o.evlinks && typeof EVL !== 'undefined') { for (const [k, L] of Object.entries(o.evlinks)) { const cur = EVL[k] || (EVL[k] = []); for (const x of L) if (!cur.some(z => z.u === x.u)) cur.push(x); } await Store.set('evlinks', EVL); }
  if (o.uc && typeof UC !== 'undefined') { for (const [k, t] of Object.entries(o.uc)) { const cur = UC[k] || (UC[k] = { cols: [], v: {} }); for (const c of t.cols || []) if (!cur.cols.some(x => x.id === c.id)) cur.cols.push(c); for (const [c, m] of Object.entries(t.v || {})) cur.v[c] = Object.assign(cur.v[c] || {}, m); } try { localStorage.setItem('f1h-uc', JSON.stringify(UC)); } catch (e) {} }
  if (o.teamimg && typeof TEAMIMG !== 'undefined') { for (const [c, ys] of Object.entries(o.teamimg)) TEAMIMG[c] = Object.assign(TEAMIMG[c] || {}, ys); await Store.set('teamimg', TEAMIMG); }
  if (o.enrich && (!D.enrich || (o.enrich.at || 0) > (D.enrich.at || 0))) { D.enrich = o.enrich; await Store.set('enrich', D.enrich); }
  await saveMasters();
}

// ---------- import F1DB (telaio, motore, gomme, luogo di nascita, giri veloci e qualifiche storiche) ----------
async function unzipJson(buf) {
  const v = new DataView(buf), u8 = new Uint8Array(buf);
  let eocd = -1;
  for (let i = buf.byteLength - 22; i >= Math.max(0, buf.byteLength - 70000); i--) if (v.getUint32(i, true) === 0x06054b50) { eocd = i; break; }
  if (eocd < 0) throw new Error('Archivio ZIP non valido');
  let p = v.getUint32(eocd + 16, true); const n = v.getUint16(eocd + 10, true);
  for (let k = 0; k < n; k++) {
    if (v.getUint32(p, true) !== 0x02014b50) break;
    const meth = v.getUint16(p + 10, true), csz = v.getUint32(p + 20, true);
    const nl = v.getUint16(p + 28, true), el = v.getUint16(p + 30, true), cl = v.getUint16(p + 32, true), lo = v.getUint32(p + 42, true);
    const name = new TextDecoder().decode(u8.subarray(p + 46, p + 46 + nl));
    p += 46 + nl + el + cl;
    if (!/\.json$/i.test(name)) continue;
    const start = lo + 30 + v.getUint16(lo + 26, true) + v.getUint16(lo + 28, true);
    const data = u8.subarray(start, start + csz);
    if (meth === 0) return new TextDecoder().decode(data);
    if (meth !== 8) throw new Error('Compressione ZIP non supportata');
    if (typeof DecompressionStream === 'undefined') throw new Error('Browser troppo vecchio per aprire lo ZIP: estrai il file .json e caricalo');
    const ds = new Blob([data]).stream().pipeThrough(new DecompressionStream('deflate-raw'));
    return await new Response(ds).text();
  }
  throw new Error('Nessun file .json nello ZIP');
}
// circuiti storici nella stessa città di quelli moderni: un vecchio import per città poteva confonderli
const CIRC_FIX = [['montjuic', 1975, 'catalunya', /catalunya/i, ['Circuit de Barcelona-Catalunya', 'Montmeló', 'Spain']], ['jarama', 1981, 'madring', /madring/i, ['Madring', 'Madrid', 'Spain']], ['osterreichring', 1987, 'red_bull_ring', /red ?bull ring|a1.?ring/i, ['Red Bull Ring', 'Spielberg', 'Austria']], ['caesars_palace', 1982, 'vegas', /strip|las vegas street/i, ['Las Vegas Strip Street Circuit', 'Las Vegas', 'USA']]];
function fixOldCirc(R, y) {
  for (const [old, last, nid, re, [nm, loc, cty]] of CIRC_FIX) {
    if (R.circ !== old || y <= last) continue;
    const m = D.circuits[nid] || Object.values(D.circuits).find(x => x.id !== old && re.test(x.name || ''));
    if (m) R.circ = m.id; else { D.circuits[nid] = { id: nid, name: nm, loc, country: cty, url: '' }; R.circ = nid; }
  }
}
const normName = s => (s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[^a-z]/g, '');
let regionIt = null;
try { regionIt = new Intl.DisplayNames(['it'], { type: 'region' }); } catch (e) {}

function buildEnrich(db) {
  const E = { ent: {}, drv: {}, fl: {}, q: {}, tyN: {}, circ: {}, src: 'F1DB', at: Date.now() };
  const countries = {}; (db.countries || []).forEach(c => countries[c.id] = c);
  const cName = id => { const c = countries[id]; if (!c) return ''; try { return (regionIt && c.alpha2Code && regionIt.of(c.alpha2Code)) || c.name; } catch (e) { return c.name; } };
  // mappa piloti: cognome + data di nascita
  const ergByKey = {};
  for (const d of Object.values(D.drivers)) ergByKey[normName(d.fn) + '|' + d.dob] = d.id;
  const dmap = {};
  for (const d of db.drivers || []) {
    const id = ergByKey[normName(d.lastName) + '|' + d.dateOfBirth];
    if (id) { dmap[d.id] = id; E.drv[id] = { pob: d.placeOfBirth || '', cob: cName(d.countryOfBirthCountryId) }; }
  }
  // indice risultati Jolpica per gara
  const resIdx = {};
  for (const s of Object.values(D.seasons)) for (const [r, rows] of Object.entries(s.res || {})) resIdx[s.year + '-' + r] = rows;
  // completa mappa piloti con numero di gara, poi mappa scuderie per co-occorrenza
  const votes = {};
  for (const race of db.races || []) {
    const rows = resIdx[race.year + '-' + race.round]; if (!rows) continue;
    for (const x of race.raceResults || []) {
      let did = dmap[x.driverId];
      if (!did) { const m = rows.find(r => String(r.n) === String(x.driverNumber) && r.p === x.positionNumber); if (m) { did = dmap[x.driverId] = m.d; } }
      if (!did) continue;
      const m = rows.find(r => r.d === did); if (!m) continue;
      const k = x.constructorId; (votes[k] || (votes[k] = {}))[m.c] = ((votes[k] || {})[m.c] || 0) + 1;
    }
  }
  for (const d of db.drivers || []) if (dmap[d.id] && !E.drv[dmap[d.id]]) E.drv[dmap[d.id]] = { pob: d.placeOfBirth || '', cob: cName(d.countryOfBirthCountryId) };
  const cmap = {};
  for (const [k, v] of Object.entries(votes)) cmap[k] = Object.entries(v).sort((a, b) => b[1] - a[1])[0][0];
  const chassis = {}; (db.chassis || []).forEach(c => chassis[c.id] = c.name || c.fullName);
  const emf = {}; (db.engineManufacturers || []).forEach(e => emf[e.id] = e.name);
  const engines = {}, engMf = {}; (db.engines || []).forEach(e => { engines[e.id] = e.fullName || ((emf[e.engineManufacturerId] || '') + ' ' + (e.name || '')).trim(); engMf[e.id] = emf[e.engineManufacturerId]; });
  const tyres = {}; (db.tyreManufacturers || []).forEach(t => tyres[t.id] = t.name);
  const add = (k, ch, en, ty, em) => {
    const o = E.ent[k] || (E.ent[k] = { ch: [], en: [], ty: [], em: [] }); o.em = o.em || [];
    ch.forEach(x => x && !o.ch.includes(x) && o.ch.push(x)); en.forEach(x => x && !o.en.includes(x) && o.en.push(x)); ty.forEach(x => x && !o.ty.includes(x) && o.ty.push(x)); (em || []).forEach(x => x && !o.em.includes(x) && o.em.push(x));
  };
  for (const s of db.seasons || []) {
    const tySet = new Set();
    for (const en of s.entrants || []) for (const ec of en.constructors || []) {
      const c = cmap[ec.constructorId] || String(ec.constructorId).replace(/-/g, '_');
      const ch = (ec.chassis || []).map(x => chassis[x.chassisId]);
      const eng = (ec.engines || []).map(x => engines[x.engineId] || emf[ec.engineManufacturerId]);
      if (!eng.length && ec.engineManufacturerId) eng.push(emf[ec.engineManufacturerId]);
      const ty = (ec.tyreManufacturers || []).map(x => tyres[x.tyreManufacturerId]);
      ty.forEach(t => t && tySet.add(t));
      const mf = (ec.engines || []).map(x => engMf[x.engineId]).concat(ec.engineManufacturerId ? [emf[ec.engineManufacturerId]] : []).filter(Boolean);
      add(s.year + '|' + c, ch, eng, ty, mf);
      for (const dr of ec.drivers || []) if (!dr.testDriver && dmap[dr.driverId]) add(s.year + '|' + c + '|' + dmap[dr.driverId], ch, eng, ty, mf);
    }
    E.tyN[s.year] = tySet.size;
  }
  const cdir = {}; (db.circuits || []).forEach(c => cdir[c.id] = c.direction || '');
  for (const race of db.races || []) {
    const key = race.year + '-' + race.round;
    { const our = D.seasons[race.year] && D.seasons[race.year].races && D.seasons[race.year].races[race.round] && D.seasons[race.year].races[race.round].circ;
      if (our && (!E.circ[our] || race.year >= E.circ[our].y)) E.circ[our] = { y: race.year, len: race.courseLength || null, turns: race.turns || null, dir: cdir[race.circuitId] || '' }; }
    const f = (race.fastestLaps || []).find(x => x.positionNumber === 1);
    if (f && dmap[f.driverId]) E.fl[key] = { d: dmap[f.driverId], c: cmap[f.constructorId] || '', t: f.time || '', lap: f.lap || null };
    if (race.year < 1994 && race.qualifyingResults && race.qualifyingResults.length) {
      E.q[key] = race.qualifyingResults.filter(x => dmap[x.driverId]).map(x => ({ d: dmap[x.driverId], c: cmap[x.constructorId] || '', n: x.driverNumber || '', p: x.positionNumber, q1: '', q2: '', q3: '', t: x.time || '' }));
    }
  }
  E.stats = { drivers: Object.keys(E.drv).length, entries: Object.keys(E.ent).length };
  return E;
}
async function importF1DB(file) {
  const text = /\.zip$/i.test(file.name) ? await unzipJson(await file.arrayBuffer()) : await file.text();
  const db = JSON.parse(text);
  if (!db.drivers || !db.races) throw new Error('Il file non sembra il database F1DB (f1db-json-single)');
  if (!Object.keys(D.drivers).length) throw new Error('Scarica prima i risultati: F1DB serve solo a completarli');
  D.enrich = buildEnrich(db);
  await Store.set('enrich', D.enrich);
  return D.enrich.stats;
}

// ===================== MODELLO STATISTICHE =====================
const isNum = x => /^\d+$/.test(x.pt) && x.p != null;
const isStart = x => !['F', 'W'].includes(x.pt) && !/qualif|withdr|did not start|not started/i.test(x.s);
const isRet = x => isStart(x) && (x.pt === 'R' || x.pt === 'N');
function lapMs(t) { if (!t) return Infinity; const m = String(t).match(/^(?:(\d+):)?(\d+(?:\.\d+)?)$/); return m ? (+(m[1] || 0)) * 60000 + parseFloat(m[2]) * 1000 : Infinity; }
function fastestOf(rows) {
  const r1 = rows.find(x => x.fl && x.fl.r === 1);
  if (r1) return { d: r1.d, c: r1.c, t: r1.fl.t, lap: r1.fl.lap };
  let best = null;
  for (const x of rows) if (x.fl && lapMs(x.fl.t) < (best ? lapMs(best.fl.t) : Infinity)) best = x;
  return best ? { d: best.d, c: best.c, t: best.fl.t, lap: best.fl.lap } : null;
}
function groupBy(rows, k) { const o = {}; for (const x of rows) (o[x[k]] || (o[x[k]] = [])).push(x); return o; }
const blank = () => ({ races: 0, poles: 0, wins: 0, fl: 0, led: 0, pod: 0, t10: 0, ret: 0, sr: 0, sp: 0, sw: 0, spod: 0, sret: 0, dbl: 0, pts: null, pos: null, pt: '', teams: [], rPts: 0 });
const blankCareer = () => ({ titles: 0, best: null, years: [], races: 0, poles: 0, wins: 0, fl: 0, led: 0, pod: 0, t10: 0, ret: 0, sr: 0, sp: 0, sw: 0, spod: 0, sret: 0, dbl: 0, pts: 0, sPos: 0, nPos: 0, sQ: 0, nQ: 0, first: null, last: null, firstWin: null, lastWin: null, firstPod: null, lastPod: null, firstPole: null });

let M = null;
function buildModel() {
  const E = D.enrich || {};
  const today = new Date().toISOString().slice(0, 10);
  M = { years: [], yi: {}, races: [], raceByKey: {}, dy: {}, cy: {}, dc: {}, cc: {}, circ: {}, empty: true };
  const DY = (d, y) => ((M.dy[d] || (M.dy[d] = {}))[y] || (M.dy[d][y] = blank()));
  const CY = (c, y) => ((M.cy[c] || (M.cy[c] = {}))[y] || (M.cy[c][y] = blank()));
  const DC = d => M.dc[d] || (M.dc[d] = blankCareer());
  const CC = c => M.cc[c] || (M.cc[c] = blankCareer());
  const years = Object.keys(D.seasons).map(Number).filter(y => Object.keys(D.seasons[y].races || {}).length).sort((a, b) => a - b);
  for (const y of years) {
    const s = D.seasons[y];
    const rounds = Object.keys(s.races).map(Number).sort((a, b) => a - b);
    const yi = { y, races: [], hasSprint: false, dst: s.dst || [], cst: s.cst || [] };
    for (const r of rounds) {
      const R = s.races[r], key = y + '-' + r;
      fixOldCirc(R, y);
      const res = (s.res && s.res[r]) || [], spr = (s.spr && s.spr[r]) || [];
      let q = (s.q && s.q[r] && s.q[r].length) ? s.q[r] : (E.q && E.q[key]) || [];
      const race = { key, y, r, name: R.name, date: R.date, circ: R.circ, url: R.url, sprintDate: R.sprintDate, res, spr, q, done: res.length > 0, events: [], dotd: (s.dotd && s.dotd[r]) || null };
      if (s.pit && s.pit[r] && typeof pitInfo === 'function') race.pit = pitInfo(race, s.pit[r]);
      if (typeof fstopInfo === 'function') race.fstop = fstopInfo(race, s);
      race.led = (s.led && s.led[r] && s.led[r].m) || null; if (race.led) yi.hasLed = true;
      const qp = q.find(x => x.p === 1);
      if (qp) race.pole = { d: qp.d, c: qp.c || (res.find(x => x.d === qp.d) || {}).c, t: qp.q3 || qp.q2 || qp.q1 || qp.t || '' };
      else { const g1 = res.find(x => x.g === 1); if (g1) race.pole = { d: g1.d, c: g1.c, t: '' }; }
      race.podium = [1, 2, 3].map(p => res.filter(x => x.p === p && isNum(x)));
      race.fl = fastestOf(res) || (E.fl && E.fl[key]) || null;
      race.laps = race.podium[0][0]?.l || (res.length ? Math.max(...res.map(x => x.l || 0)) : null);
      const c1 = new Set(race.podium[0].map(x => x.c));
      race.dbl = race.podium[1].map(x => x.c).find(c => c1.has(c)) || null;
      if (spr.length) {
        yi.hasSprint = true; race.hasSpr = true;
        const sg = spr.find(x => x.g === 1); race.sPole = sg ? { d: sg.d, c: sg.c } : null;
        race.sPodium = [1, 2, 3].map(p => spr.filter(x => x.p === p && isNum(x)));
        race.sFl = fastestOf(spr); race.sLaps = race.sPodium[0][0]?.l || null;
      } else if (R.sprintDate) { race.hasSpr = true; yi.hasSprint = true; race.sPodium = [[], [], []]; }
      yi.races.push(race); M.races.push(race); M.raceByKey[key] = race;
      if (race.circ) { const ci = M.circ[race.circ] || (M.circ[race.circ] = { id: race.circ, races: [] }); ci.races.push(race); }
      if (!race.done) continue;
      M.empty = false;
      // --- piloti ---
      for (const [d, rows] of Object.entries(groupBy(res, 'd'))) {
        const st = rows.filter(isStart); if (!st.length) continue;
        const o = DY(d, y), car = DC(d);
        o.races++; rows.forEach(x => { if (!o.teams.includes(x.c)) o.teams.push(x.c); o.rPts += x.pts; });
        if (race.led && race.led[d]) o.led += race.led[d];
        const nums = rows.filter(isNum).map(x => x.p), best = nums.length ? Math.min(...nums) : null;
        if (best === 1) { o.wins++; if (!car.firstWin) car.firstWin = race; car.lastWin = race; }
        if (best != null && best <= 3) { o.pod++; if (!car.firstPod) car.firstPod = race; car.lastPod = race; }
        if (best != null && best <= 10) o.t10++;
        if (st.every(isRet)) o.ret++;
        if (best != null) { car.sPos += best; car.nPos++; }
        const qr = q.find(x => x.d === d), qv = qr ? qr.p : (rows.find(x => x.g > 0) || {}).g;
        if (qv) { car.sQ += qv; car.nQ++; }
        if (!car.first) car.first = race; car.last = race;
      }
      if (race.pole) { DY(race.pole.d, y).poles++; const cd = DC(race.pole.d); if (!cd.firstPole) cd.firstPole = race; }
      // --- scuderie ---
      for (const [c, rows] of Object.entries(groupBy(res, 'c'))) {
        const st = rows.filter(isStart); if (!st.length) continue;
        const o = CY(c, y), car = CC(c);
        o.races++;
        rows.forEach(x => { o.rPts += x.pts; if (race.led && race.led[x.d]) o.led += race.led[x.d]; });
        if (race.podium[0].some(x => x.c === c)) { o.wins++; if (!car.firstWin) car.firstWin = race; car.lastWin = race; }
        const pods = rows.filter(x => isNum(x) && x.p <= 3).length;
        if (pods) { o.pod += pods; if (!car.firstPod) car.firstPod = race; car.lastPod = race; }
        o.t10 += rows.filter(x => isNum(x) && x.p <= 10).length;
        o.ret += rows.filter(isRet).length;
        if (race.dbl === c) o.dbl++;
        rows.filter(isNum).forEach(x => { car.sPos += x.p; car.nPos++; });
        const qr = q.filter(x => x.c === c);
        if (qr.length) qr.forEach(x => { if (x.p) { car.sQ += x.p; car.nQ++; } });
        else rows.forEach(x => { if (x.g > 0) { car.sQ += x.g; car.nQ++; } });
        if (!car.first) car.first = race; car.last = race;
      }
      if (race.fl && race.fl.d && M.dy[race.fl.d] && M.dy[race.fl.d][y]) { M.dy[race.fl.d][y].fl++; if (race.fl.c && M.cy[race.fl.c] && M.cy[race.fl.c][y]) M.cy[race.fl.c][y].fl++; }
      if (race.pole && race.pole.c) { CY(race.pole.c, y).poles++; const cc = CC(race.pole.c); if (!cc.firstPole) cc.firstPole = race; }
      // --- sprint ---
      if (spr.length) {
        for (const [d, rows] of Object.entries(groupBy(spr, 'd'))) {
          if (!rows.some(isStart)) continue;
          const o = DY(d, y); o.sr++; rows.forEach(x => o.rPts += x.pts);
          if (rows.some(x => isNum(x) && x.p === 1)) o.sw++;
          if (rows.some(x => isNum(x) && x.p <= 3)) o.spod++;
          if (rows.filter(isStart).every(isRet)) o.sret++;
        }
        for (const [c, rows] of Object.entries(groupBy(spr, 'c'))) {
          if (!rows.some(isStart)) continue;
          const o = CY(c, y); o.sr++; rows.forEach(x => o.rPts += x.pts);
          if (rows.some(x => isNum(x) && x.p === 1)) o.sw++;
          o.spod += rows.filter(x => isNum(x) && x.p <= 3).length;
          o.sret += rows.filter(isRet).length;
        }
        if (race.sPole) { DY(race.sPole.d, y).sp++; CY(race.sPole.c, y).sp++; }
      }
    }
    const last = yi.races[yi.races.length - 1];
    yi.complete = yi.races.length > 0 && yi.races.every(r => r.done) && (y < CUR_YEAR || (last && last.date < today)) && !(s.manual && !(s.dst || []).length);
    yi.started = yi.races.some(r => r.done);
    // classifiche
    for (const e of yi.dst) { const o = M.dy[e.d] && M.dy[e.d][y]; if (o) { o.pts = e.pts; o.pos = e.p; o.pt = e.pt; } }
    for (const e of yi.cst) { const o = M.cy[e.c] && M.cy[e.c][y]; if (o) { o.pts = e.pts; o.pos = e.p; o.pt = e.pt; } }
    for (const d in M.dy) { const o = M.dy[d][y]; if (o && o.pts == null) o.pts = o.rPts; }
    if (yi.complete) {
      const ch = yi.dst.find(e => e.p === 1); if (ch) { yi.champ = ch.d; DC(ch.d).titles++; }
      const cc = yi.cst.find(e => e.p === 1); if (cc) { yi.cChamp = cc.c; CC(cc.c).titles++; }
    }
    if (yi.started) M.years.push(y);
    M.yi[y] = yi;
  }
  // totali carriera
  const sum = (tot, per) => { for (const [y, o] of Object.entries(per)) { tot.years.push(+y); for (const k of ['races', 'poles', 'wins', 'fl', 'led', 'pod', 't10', 'ret', 'sr', 'sp', 'sw', 'spod', 'sret', 'dbl']) tot[k] += o[k] || 0; tot.pts += o.pts || 0; if (o.pos != null && M.yi[y] && M.yi[y].complete && (tot.best == null || o.pos < tot.best)) tot.best = o.pos; } };
  for (const d in M.dy) sum(DC(d), M.dy[d]);
  for (const c in M.cy) sum(CC(c), M.cy[c]);
  M.drvIds = Object.keys(M.dc).filter(d => M.dc[d].races > 0);
  M.conIds = Object.keys(M.cc).filter(c => M.cc[c].races > 0);
  M.lastYear = M.years[M.years.length - 1];
  buildEvents();
  return M;
}

// ---------- eventi salienti generati dai dati ----------
// risultati validi (scarti) del Mondiale Piloti: n = migliori n; h = [gare 1ª metà, migliori 1ª metà, migliori 2ª metà]
const DROPS = { 1950: { n: 4 }, 1951: { n: 4 }, 1952: { n: 4 }, 1953: { n: 4 }, 1954: { n: 5 }, 1955: { n: 5 }, 1956: { n: 5 }, 1957: { n: 5 }, 1958: { n: 6 }, 1959: { n: 5 }, 1960: { n: 6 },
  1961: { n: 5 }, 1962: { n: 5 }, 1963: { n: 6 }, 1964: { n: 6 }, 1965: { n: 6 }, 1966: { n: 5 }, 1967: { h: [6, 5, 4] }, 1968: { h: [6, 5, 5] }, 1969: { h: [6, 5, 4] }, 1970: { h: [7, 6, 5] },
  1971: { h: [6, 5, 4] }, 1972: { h: [6, 5, 5] }, 1973: { h: [8, 7, 6] }, 1974: { h: [8, 7, 6] }, 1975: { h: [7, 6, 6] }, 1976: { h: [8, 7, 7] }, 1977: { h: [9, 8, 7] }, 1978: { h: [8, 7, 7] },
  1979: { h: [7, 4, 4] }, 1980: { h: [7, 5, 5] } };
for (let y = 1981; y <= 1990; y++) DROPS[y] = { n: 11 };
const bestSum = (arr, n) => arr.slice().sort((a, b) => b - a).slice(0, n).reduce((s, v) => s + v, 0);
// punti validi da un elenco di punti per gara (indice = gara)
function countedPts(y, perRace) {
  const r = DROPS[y];
  if (!r) return perRace.reduce((s, v) => s + (v || 0), 0);
  const v = perRace.map(x => x || 0);
  if (r.n) return bestSum(v, r.n);
  return bestSum(v.slice(0, r.h[0]), r.h[1]) + bestSum(v.slice(r.h[0]), r.h[2]);
}
// gara in cui il titolo è diventato matematicamente sicuro
function clinchRound(yi, kind) {
  const races = yi.races, y = yi.y, N = races.length;
  if (kind === 'c' && (y < 1958 || (y < 1979))) return null; // costruttori fino al 1978: contava solo la vettura migliore, con scarti
  const k = kind === 'd' ? 'd' : 'c';
  const top = rows => { const by = {}; for (const x of rows) by[x[k]] = (by[x[k]] || 0) + (x.pts || 0); const p = Object.values(by).sort((a, b) => b - a); return p[0] || 0; };
  const done = races.filter(r => r.done);
  if (!done.length) return null;
  const maxRace = Math.max(0, ...done.map(r => top(r.res)));
  const maxSpr = Math.max(0, ...races.filter(r => r.spr && r.spr.length).map(r => top(r.spr)));
  const pr = {}; // punti per gara di ogni pilota/scuderia
  const fut = j => maxRace * ((y === 2014 && j === N - 1) ? 2 : 1) + (races[j].hasSpr ? maxSpr : 0);
  const add = (rows, i) => { for (const x of rows) { const a = pr[x[k]] || (pr[x[k]] = new Array(N).fill(0)); a[i] += x.pts || 0; } };
  // controlla se il titolo è sicuro: dopo la gara i (raceLeft = la gara i è ancora da correre, Sprint già corsa)
  const check = (i, raceLeft) => {
    const cur = Object.keys(pr).map(id => ({ id, v: countedPts(y, pr[id]) })).sort((a, b) => b.v - a.v);
    if (!cur.length) return null;
    const lead = cur[0];
    const fill = arr => { const b = arr.slice(); if (raceLeft) b[i] += maxRace * ((y === 2014 && i === N - 1) ? 2 : 1); for (let j = i + 1; j < N; j++) b[j] += fut(j); return b; };
    for (const o of cur.slice(1)) if (countedPts(y, fill(pr[o.id])) >= lead.v) return null;
    if (countedPts(y, fill(new Array(N).fill(0))) >= lead.v) return null;
    return lead.id;
  };
  for (let i = 0; i < N; i++) {
    const r = races[i]; if (!r.done) return null;
    if (r.spr && r.spr.length) { add(r.spr, i); const id = check(i, true); if (id) return { r, id, left: N - i, last: false, spr: true }; }
    add(r.res, i);
    if (i === N - 1) { if (!yi.complete) return null; const cur = Object.keys(pr).map(id => ({ id, v: countedPts(y, pr[id]) })).sort((a, b) => b.v - a.v); return cur.length ? { r, id: cur[0].id, left: 0, last: true } : null; }
    const id = check(i, false); if (id) return { r, id, left: N - 1 - i, last: false };
  }
  return null;
}
const anticipo = n => n === 1 ? 'con 1 gara di anticipo' : `con ${n} gare di anticipo`;
function buildEvents() {
  const dn = d => dName(d), cn = c => cName(c);
  for (const y of M.years) {
    const yi = M.yi[y];
    const debut = {};
    for (const d of M.drvIds) { const f = M.dc[d].first; if (f && f.y === y) (debut[f.key] || (debut[f.key] = [])).push(d); }
    for (const race of yi.races) {
      if (!race.done) continue;
      const ev = race.events = [];
      const w = race.podium[0][0];
      if (w) {
        if (M.dc[w.d].firstWin === race) ev.push({ t: 'win', x: `Prima vittoria in carriera per ${dn(w.d)}` });
        if (M.cc[w.c] && M.cc[w.c].firstWin === race) ev.push({ t: 'team', x: `Prima vittoria nella storia per ${cn(w.c)}` });
        if (w.g >= 8) ev.push({ t: 'rec', x: `${dn(w.d)} vince partendo dalla ${w.g}ª posizione in griglia` });
        if (w.g === 0) ev.push({ t: 'rec', x: `${dn(w.d)} vince partendo dalla pit lane` });
      }
      for (const p of [race.podium[1], race.podium[2]]) for (const x of p) if (M.dc[x.d].firstPod === race) ev.push({ t: 'pod', x: `Primo podio in carriera per ${dn(x.d)}` });
      if (race.pole && M.dc[race.pole.d] && M.dc[race.pole.d].firstPole === race) ev.push({ t: 'pole', x: `Prima pole position per ${dn(race.pole.d)}` });
      if (race.dbl) ev.push({ t: 'team', x: `Doppietta ${cn(race.dbl)}` });
      const deb = debut[race.key];
      if (deb && deb.length) ev.push({ t: 'deb', x: `Esordio in F1: ${deb.slice(0, 8).map(dn).join(', ')}${deb.length > 8 ? ` e altri ${deb.length - 8}` : ''}` });
      if (yi.complete || y < CUR_YEAR) {
        const fin = M.drvIds.filter(d => M.dc[d].last === race && M.dc[d].races >= 30 && M.lastYear > y);
        if (fin.length) ev.push({ t: 'deb', x: `Ultima gara in carriera: ${fin.map(dn).join(', ')}` });
      }
      const led = race.res.filter(x => isRet(x) && x.g === 1);
      if (led.length && race.pole) ev.push({ t: 'ret', x: `Ritiro del poleman ${dn(led[0].d)} (${led[0].s})` });
    }
    const dcl = yi.clD = clinchRound(yi, 'd');
    yi.clC = clinchRound(yi, 'c');
    if (dcl) dcl.r.events.unshift({ t: 'title', x: `${dn(dcl.id)} è Campione del Mondo${dcl.last ? ' all\'ultima gara' : ' ' + anticipo(dcl.left) + (dcl.spr ? ' (nella Sprint)' : '')}` });
    else if (yi.complete && yi.champ) { const lr = yi.races[yi.races.length - 1]; lr.events.unshift({ t: 'title', x: `${dn(yi.champ)} è Campione del Mondo` }); }
    if (yi.complete && yi.cChamp) {
      const ccl = yi.clC; const lr = (ccl && ccl.id === yi.cChamp) ? ccl.r : yi.races[yi.races.length - 1];
      lr.events.unshift({ t: 'title', x: `${cn(yi.cChamp)} vince il Mondiale Costruttori${ccl && ccl.id === yi.cChamp ? (ccl.last ? ' all\'ultima gara' : ' ' + anticipo(ccl.left)) : ''}` });
      if (M.years.filter(yy => yy <= y && M.yi[yy].cChamp === yi.cChamp).length === 1) lr.events.push({ t: 'team', x: `Primo titolo Costruttori per ${cn(yi.cChamp)}` });
    }
    if (yi.complete && yi.champ && M.years.filter(yy => yy <= y && M.yi[yy].champ === yi.champ).length === 1) {
      const lr = (dcl && dcl.r) || yi.races[yi.races.length - 1];
      lr.events.push({ t: 'title', x: `Primo titolo mondiale per ${dn(yi.champ)}` });
    }
  }
}

// ---------- What If ----------
function whatIf(y, cfg) {
  const yi = M.yi[y]; if (!yi) return null;
  const drv = {}, con = {};
  const D_ = d => drv[d] || (drv[d] = { id: d, scores: [], spr: 0, cnt: {} });
  const C_ = c => con[c] || (con[c] = { id: c, pts: 0, cnt: {} });
  for (const race of yi.races) {
    if (!race.done) continue;
    const per = {};
    for (const x of race.res) {
      if (!isNum(x)) { if (isStart(x)) D_(x.d); continue; }
      const p = (cfg.pts[x.p - 1] || 0);
      per[x.d] = Math.max(per[x.d] || 0, p);
      D_(x.d).cnt[x.p] = (D_(x.d).cnt[x.p] || 0) + 1;
      C_(x.c).pts += p; C_(x.c).cnt[x.p] = (C_(x.c).cnt[x.p] || 0) + 1;
    }
    if (cfg.fl && race.fl) {
      const fr = race.res.find(x => x.d === race.fl.d);
      if (fr && (!cfg.flTop || (isNum(fr) && fr.p <= cfg.flTop))) { per[race.fl.d] = (per[race.fl.d] || 0) + cfg.fl; C_(fr.c).pts += cfg.fl; }
    }
    for (const [d, p] of Object.entries(per)) D_(d).scores.push(p);
    if (race.spr.length && cfg.sprint !== 'none') {
      for (const x of race.spr) {
        const p = cfg.sprint === 'real' ? x.pts : (isNum(x) ? (cfg.sprint[x.p - 1] || 0) : 0);
        D_(x.d).spr += p; C_(x.c).pts += p;
      }
    }
  }
  const cb = (a, b) => { for (let p = 1; p <= 40; p++) { const d = (b.cnt[p] || 0) - (a.cnt[p] || 0); if (d) return d; } return 0; };
  const dl = Object.values(drv).map(o => {
    const s = o.scores.slice().sort((a, b) => b - a);
    const used = cfg.best > 0 ? s.slice(0, cfg.best) : s;
    o.tot = used.reduce((a, b) => a + b, 0) + o.spr;
    const real = yi.dst.find(e => e.d === o.id); o.realPos = real ? real.p : null; o.realPts = real ? real.pts : null;
    return o;
  }).sort((a, b) => b.tot - a.tot || cb(a, b));
  dl.forEach((o, i) => o.pos = i + 1);
  const cl = Object.values(con).map(o => { const real = yi.cst.find(e => e.c === o.id); o.tot = o.pts; o.realPos = real ? real.p : null; o.realPts = real ? real.pts : null; return o; })
    .sort((a, b) => b.tot - a.tot || cb(a, b));
  cl.forEach((o, i) => o.pos = i + 1);
  return { drivers: dl, cons: cl };
}

// ---------- utilità nomi/date ----------
function dName(d) { const x = D.drivers[d]; return x ? `${x.gn} ${x.fn}` : d; }
function cName(c) { const x = D.cons[c]; return x ? x.name : c; }
function dAbbr(d) { const x = D.drivers[d]; return x ? (x.code || normName(x.fn).slice(0, 3).toUpperCase()) : ''; }
function fmtDate(iso) { if (!iso) return ''; const [y, m, d] = iso.split('-'); return `${d}/${m}/${y}`; }
function ageAt(dob, iso) {
  if (!dob || !iso) return null;
  const [a, b, c] = dob.split('-').map(Number), [x, y, z] = iso.split('-').map(Number);
  return x - a - ((y < b || (y === b && z < c)) ? 1 : 0);
}

if (typeof module !== 'undefined') module.exports = { D, Store, Sync, Net, ReqLog, runSync, syncSeasonF1A, buildModel, whatIf, getM: () => M, dName, importF1DB, buildEnrich, makeSnapshot, applySnapshot, clinchRound };
