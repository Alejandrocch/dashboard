let STATE = {
  rfqs: [], offers: [], orders: [], billing: [],
  production: [],
  objectives: { nac:0, exp:0, hot:0, car:0, breakdown:{} },
  analytics: { ordersByYear: {}, offersByYear: {}, mode: 'orders' }
};
let charts = {};

// Utilidades de rendimiento (evita bloqueos al escribir en filtros)
function debounce(fn, wait = 120){
  let t = null;
  return function(...args){
    if(t) clearTimeout(t);
    t = setTimeout(() => fn.apply(this, args), wait);
  };
}

function rafThrottle(fn){
  let queued = false;
  return function(...args){
    if(queued) return;
    queued = true;
    requestAnimationFrame(() => {
      queued = false;
      fn.apply(this, args);
    });
  };
}

// Plantilla de estado para evitar versiones corruptas (import JSON / server)
const DEFAULT_STATE = JSON.parse(JSON.stringify(STATE));
function normalizeState(s){
  // mezcla profunda muy simple: asegura llaves mínimas
  if(!s || typeof s !== 'object') return JSON.parse(JSON.stringify(DEFAULT_STATE));
  const out = JSON.parse(JSON.stringify(DEFAULT_STATE));
  try{
    // copia superficial
    for(const k of Object.keys(s)) out[k]=s[k];
    // asegura sub-objetos
    out.objectives = out.objectives && typeof out.objectives==='object' ? out.objectives : {nac:0,exp:0,hot:0,car:0,breakdown:{}};
    out.objectives.breakdown = out.objectives.breakdown && typeof out.objectives.breakdown==='object' ? out.objectives.breakdown : {};
    out.analytics = out.analytics && typeof out.analytics==='object' ? out.analytics : {ordersByYear:{}, offersByYear:{}, mode:'orders'};
    out.analytics.ordersByYear = out.analytics.ordersByYear && typeof out.analytics.ordersByYear==='object' ? out.analytics.ordersByYear : {};
    out.analytics.offersByYear = out.analytics.offersByYear && typeof out.analytics.offersByYear==='object' ? out.analytics.offersByYear : {};
  }catch(e){
    return JSON.parse(JSON.stringify(DEFAULT_STATE));
  }
  return out;
}


// ------------------------------
// Roles (modo local)
// ------------------------------
const USERS = {
  // Importante: los nombres de allow deben coincidir con data-view (rfqs/offers/orders)
  'Alejandro': { role: 'admin', allow: ['dashboard','analytics','objetivos','facturacion','produccion','informes','rfqs','offers','orders','paneldiario'] },
  'David':     { role: 'produccion', allow: ['dashboard','produccion','informes','rfqs','offers','orders','paneldiario'] },
  'Veronica':  { role: 'produccion', allow: ['dashboard','produccion','informes','rfqs','offers','orders','paneldiario'] },
  'Rafa':      { role: 'produccion', allow: ['dashboard','produccion','informes','rfqs','offers','orders','paneldiario'] },
  'Isabel':    { role: 'facturacion', allow: ['dashboard','facturacion','informes'] },
  'Diego':     { role: 'facturacion', allow: ['dashboard','facturacion','informes'] },
};

let currentUser = localStorage.getItem('pg_user') || '';


// Fallback: si Chart.js no está disponible (sin internet / CSP), evitamos que la app se rompa.
// FULLBLACK: chart theme + dark chart areas
function isDarkTheme(){ return document.documentElement.getAttribute("data-theme")==="dark"; }
const chartAreaBgPlugin={id:"chartAreaBg",beforeDraw:function(chart,args,opts){
  const a=chart.chartArea; if(!a) return;
  const ctx=chart.ctx; ctx.save();
  ctx.fillStyle=(opts&&opts.color)?opts.color:(isDarkTheme()?"#0b1220":"#ffffff");
  ctx.fillRect(a.left,a.top,a.right-a.left,a.bottom-a.top);
  ctx.restore();
}};
let __chartPluginsRegistered=false;
function registerChartPlugins(){
  if(__chartPluginsRegistered) return;
  if(typeof Chart!=="undefined" && Chart.register){ try{ Chart.register(chartAreaBgPlugin); }catch(e){} }
  __chartPluginsRegistered=true;
}
function injectChartTheme(cfg){
  if(!cfg || typeof cfg!=="object") return;
  const dark=isDarkTheme();
  cfg.options=cfg.options||{};
  cfg.options.plugins=cfg.options.plugins||{};
  cfg.options.plugins.legend=cfg.options.plugins.legend||{};
  cfg.options.plugins.legend.labels=cfg.options.plugins.legend.labels||{};
  cfg.options.plugins.legend.labels.color = dark?"#e5e7eb":"#111827";
  cfg.options.plugins.tooltip=cfg.options.plugins.tooltip||{};
  cfg.options.plugins.tooltip.titleColor = dark?"#e5e7eb":"#111827";
  cfg.options.plugins.tooltip.bodyColor = dark?"#e5e7eb":"#111827";
  cfg.options.plugins.chartAreaBg=cfg.options.plugins.chartAreaBg||{};
  cfg.options.plugins.chartAreaBg.color = dark?"#0b1220":"#ffffff";
  cfg.options.scales=cfg.options.scales||{};
  Object.keys(cfg.options.scales).forEach(k=>{
    const s=cfg.options.scales[k]||{};
    s.ticks=s.ticks||{};
    s.grid=s.grid||{};
    if(s.ticks.color==null) s.ticks.color = dark?"#cbd5e1":"#334155";
    if(s.grid.color==null) s.grid.color = dark?"rgba(255,255,255,0.10)":"rgba(15,23,42,0.10)";
    cfg.options.scales[k]=s;
  });
}
function refreshAllCharts(){
  try{ charts.forEach(ch=>{ try{ injectChartTheme(ch.config); ch.update(); }catch(e){} }); }catch(e){}
}
function safeChart(ctx,cfg){
  try{
    registerChartPlugins();
    injectChartTheme(cfg);
    if(charts.has(ctx.canvas)){ try{charts.get(ctx.canvas).destroy();}catch(e){}
      charts.delete(ctx.canvas);
    }
    const c=new Chart(ctx,cfg);
    charts.set(ctx.canvas,c);
    return c;
  }catch(e){
    console.warn("Chart error",e);
    return null;
  }
}


// Storage
const LS_KEY = 'cch_v55_db';
// ------------------------------
// Persistencia en servidor (opcional)
// - Si el panel está servido por HTTP/HTTPS y existe api.php, guardamos/recuperamos el STATE.
// - Evita tener que subir CSVs cada vez.
// ------------------------------
const SERVER_ENABLED = (typeof location !== 'undefined') && (location.protocol === 'http:' || location.protocol === 'https:');
const SERVER_API = 'api.php';
let _serverAvailable = null; // null=desconocido, true/false tras probar
let _serverLastOkAt = null;
let _serverLastErrAt = null;
let _serverLastErrMsg = '';

function updateServerStatusUI(){
  const el = document.getElementById('server-status');
  if(!el) return;

  if(!SERVER_ENABLED){
    el.textContent = 'Servidor: OFF';
    el.title = 'Modo file:// (sin servidor).';
    return;
  }

  if(_serverAvailable === false){
    el.textContent = 'Servidor: 🔴 no disponible';
    el.title = _serverLastErrMsg || 'No se pudo contactar con api.php';
    return;
  }
  if(_serverAvailable === null){
    el.textContent = 'Servidor: …';
    el.title = 'Comprobando servidor...';
    return;
  }

  // Disponible
  const t = _serverLastOkAt ? _fmtHHMM(_serverLastOkAt) : '—';
  el.textContent = `Servidor: 🟢 sync ${t}`;
  el.title = _serverLastErrAt ? (`Último error: ${_serverLastErrMsg} (${_fmtHHMM(_serverLastErrAt)})`) : 'Sincronización activa';
}

async function serverPing(){
  if(!SERVER_ENABLED) return false;
  if(_serverAvailable !== null) return _serverAvailable;
  try{
    const r = await fetch(`${SERVER_API}?action=ping`, { cache: 'no-store' });
    _serverAvailable = !!(r && r.ok);
    if(_serverAvailable){
      _serverLastOkAt = new Date();
      _serverLastErrAt = null;
      _serverLastErrMsg = '';
    }else{
      _serverLastErrAt = new Date();
      _serverLastErrMsg = 'ping no OK';
    }
  }catch(e){
    _serverAvailable = false;
    _serverLastErrAt = new Date();
    _serverLastErrMsg = (e && e.message) ? e.message : 'ping error';
  }
  updateServerStatusUI();
  return _serverAvailable;
}

async function serverGet(key){
  if(!(await serverPing())) return null;
  try{
    const r = await fetch(`${SERVER_API}?action=get&key=${encodeURIComponent(key)}`, { cache: 'no-store' });
    if(!r.ok) return null;
    const j = await r.json();
    return (j && j.ok) ? j.value : null;
  }catch(e){ return null; }
}

async function serverSet(key, value){
  if(!(await serverPing())) return false;
  try{
    const r = await fetch(`${SERVER_API}?action=set`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key, value })
    });
    return !!(r && r.ok);
  }catch(e){ return false; }
}

async function serverGetFull(){
  if(!(await serverPing())) return null;
  try{
    const r = await fetch(`${SERVER_API}?action=get_full`, { cache: 'no-store' });
    if(!r.ok) return null;
    const j = await r.json();
    if(!(j && j.ok)) return null;
    const v = j.value || null;
    if(v && typeof v === 'object'){
      v.__serverBlobs = j.blobs || null;
    }
    return v;
  }catch(e){ return null; }
}

async function serverSetFull(value){
  if(!(await serverPing())) return false;
  try{
    const r = await fetch(`${SERVER_API}?action=set_full`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(value)
    });
    if(!r || !r.ok) return false;
    const j = await r.json().catch(()=>null);
    return j && j.ok === true;
  }catch(e){ return false; }
}

async function serverSetBlob(key, value){
  try{
    const r = await fetch('api.php?action=set_blob', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({key, value})
    });
    const j = await r.json().catch(()=>null);
    return !!(j && j.ok);
  }catch(e){ return false; }
}
async function serverGetBlob(key){
  try{
    const r = await fetch('api.php?action=get_blob&key='+encodeURIComponent(key));
    const j = await r.json().catch(()=>null);
    if(j && j.ok) return j.value;
  }catch(e){}
  return null;
}


let _serverSaveT = null;
function scheduleServerSave(){
  if(!SERVER_ENABLED) return;
  clearTimeout(_serverSaveT);
  _serverSaveT = setTimeout(async ()=>{
    try{
      const payload = JSON.parse(JSON.stringify(STATE));
      // Guardamos blobs grandes (ERP) por separado para evitar límites de tamaño
      const _offersBlob = STATE.offers || [];
      const _ordersBlob = STATE.orders || [];
      // El payload principal NO lleva listas gigantes
      payload.offers = [];
      payload.orders = [];
// Igual que en saveState: por seguridad no subimos rows gigantes si existieran
      if(payload?.analytics?.ordersByYear){
        for(const y of Object.keys(payload.analytics.ordersByYear)){
          if(payload.analytics.ordersByYear[y]?.rows) delete payload.analytics.ordersByYear[y].rows;
        }
      }
      payload.__serverSavedAt = new Date().toISOString();
      const okOffers = await serverSetBlob('offers', _offersBlob);
      const okOrders = await serverSetBlob('orders', _ordersBlob);
      payload.__blobs = {offers: okOffers, orders: okOrders};
      const ok = await serverSetFull(payload);
      if(ok){ _serverLastOkAt = new Date(); _serverLastErrAt=null; _serverLastErrMsg=''; }
      else { _serverLastErrAt = new Date(); _serverLastErrMsg='No se pudo guardar (¿tamaño demasiado grande?)'; }
      updateServerStatusUI();
    }catch(e){}
  }, 900);
}
// localStorage tiene un límite (normalmente ~5MB). Los pedidos (filas) pueden excederlo.
// Guardamos las filas completas de pedidos en IndexedDB y en localStorage solo dejamos resúmenes.
const IDB_NAME = 'cch_panelgeneral_db';
const IDB_VERSION = 1;
const IDB_STORE_ORDERS = 'orders_raw_by_year';

// Cache en memoria (no se persiste en localStorage)
const ORDERS_ROWS_CACHE = Object.create(null);

function idbOpen(){
  return new Promise((resolve, reject) => {
    try{
      const req = indexedDB.open(IDB_NAME, IDB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if(!db.objectStoreNames.contains(IDB_STORE_ORDERS)){
          db.createObjectStore(IDB_STORE_ORDERS, { keyPath: 'year' });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    }catch(err){ reject(err); }
  });
}

async function idbSetYearRows(year, rows){
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE_ORDERS, 'readwrite');
    tx.objectStore(IDB_STORE_ORDERS).put({ year, rows });
    tx.oncomplete = () => { db.close(); resolve(true); };
    tx.onerror = () => { const e = tx.error || new Error('IndexedDB error'); db.close(); reject(e); };
  });
}

async function idbGetYearRows(year){
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE_ORDERS, 'readonly');
    const req = tx.objectStore(IDB_STORE_ORDERS).get(year);
    req.onsuccess = () => { const v = req.result ? req.result.rows : null; db.close(); resolve(v); };
    req.onerror = () => { const e = req.error || new Error('IndexedDB error'); db.close(); reject(e); };
  });
}

async function idbDeleteYearRows(year){
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE_ORDERS, 'readwrite');
    tx.objectStore(IDB_STORE_ORDERS).delete(year);
    tx.oncomplete = () => { db.close(); resolve(true); };
    tx.onerror = () => { const e = tx.error || new Error('IndexedDB error'); db.close(); reject(e); };
  });
}

async function idbClearAll(){
  const db = await idbOpen();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(IDB_STORE_ORDERS, 'readwrite');
    tx.objectStore(IDB_STORE_ORDERS).clear();
    tx.oncomplete = () => { db.close(); resolve(true); };
    tx.onerror = () => { const e = tx.error || new Error('IndexedDB error'); db.close(); reject(e); };
  });
}

// Normaliza nombres de vistas (evita "production" vs "produccion" y otros alias)
function normalizeViewName(view){
  const v = String(view || '').trim().toLowerCase();
  const map = {
    'home': 'dashboard',
    'inicio': 'dashboard',
    'production': 'produccion',
    'producción': 'produccion',
    'objectives': 'objetivos',
    'billing': 'facturacion',
    'facturación': 'facturacion'
  };
  return map[v] || v;
}

window.nav = function(view) {
    try {
        view = normalizeViewName(view);
        if (currentUser && !canAccess(view)) {
            alert('Sin permiso para esta sección.');
            view = 'dashboard';
        }
        document.querySelectorAll('.view-section').forEach(e => e.classList.add('hidden'));
        document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
    
        const isTable = ['rfqs','offers','orders'].includes(view);
        if (isTable) {
            const vt = document.getElementById('view-table');
            if (vt) vt.classList.remove('hidden');
            renderTable(view);
        } else {
            const target = document.getElementById(`view-${view}`) || document.getElementById('view-produccion') || document.getElementById('view-production');
            if (target) {
                target.classList.remove('hidden');
            } else {
                // Fallback: nunca dejar la app en blanco
                const vd = document.getElementById('view-dashboard');
                if (vd) vd.classList.remove('hidden');
                view = 'dashboard';
            }
        }

        const btn = Array.from(document.querySelectorAll('.nav-btn')).find(b => normalizeViewName(b.dataset.view || '') === view);
        if (btn) btn.classList.add('active');

        if(view === 'analytics') renderAnalytics();
        if(view === 'produccion') renderGantt();
        if(view === 'objetivos') { renderMiniCharts(); renderObjectiveCharts(); }
        if(view === 'facturacion') { initBillingUI(); renderBilling(); }

        // FIX crítico (v120): reset de scroll REAL para evitar el “hueco blanco”.
        // Hay vistas que internamente usan contenedores con overflow (p.ej. Producción).
        // Si el scroll queda “guardado” en el contenedor, parece que la página está vacía.
        const resetScroll = () => {
            try {
                // 1) Scroll de la ventana
                window.scrollTo(0, 0);
                document.documentElement.scrollTop = 0;
                document.body.scrollTop = 0;

                // 2) Scroll de contenedores principales
                const mc = document.getElementById('main-content');
                if (mc) mc.scrollTop = 0;

                // 3) Scroll del contenedor de la vista activa
                const activeView = isTable
                    ? document.getElementById('view-table')
                    : (document.getElementById(`view-${view}`) || document.getElementById('view-produccion'));
                if (activeView) activeView.scrollTop = 0;

                // 4) Scroll de cualquier sub-contenedor scrollable dentro de la vista
                if (activeView) {
                    activeView.querySelectorAll('[data-scrollable], .scrollable, .table-wrapper, .gantt-wrapper')
                        .forEach(el => { try { el.scrollTop = 0; } catch(e) {} });
                }
                // 5) Forzar arranque justo bajo el menú (ancla por vista)
                const anchorId = isTable ? "anchor-table" : (view === "facturacion" ? "anchor-facturacion" : (view === "produccion" ? "anchor-produccion" : (view === "informes" ? "anchor-informes" : null)));
                if (anchorId) {
                    const a = document.getElementById(anchorId);
                    if (a && typeof a.scrollIntoView === "function") {
                        a.scrollIntoView({ block: "start" });
                    }
                }

            } catch (e) {}
        };

        // Desactiva la restauración automática del navegador (file:// + hash)
        try { if ('scrollRestoration' in history) history.scrollRestoration = 'manual'; } catch(e) {}

        // Ejecutamos tras 2 frames para asegurar que el DOM ya está visible y con su altura real
        requestAnimationFrame(() => requestAnimationFrame(resetScroll));
    } catch (err) {
        console.error('NAV ERROR', err);
        // Fallback absoluto
        const vd = document.getElementById('view-dashboard');
        if (vd) vd.classList.remove('hidden');
    }
};

document.addEventListener('DOMContentLoaded', () => {
  (async () => {
    try{
      restoreState();

      // Si estamos en servidor y existe api.php, intentamos recuperar estado guardado.
      // Objetivo: no tener que subir CSVs cada vez.
      if(SERVER_ENABLED){
        const serverState = await serverGetFull();
        if(serverState && typeof serverState === 'object'){
          const localStamp = (()=>{
            const raw = localStorage.getItem('cch_panel_last_save');
            const n = raw ? parseInt(raw,10) : 0;
            return Number.isFinite(n) ? n : 0;
          })();
          const serverStamp = (()=>{
            const iso = serverState.__serverSavedAt || serverState.__serverSavedAtISO || '';
            const t = iso ? Date.parse(iso) : 0;
            return Number.isFinite(t) ? t : 0;
          })();

          const localLooksEmpty = (
            (STATE.orders||[]).length===0 &&
            (STATE.offers||[]).length===0 &&
            (STATE.production||[]).length===0 &&
            (STATE.billing||[]).length===0 &&
            Object.keys(STATE.analytics?.ordersByYear||{}).length===0 &&
            Object.keys(STATE.analytics?.offersByYear||{}).length===0
          );

          // Caso típico: localStorage se quedó en "minimal" por cuota (solo objectives/analytics).
          const localMissingCore = (
            (!Array.isArray(STATE.orders) || STATE.orders.length===0) &&
            (!Array.isArray(STATE.offers) || STATE.offers.length===0) &&
            (!Array.isArray(STATE.billing) || STATE.billing.length===0) &&
            (!Array.isArray(STATE.production) || STATE.production.length===0) &&
            (Object.keys(STATE.analytics?.ordersByYear||{}).length>0 || Object.keys(STATE.objectives||{}).length>0)
          );

          if(localLooksEmpty || localMissingCore || serverStamp > localStamp){
            STATE = serverState;
            // normaliza por si viene antiguo
            if(!STATE || typeof STATE !== 'object') STATE = {};
            if(!Array.isArray(STATE.rfqs)) STATE.rfqs = [];
            if(!Array.isArray(STATE.offers)) STATE.offers = [];
            if(!Array.isArray(STATE.orders)) STATE.orders = [];
            if(!Array.isArray(STATE.billing)) STATE.billing = [];
            if(!Array.isArray(STATE.production)) STATE.production = [];
            if(!STATE.analytics || typeof STATE.analytics !== 'object') STATE.analytics = { ordersByYear: {}, offersByYear: {}, mode: 'orders' };
            if(!STATE.analytics.ordersByYear) STATE.analytics.ordersByYear = {};
            if(!STATE.analytics.offersByYear) STATE.analytics.offersByYear = {};
            if(!STATE.analytics.mode) STATE.analytics.mode = 'orders';
            // legado
            if((!STATE.billing || !STATE.billing.length) && Array.isArray(STATE.facturacion?.items)) STATE.billing = STATE.facturacion.items;
            if(STATE.facturacion) delete STATE.facturacion;

            // IMPORTANTE: ofertas/pedidos grandes se guardan como blobs en servidor.
            // Si existen, los recuperamos aquí para que al reiniciar NO se quede pelado.
            try{
              const meta = (STATE && STATE.__serverBlobs) ? STATE.__serverBlobs : null;
              if(!meta || meta.offers){
                const bOffers = await serverGetBlob('offers');
                if(Array.isArray(bOffers)) STATE.offers = bOffers;
              }
              if(!meta || meta.orders){
                const bOrders = await serverGetBlob('orders');
                if(Array.isArray(bOrders)) STATE.orders = bOrders;
              }
            }catch(e){}

            try{ localStorage.setItem('cch_v55_db', JSON.stringify(STATE)); }catch(e){}
            try{ localStorage.setItem('cch_panel_last_save', String(Date.now())); }catch(e){}
          }
        }
      }

      // Login / permisos
      showLogin(false);
      if (currentUser) applyAccessControl();
      // Mostrar estado de autoguardado al arrancar
      updateAutosaveStatus();
      updateServerStatusUI();
      initMocks();
      initHandlers();
      initNavBindings();
      initInformes();
      updateUI();
      // Si hay hash (por ejemplo #produccion), lo respetamos, pero evitamos el salto de scroll nativo
      const hash = (location.hash || '').replace('#','').trim();
      try { if (hash) history.replaceState(null, '', location.pathname + location.search); } catch(e) {}
      nav(hash || 'dashboard');
    }catch(err){
      console.error('Fallo al arrancar la app:', err);
      // Failsafe: dejar al menos Inicio visible y la navegación operativa
      try{
        document.querySelectorAll('.view-section').forEach(v => v.classList.remove('active'));
        document.getElementById('view-dashboard')?.classList.add('active');
      }catch(e){}
      alert('⚠️ El panel detectó un error al arrancar.\n\nSuele ser por datos antiguos guardados en el navegador.\n\nHe reiniciado lo necesario. Si persiste, usa: Ajustes → Reset local.');
    }
  })();
});

// Navegación robusta: sin onclick inline (CSP/Hostinger)
function initNavBindings(){
    document.querySelectorAll('.nav-btn[data-view]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            const v = btn.dataset.view;
            if (v) window.nav(v);
        });
    });

    // Failsafe: enlaces tipo href="#produccion"
    document.querySelectorAll('a[href^="#"]').forEach(a => {
        a.addEventListener('click', (e) => {
            const h = (a.getAttribute('href') || '').replace('#','').trim();
            if(!h) return;
            const nh = normalizeViewName(h);
            if(['dashboard','analytics','objetivos','facturacion','produccion','informes','rfqs','offers','orders','paneldiario'].includes(nh)){
                e.preventDefault();
                window.nav(nh);
            }
        });
    });
}

function initHandlers() {
    document.getElementById('btn-layout').onclick = () => document.getElementById('app-container').classList.toggle('layout-side-nav');
    // Tema Día/Noche (con icono)
    const btnTheme = document.getElementById('btn-theme');
    const applyThemeIcon = () => {
        const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
        const i = btnTheme?.querySelector('i');
        if (i) i.className = isDark ? 'fa-solid fa-sun' : 'fa-solid fa-moon';
    };
    // Arranque seguro: siempre iniciar en modo día (white)
    document.documentElement.setAttribute('data-theme', 'light');
    try { localStorage.setItem('pg_theme', 'light'); } catch(e) {}
    applyThemeIcon();
    try{refreshAllCharts();}catch(e){}
    btnTheme.onclick = () => {
        const d = document.documentElement;
        const next = d.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        d.setAttribute('data-theme', next);
        localStorage.setItem('pg_theme', next);
        try{refreshAllCharts();}catch(e){}
        applyThemeIcon();
    };

    // Ajustes (export/import/reset + cambio de usuario)
    const btnSettings = document.getElementById('btn-settings');
    const settingsMenu = document.getElementById('settings-menu');
    const menuExport = document.getElementById('menu-export-json');
    const menuImport = document.getElementById('menu-import-json');
    const menuExportServer = document.getElementById('menu-export-server');
    const menuImportServer = document.getElementById('menu-import-server');
    const menuReset  = document.getElementById('menu-reset');
    const menuSwitch = document.getElementById('menu-switch-user');
    const restoreInput = document.getElementById('restore-file');

    const closeSettings = () => settingsMenu?.classList.add('hidden');
    const toggleSettings = () => settingsMenu?.classList.toggle('hidden');

    if (btnSettings) btnSettings.onclick = (e) => { e.stopPropagation(); toggleSettings(); };
    document.addEventListener('click', () => closeSettings());
    settingsMenu?.addEventListener('click', (e) => e.stopPropagation());

    const exportJSON = () => {
        const payload = { exportedAt: new Date().toISOString(), state: STATE };
        const blob = new Blob([JSON.stringify(payload, null, 2)], {type: 'application/json'});
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = 'panelgeneral.json';
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(a.href), 5000);
    };

    if (menuExport) menuExport.onclick = () => { closeSettings(); exportJSON(); };
    if (menuImport) menuImport.onclick = () => { closeSettings(); restoreInput?.click(); restoreInput.__uploadToServer = false; };

if (menuExportServer) menuExportServer.onclick = async () => {
    closeSettings();
    if(!SERVER_ENABLED){
        alert('Este panel no está en modo servidor (file://).');
        return;
    }
    const ok = await serverPing();
    if(!ok){
        alert('No puedo contactar con el servidor (api.php).');
        updateServerStatusUI();
        return;
    }
    const state = await serverGetFull();
    if(!state){
        alert('No hay base en servidor todavía.');
        return;
    }
    const blob = new Blob([JSON.stringify({ exportedAt: new Date().toISOString(), state }, null, 2)], {type:'application/json'});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'database_general.json';
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(()=>URL.revokeObjectURL(a.href), 5000);
};

if (menuImportServer) menuImportServer.onclick = () => {
    closeSettings();
    // Reutilizamos el input de restore, pero avisamos que se subirá al servidor.
    if(!SERVER_ENABLED){
        alert('Este panel no está en modo servidor (file://).');
        return;
    }
    restoreInput?.click();
    restoreInput.__uploadToServer = true;
};

    if (restoreInput) restoreInput.onchange = async (e) => {
        const file = e.target.files?.[0];
        if (!file) return;
        try {
            const txt = await file.text();
            const obj = JSON.parse(txt);
            if (obj?.state) STATE = obj.state;
            else if (obj) STATE = obj;
            saveState();
            updateUI();
            if(SERVER_ENABLED && restoreInput.__uploadToServer){
                const ok = await serverSetFull(STATE);
                if(ok){ _serverLastOkAt = new Date(); _serverLastErrAt=null; _serverLastErrMsg=''; }
                else { _serverLastErrAt = new Date(); _serverLastErrMsg='No se pudo restaurar en servidor'; }
                updateServerStatusUI();
                alert(ok ? 'Backup restaurado en servidor.' : 'No se pudo restaurar en servidor (revisa tamaño/permisos).');
            }else{
                alert('Backup cargado.');
            }
        } catch(err) {
            console.error(err);
            alert('El archivo no parece un panelgeneral.json válido.');
        } finally {
            restoreInput.value = '';
        }
    };

    if (menuReset) menuReset.onclick = () => {
        closeSettings();
        if (!confirm('¿Seguro? Borra datos locales (este navegador).')) return;
        localStorage.removeItem('pg_state');
        localStorage.removeItem('cch_v55_db');
        localStorage.removeItem('cch_panel_last_save');
        localStorage.removeItem('pg_theme');
        localStorage.removeItem('pg_billing');
        localStorage.removeItem('pg_user');
        location.reload();
    };

    if (menuSwitch) menuSwitch.onclick = () => { closeSettings(); showLogin(true); };
    document.getElementById('file-offers').onchange = (e) => loadCSV(e, 'offers');
    document.getElementById('file-orders').onchange = (e) => loadCSV(e, 'orders');
    document.getElementById('file-bill').onchange = (e) => loadBillingFile(e);

    // Filtros del dashboard (Sección / Comercial)
    const fSerie = document.getElementById('f-serie');
    const fAgent = document.getElementById('f-agent');
    const updateUIDeferred = rafThrottle(updateUI);
    if(fSerie) fSerie.addEventListener('change', () => { window.__CCH_FILTER_SERIE = String(fSerie.value||'all'); updateUIDeferred(); });
    if(fAgent) fAgent.addEventListener('change', () => { window.__CCH_FILTER_AGENT = String(fAgent.value||'Todos'); updateUIDeferred(); });

    initGlobalSearchAndMic();
    initBillingUI();

    // Producción (CSV + filtros) – handlers seguros
    const prodFile = document.getElementById('prod-file');
    if(prodFile){
        prodFile.onchange = async (e) => {
            const f = e.target.files?.[0];
            if(!f) return;
            try{
                await loadProductionCSVFile(f);
                toast('Pedidos 2026 cargados en Producción.');
            }catch(err){
                console.error(err);
                alert('No se pudo leer el CSV de Producción.');
            }finally{
                prodFile.value = '';
            }
        };
    }
    const renderGanttDeferred = debounce(() => { try{ renderGantt(); }catch(e){} }, 120);
    ['prod-search','prod-serie','prod-agente','prod-sociedad','prod-st-pend','prod-st-proc','prod-st-inc','prod-st-fin'].forEach(id => {
        const el = document.getElementById(id);
        if(el) el.addEventListener('input', renderGanttDeferred);
        if(el) el.addEventListener('change', renderGanttDeferred);
    });

    // Analítica (pedidos por año) – handlers seguros (existen aunque la vista esté oculta)
    initAnalyticsHandlers();
}

function restoreState() {
    // IMPORTANTE:
    // En Chrome es relativamente común que quede un JSON corrupto en localStorage tras iteraciones
    // (cortes, ediciones manuales, versiones antiguas, etc.). Si JSON.parse revienta en el arranque,
    // se "tosta" toda la app y no responde a clics.
    // Por eso: parse seguro + autorecuperación.
    const s = localStorage.getItem('cch_v55_db');
    if(s){
        try{
            STATE = JSON.parse(s);
        }catch(err){
            console.warn('STATE corrupto en localStorage. Se reinicia cch_v55_db.', err);
            try{ localStorage.removeItem('cch_v55_db'); }catch(e){}
            // Dejar STATE como estructura mínima válida
            STATE = {
                rfqs: [],
                offers: [],
                orders: [],
                billing: [],
                production: [],
                analytics: { ordersByYear: {}, offersByYear: {}, mode: 'orders' }
            };
        }
    }

    // --- Normalización / backward compatible ---
    if(!STATE || typeof STATE !== 'object') STATE = {};

    // arrays base
    if(!Array.isArray(STATE.rfqs)) STATE.rfqs = [];
    if(!Array.isArray(STATE.offers)) STATE.offers = [];
    if(!Array.isArray(STATE.orders)) STATE.orders = [];
    if(!Array.isArray(STATE.billing)) STATE.billing = [];
    if(!Array.isArray(STATE.production)) STATE.production = [];

    // algunas versiones antiguas usaban facturacion/items
    if(!Array.isArray(STATE.billing) || !STATE.billing.length){
        const legacy = STATE.facturacion?.items;
        if(Array.isArray(legacy) && legacy.length){
            STATE.billing = legacy;
        }
    }
    // limpia legado para no confundir
    if(STATE.facturacion) delete STATE.facturacion;

    // analytics
    if(!STATE.analytics || typeof STATE.analytics !== 'object') STATE.analytics = { ordersByYear: {}, offersByYear: {}, mode: 'orders' };
    if(!STATE.analytics.ordersByYear) STATE.analytics.ordersByYear = {};
    if(!STATE.analytics.offersByYear) STATE.analytics.offersByYear = {};
    if(!STATE.analytics.mode) STATE.analytics.mode = 'orders';
}

// ------------------------------
// Login / permisos (modo local)
// ------------------------------
function showLogin(force=false){
    const modal = document.getElementById('login-modal');
    const sel = document.getElementById('login-user');
    const btn = document.getElementById('login-enter');
    if (!modal || !sel || !btn) return;
    if (!force && currentUser) return;
    modal.classList.remove('hidden');
    if (currentUser) sel.value = currentUser;
    btn.onclick = () => {
        currentUser = sel.value;
        localStorage.setItem('pg_user', currentUser);
        modal.classList.add('hidden');
        applyAccessControl();
        // Si el usuario no tiene acceso a la vista actual, vuelve a Inicio
        const key = [...document.querySelectorAll('.view')].find(v => !v.classList.contains('hidden'))?.id?.replace('view-','') || 'dashboard';
        if (!canAccess(key)) nav('dashboard');
    };
}

function canAccess(viewKey){
    const u = USERS[currentUser];
    if (!u) return false;
    return u.allow.includes(viewKey);
}

function applyAccessControl(){
    const u = USERS[currentUser];
    if (!u) return;

    // Tooltip de estado
    const status = document.getElementById('autosave-status');
    if (status) status.title = `Usuario: ${currentUser} · Rol: ${u.role}`;

    // Oculta navegación no permitida
    document.querySelectorAll('.nav-btn[data-view]').forEach(btn => {
        const view = btn.getAttribute('data-view');
        btn.style.display = u.allow.includes(view) ? '' : 'none';
    });

    // Restringe acciones sensibles
    const isAdmin = u.role === 'admin';
    const resetBtn = document.getElementById('menu-reset');
    if (resetBtn) resetBtn.style.display = isAdmin ? '' : 'none';
}

function _fmtHHMM(d){
    const hh = String(d.getHours()).padStart(2,'0');
    const mm = String(d.getMinutes()).padStart(2,'0');
    return `${hh}:${mm}`;
}

function updateAutosaveStatus(ts=null){
    const el = document.getElementById('autosave-status');
    if(!el) return;
    let t = ts;
    if(!t) {
        const raw = localStorage.getItem('cch_panel_last_save');
        t = raw ? new Date(parseInt(raw,10)) : null;
    }
    el.textContent = t ? `Guardado OK (${_fmtHHMM(t)})` : '⟳ listo';
}

function saveState() {
    // localStorage tiene un límite; evitamos guardar datasets grandes.
    // Las filas completas de pedidos se almacenan en IndexedDB; aquí solo guardamos resúmenes.
    const safe = JSON.parse(JSON.stringify(STATE));
    // localStorage: nunca guardamos listas gigantes (ofertas/pedidos) para evitar quota
    safe.offers = [];
    safe.orders = [];
    if(safe?.analytics?.ordersByYear){
      for(const y of Object.keys(safe.analytics.ordersByYear||{})){
        if(safe.analytics.ordersByYear[y]?.rows) delete safe.analytics.ordersByYear[y].rows;
      }
    }

    if (safe && safe.analytics && safe.analytics.ordersByYear) {
        for (const y of Object.keys(safe.analytics.ordersByYear)) {
            if (safe.analytics.ordersByYear[y] && safe.analytics.ordersByYear[y].rows) {
                delete safe.analytics.ordersByYear[y].rows; // seguridad extra
            }
        }
    }

    try {
        localStorage.setItem('cch_v55_db', JSON.stringify(safe));
    } catch (e) {
        // Si aun así se excede la cuota, guardamos una versión mínima.
        console.warn('saveState quota exceeded; saving minimal state', e);
        const minimal = {
            __isMinimal: true,
            objectives: safe.objectives,
            analytics: { ordersByYear: safe.analytics?.ordersByYear || {} },
        };
        try {
            localStorage.setItem('cch_v55_db', JSON.stringify(minimal));
        } catch (e2) {
            // Último recurso: no rompemos la UI.
            console.warn('Unable to save even minimal state', e2);
        }
    }
    const now = Date.now();
    localStorage.setItem('cch_panel_last_save', String(now));
    updateAutosaveStatus(new Date(now));

    // Sync opcional con servidor (si existe api.php)
    scheduleServerSave();
}

// --- PARSER OBJETIVOS BLINDADO v55 ---
window.triggerObjUpload = (key) => document.getElementById(`file-obj-${key}`).click();
window.loadObjectiveCSV = function(el, key, filterKeyword='') {
    const file = el.files[0];
    if(!file) return;

    if(file.name.toLowerCase().endsWith('.xlsx')) {
        alert("⚠️ ATENCIÓN: El navegador no puede leer .xlsx. Guarda como CSV en Excel.");
        return;
    }

    // keywords (fallback legacy)
    let kwList = Array.isArray(filterKeyword)
        ? filterKeyword.map(s => String(s || '').toLowerCase()).filter(Boolean)
        : [String(filterKeyword || '').toLowerCase()].filter(Boolean);

    // Regla especial legacy: NACIONAL = CARM = Promoción + Caramelos
    if(key === 'nac' && kwList.length === 1 && kwList[0] === 'promocion') {
        kwList = ['promocion','promoción','caramel','caramelo','caramelos','carm'];
    }

    const reader = new FileReader();
    reader.onload = (e) => {
        try{
            const text = e.target.result || '';
            const lines = text.replace(/^\uFEFF/, '').split(/\r\n|\n/).filter(l => l !== undefined);

            const delim = (lines[0] && lines[0].includes(';')) ? ';' : ',';

            const parseNum = (valStr) => {
                if(valStr === undefined || valStr === null) return 0;
                let s = String(valStr).trim().replace(/"/g,'');
                if(!s) return 0;
                // Normalización ES/EN:
                //  - 334.000  -> 334000
                //  - 334.000,50 -> 334000.50
                //  - 334,50  -> 334.50
                if(s.includes('.') && s.includes(',')) {
                    // punto = miles, coma = decimales
                    s = s.replace(/\./g,'').replace(',','.');
                } else if(s.includes(',') && !s.includes('.')) {
                    // coma = decimales
                    s = s.replace(',','.');
                } else if(s.includes('.') && !s.includes(',')) {
                    // Si el patrón es miles (1.234 o 12.345.678), quitamos los puntos
                    if(/^\d{1,3}(\.\d{3})+$/.test(s)) {
                        s = s.replace(/\./g,'');
                    }
                }
                const n = parseFloat(s);
                return isNaN(n) ? 0 : n;
            };

            const monthMap = {ENERO:0,FEBRERO:1,MARZO:2,ABRIL:3,MAYO:4,JUNIO:5,JULIO:6,AGOSTO:7,SEPTIEMBRE:8,OCTUBRE:9,NOVIEMBRE:10,DICIEMBRE:11};

            // === PARSER NUEVO (CSV FINAL) PARA NACIONAL / EXPORT: Objetivo vs Real ===
            const hasObj = text.toLowerCase().includes('objetivo');
            if((key === 'nac' || key === 'exp' || key === 'hot' || key === 'car') && hasObj){
                const realMonthly = new Array(12).fill(0);
                const objMonthly  = new Array(12).fill(0);

                for(const raw of lines){
                    if(!raw) continue;
                    const cols = raw.split(delim);
                    const m = String(cols[0]||'').trim().toUpperCase();
                    if(monthMap[m] === undefined) continue;
                    const mi = monthMap[m];

                    if(key === 'nac'){
                        // Estructura (según DESGLOSE VENTAS NACIONAL 2025_FINAL.csv):
                        // MES; REAL_CARAM; OBJ_CARAM; ; REAL_PROMO; ; OBJ_PROMO; REAL_TOTAL; ; OBJ_TOTAL
                        const realTotal = parseNum(cols[7]);
                        const objTotal  = parseNum(cols[9]) || (parseNum(cols[2]) + parseNum(cols[6]));
                        realMonthly[mi] = realTotal;
                        objMonthly[mi]  = objTotal;
                    }else if(key === 'exp'){
                        // Estructura (según DESGLOSE VENTAS EXPORT 2025_FINAL.csv):
                        // MES; AGENCIAS; CLIENTES; ... ; TOTAL_REAL; OBJETIVO_TOTAL ...
                        const realTotal = parseNum(cols[5]);
                        const objTotal  = parseNum(cols[6]);
                        realMonthly[mi] = realTotal;
                        objMonthly[mi]  = objTotal;
                    }else if(key === 'hot'){
                        // Estructura (según DESGLOSE VENTAS HOTELES_NACIONAL 2025_FINAL.csv):
                        // MES; MELIA; OBJ_MELIA; ; HOTELES; ; OBJ_HOTELES; TOTAL; ; OBJ_TOTAL
                        const realTotal = parseNum(cols[7]);
                        const objTotal  = parseNum(cols[9]) || (parseNum(cols[2]) + parseNum(cols[6]));
                        realMonthly[mi] = realTotal;
                        objMonthly[mi]  = objTotal;
                    }else if(key === 'car'){
                        // Estructura (según DESGLOSE VENTAS_CARAMELOSNACIONAL 2025_FINAL.csv):
                        // MES; CARAMELOS; OBJETIVO CARAMELOS
                        const realTotal = parseNum(cols[1]);
                        const objTotal  = parseNum(cols[2]);
                        realMonthly[mi] = realTotal;
                        objMonthly[mi]  = objTotal;
                    }
                }

                const realC1 = realMonthly.slice(0,4).reduce((a,b)=>a+b,0);
                const realC2 = realMonthly.slice(4,8).reduce((a,b)=>a+b,0);
                const realC3 = realMonthly.slice(8,12).reduce((a,b)=>a+b,0);

                const objC1 = objMonthly.slice(0,4).reduce((a,b)=>a+b,0);
                const objC2 = objMonthly.slice(4,8).reduce((a,b)=>a+b,0);
                const objC3 = objMonthly.slice(8,12).reduce((a,b)=>a+b,0);

                const realTotal = realC1 + realC2 + realC3;
                const objTotal  = objC1 + objC2 + objC3;

                STATE.objectives.data = STATE.objectives.data || {};
                STATE.objectives.data[key] = {
                    realMonthly, objMonthly,
                    realCuatr: {c1:realC1,c2:realC2,c3:realC3},
                    objCuatr:  {c1:objC1,c2:objC2,c3:objC3},
                    realTotal, objTotal
                };

                // Compat (legacy): STATE.objectives[key] sigue siendo "REAL" para que no rompa nada
                STATE.objectives[key] = realTotal;

                // Para mini-charts legacy
                STATE.objectives.breakdown = STATE.objectives.breakdown || {};
                STATE.objectives.breakdown[key] = {c1:realC1,c2:realC2,c3:realC3};

                STATE.objectives.monthly = STATE.objectives.monthly || {};
                STATE.objectives.monthly[key] = realMonthly;

                saveState();
                updateUI();
                renderMiniCharts();
                renderObjectiveCharts();

                alert(`✅ Carga ${key.toUpperCase()} OK: Real ${formatMoney(realTotal)} | Obj ${formatMoney(objTotal)}`);
                return;
            }

            // === FALLBACK LEGACY (sumatorio de filas por keyword) ===
            let cData = {c1:0, c2:0, c3:0};
            let monthly = new Array(12).fill(0);
            let headerRowIdx = -1;

            for(let i=0; i<lines.length; i++) {
                const row = (lines[i] || '').toLowerCase();
                if(row.includes('enero')) { headerRowIdx = i; break; }
            }
            if(headerRowIdx === -1) {
                alert("⚠️ Error: No se encontró la columna 'ENERO'. Revisa el archivo.");
                return;
            }

            const headerCols = lines[headerRowIdx].split(delim).map(c => c.trim().replace(/"/g, '').toLowerCase());
            const months = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
            const monthIdxs = months.map(m => headerCols.indexOf(m));

            const rowMatches = (rawRow) => {
                if(!kwList.length) return true;
                const rr = (rawRow || '').toLowerCase();
                return kwList.some(k => rr.includes(k));
            };

            for(let i = headerRowIdx + 1; i < lines.length; i++) {
                const rawRow = lines[i];
                if(!rawRow) continue;
                if(!rowMatches(rawRow)) continue;

                const row = rawRow.split(delim).map(c => c.trim());
                if(row.length < 2) continue;
                if((row[0]||'').toLowerCase() === 'total') continue;

                for(let mi=0; mi<12; mi++) {
                    const idx = monthIdxs[mi];
                    if(idx === -1) continue;
                    monthly[mi] += parseNum(row[idx]);
                }
            }

            const c1 = monthly.slice(0,4).reduce((a,b)=>a+b,0);
            const c2 = monthly.slice(4,8).reduce((a,b)=>a+b,0);
            const c3 = monthly.slice(8,12).reduce((a,b)=>a+b,0);

            cData.c1 = c1; cData.c2 = c2; cData.c3 = c3;
            const totalSum = c1 + c2 + c3;

            STATE.objectives[key] = totalSum;
            STATE.objectives.breakdown = STATE.objectives.breakdown || {};
            STATE.objectives.breakdown[key] = cData;

            STATE.objectives.monthly = STATE.objectives.monthly || {};
            STATE.objectives.monthly[key] = monthly;

            saveState();
            updateUI();
            renderMiniCharts();
            renderObjectiveCharts();

            alert(`✅ Carga ${key.toUpperCase()} OK: ${formatMoney(totalSum)}`);
        }catch(err){
            console.error('loadObjectiveCSV error', err);
            alert("⚠️ Error leyendo el CSV. Revisa formato/encoding.");
        }
    };
    reader.readAsText(file, 'ISO-8859-1');
};

// --- RESTO DEL CÓDIGO (ESTABLE) ---

function loadBillingFile(event){
    const file = event.target.files && event.target.files[0];
    if(!file) return;

    const name = (file.name||'').toLowerCase();
    const isCSV = name.endsWith('.csv');

    // Si XLSX no está aún cargado y el usuario sube Excel, intentamos esperar un pelín.
    const tryParseExcel = async () => {
        if(typeof XLSX === 'undefined'){
            // fallback: pedir CSV
            alert('⚠️ No se ha cargado el lector Excel (XLSX). Prueba de nuevo en 3 segundos o sube CSV.');
            return;
        }
        const data = await file.arrayBuffer();
        const wb = XLSX.read(data, { type:'array' });
        const sheetName = wb.SheetNames[0];
        const ws = wb.Sheets[sheetName];
        const rows = XLSX.utils.sheet_to_json(ws, { defval:'', raw:false });
        const parsed = parseBillingRows(rows);
        STATE.billing = parsed;
        saveBillingMeta();
        saveState();
        renderBilling();
        alert(`✅ FACTURACIÓN: ${parsed.length} facturas cargadas.`);
    };

    if(isCSV){
        const reader = new FileReader();
        reader.onload = (e) => {
            const text = e.target.result || '';
            const parsed = parseBillingCSV(text);
            STATE.billing = parsed;
            saveBillingMeta();
            saveState();
            renderBilling();
            alert(`✅ FACTURACIÓN: ${parsed.length} facturas cargadas.`);
        };
        reader.readAsText(file, 'ISO-8859-1');
    }else{
        tryParseExcel();
    }
}

function parseBillingCSV(text){
    const rawLines = String(text||'').split(/\r\n|\n/).map(l=>String(l||'').replace(/\uFEFF/g,'')).filter(l=>l.trim().length);
    if(!rawLines.length) return [];

    // 1) localizar cabecera real (la primera línea que contenga "factura" y "cliente/tercero/razon")
    let headerIdx = rawLines.findIndex(l => {
        const ll = l.toLowerCase();
        return ll.includes('factura') && (ll.includes('cliente') || ll.includes('tercero') || ll.includes('razon') || ll.includes('razón') || ll.includes('empresa'));
    });
    if(headerIdx === -1) headerIdx = 0;

    const headerLine = rawLines[headerIdx];

    // 2) detectar delimitador usando la cabecera (evita que una línea de resumen rompa la detección)
    const delimScores = [
        { d:';', n:(headerLine.match(/;/g)||[]).length },
        { d:'\t', n:(headerLine.match(/\t/g)||[]).length },
        { d:',', n:(headerLine.match(/,/g)||[]).length },
        { d:'|', n:(headerLine.match(/\|/g)||[]).length },
    ].sort((a,b)=>b.n-a.n);
    const delim = (delimScores[0] && delimScores[0].n>0) ? delimScores[0].d : (headerLine.includes(';')?';':',');

    const headers = splitCSVLine(headerLine, delim).map(h => String(h||'').trim());

    // 3) Algunas exportaciones vienen agrupadas por comercial y precedidas por líneas tipo:
    //    "Total pendiente de JUAN MANUEL RIEGO : 70 Facturas , 127.181,85€"
    let currentCommercial = '';
    const rows = [];

    for(let i=headerIdx+1;i<rawLines.length;i++){
        const line = rawLines[i];
        const ll = line.toLowerCase();

        // detectar línea de resumen por comercial
        const mTot = line.match(/total\s+pendiente\s+de\s+(.+?)\s*:\s*\d+/i);
        if(mTot){
            currentCommercial = String(mTot[1]||'').trim();
            continue;
        }

        if(!line.trim()) continue;

        const cols = splitCSVLine(line, delim);

        // Si es una línea suelta con el nombre del comercial (sin ":"), también la aceptamos
        if(cols.filter(c=>String(c||'').trim()).length === 1 && !ll.includes('factura')){
            const solo = String(cols[0]||'').trim();
            if(solo.length >= 2) { currentCommercial = solo; continue; }
        }

        const obj = {};
        headers.forEach((h,idx)=> obj[h] = (cols[idx] ?? ''));

        // si no trae comercial en columnas, lo inyectamos desde el bloque
        if(currentCommercial){
            const hasCommercialHeader = headers.some(h=>{
                const nk = normKey(h);
                return nk.includes('comercial') || nk.includes('vendedor') || nk.includes('agente') || nk.includes('responsable');
            });
            if(!hasCommercialHeader){
                obj['Comercial'] = currentCommercial;
            }
        }
        rows.push(obj);
    }

    return parseBillingRows(rows);
}

function normKey(k){
    return String(k||'').toLowerCase()
      .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
      .replace(/\s+/g,' ')
      .trim();
}
function pick(obj, candidates){
    const map = {};
    Object.keys(obj||{}).forEach(k => map[normKey(k)] = obj[k]);
    for(const c of candidates){
        const v = map[normKey(c)];
        if(v !== undefined && v !== null && String(v).trim() !== '') return String(v).trim();
    }
    return '';
}
function parseMoney(v){
    const s = String(v||'').replace(/\s/g,'').replace(/[€]/g,'');
    if(!s) return 0;
    // 1.234,56 o 1,234.56
    const hasComma = s.includes(',');
    const hasDot = s.includes('.');
    let n = s;
    if(hasComma && hasDot){
        // si la coma va al final como decimal
        if(s.lastIndexOf(',') > s.lastIndexOf('.')){
            n = s.replace(/\./g,'').replace(',', '.');
        }else{
            n = s.replace(/,/g,'');
        }
    }else if(hasComma && !hasDot){
        n = s.replace(/\./g,'').replace(',', '.');
    }else{
        n = s.replace(/,/g,'');
    }
    const out = parseFloat(n);
    return Number.isFinite(out) ? out : 0;
}
function parseYearFromRow(r){
    const id = String(r.id||'');
    const m = id.match(/(19|20)\d{2}/);
    if(m) return m[0];
    const d = String(r.date||r.emit||'');
    const m2 = d.match(/(19|20)\d{2}/);
    if(m2) return m2[0];
    return '';
}

function yearFromAnyDate(v){
  if(v===null||v===undefined) return null;
  if(typeof v==='number'){
    const n = Math.trunc(v);
    if(n>1900 && n<2100) return String(n);
  }
  const s = String(v).trim();
  if(!s) return null;
  const m = s.match(/(19|20)\d{2}/);
  if(m) return m[0];
  const base = s.split(/[T\s]/)[0];
  const parts = base.split(/[\/-]/);
  if(parts.length===3){
    if(parts[0].length===4) return parts[0];
    if(parts[2].length===4) return parts[2];
    if(parts[2].length===2){
      const yy = parseInt(parts[2],10);
      if(Number.isFinite(yy)) return String(yy>=70 ? 1900+yy : 2000+yy);
    }
  }
  return null;
}

function deriveSerieCategory(factura){
    const f = String(factura||'').trim().toUpperCase();
    const prefix = (f.match(/^[A-Z]{2}/)||[''])[0];
    if(prefix === 'EA') return { serie:'EA', category:'Export' };
    if(prefix === 'FN') return { serie:'FN', category:'Nacional' };
    if(prefix) return { serie: prefix, category:'Hoteles' };
    return { serie:'', category:'' };
}

const BILLING_META_KEY = 'cch_billing_meta_v1';
let BILLING_META = null;
function loadBillingMeta(){
    if(BILLING_META) return BILLING_META;
    try{
        BILLING_META = JSON.parse(localStorage.getItem(BILLING_META_KEY) || '{}') || {};
    }catch(e){ BILLING_META = {}; }
    return BILLING_META;
}
function saveBillingMeta(){
    try{
        localStorage.setItem(BILLING_META_KEY, JSON.stringify(loadBillingMeta()));
    }catch(e){}
}
function billingKey(row){
    return `${row.id||''}||${row.client||''}`.toLowerCase();
}

function parseBillingRows(rawRows){
    const meta = loadBillingMeta();
    const out = [];
    (rawRows||[]).forEach(o => {
        const factura = pick(o, ['Factura','Nº Factura','Numero Factura','N_Factura','Factura Nº','Doc','Documento']);
        const cliente = pick(o, ['Cliente','Nombre','Tercero','Razón social','Razon social','Empresa']);
        const comercial = pick(o, ['Comercial','Vendedor','Agente','Responsable','Nombre Comercial']);
        const emision = pick(o, ['Emisión','Emision','Fecha','Fecha emisión','Fecha Emisión','Fecha Doc','F. Emisión','F Emision']);
        const venc = pick(o, ['Vencimiento','Vto','Fecha vto','Fecha Vto','F. Vto','F Vto','Venc']);
        const importe = pick(o, ['Pendiente','Importe pendiente','Imp. pendiente','Importe Pdte','Saldo','Importe','Total']);
        if(!factura && !cliente && !importe) return;

        const serieInfo = deriveSerieCategory(factura);
        const row = {
            id: factura,
            client: cliente,
            agent: comercial,
            emit: emision,
            vto: venc,
            amount: parseMoney(importe),
            year: '',
            serie: serieInfo.serie,
            serieCategory: serieInfo.category,
            pending: true,
            reclaimed: false,
            payApprox: '',
            notes: pick(o, ['Descripción','Descripcion','Detalle','Observaciones','Observacion','Obs'])
        };
        row.year = parseYearFromRow(row) || '';
        const k = billingKey(row);
        const st = meta[k] || {};
        if(typeof st.pending === 'boolean') row.pending = st.pending;
        if(typeof st.reclaimed === 'boolean') row.reclaimed = st.reclaimed;
        if(st.payApprox) row.payApprox = st.payApprox;

        // default: si importe 0, la descartamos
        if(row.amount <= 0) return;
        out.push(row);
    });
    // ordenar por importe desc
    out.sort((a,b)=> (b.amount||0)-(a.amount||0));
    // poblar años selector
    try{ populateBillingYears(out); }catch(e){}
    return out;
}

function populateBillingYears(rows){
    const sel = document.getElementById('bill-year');
    if(!sel) return;
    const years = Array.from(new Set((rows||[]).map(r=>r.year).filter(Boolean))).sort();
    const cur = sel.value;
    sel.innerHTML = '<option value="">Todos</option>' + years.map(y=>`<option value="${y}">${y}</option>`).join('');
    if(years.includes(cur)) sel.value = cur;
}

function initBillingUI(){
    const fsBtn = document.getElementById('btn-fact-fullscreen');
    const view = document.getElementById('view-facturacion');
    if(fsBtn && view){
        fsBtn.onclick = () => view.classList.toggle('facturacion-fullscreen');
    }
    ['bill-search','bill-serie','bill-year','bill-sales','bill-only-pending','bill-only-reclaimed'].forEach(id=>{
        const el = document.getElementById(id);
        if(el) el.addEventListener('input', () => renderBilling());
        if(el) el.addEventListener('change', () => renderBilling());
    });
}

function renderBilling(){
    const tbody = document.getElementById('tbody-billing');
    if(!tbody) return;

    const q = String((document.getElementById('bill-search')||{}).value||'').toLowerCase().trim();
    const serieCat = String((document.getElementById('bill-serie')||{}).value||'');
    const year = String((document.getElementById('bill-year')||{}).value||'');
    const agent = String((document.getElementById('bill-sales')||{}).value||'');
    const onlyPending = !!((document.getElementById('bill-only-pending')||{}).checked);
    const onlyReclaimed = !!((document.getElementById('bill-only-reclaimed')||{}).checked);

    const rows = (STATE.billing||[]).filter(r=>{
        if(serieCat && r.serieCategory !== serieCat) return false;
        if(year && String(r.year) !== String(year)) return false;
        if(agent && String(r.agent||'') !== agent) return false;
        if(onlyPending && !r.pending) return false;
        if(onlyReclaimed && !r.reclaimed) return false;
        if(q){
            const hay = `${r.id} ${r.client} ${r.notes} ${r.agent} ${r.serie} ${r.year}`.toLowerCase();
            if(!hay.includes(q)) return false;
        }
        return true;
    });

    // KPIs
    const count = rows.length;
    const amount = rows.reduce((s,r)=>s+(r.amount||0),0);
    const reclaimed = rows.filter(r=>r.reclaimed).length;
    const setTxt = (id,val)=>{ const el=document.getElementById(id); if(el) el.innerText = val; };
    setTxt('bill-kpi-count', String(count));
    setTxt('bill-kpi-amount', formatMoney(amount));
    setTxt('bill-kpi-reclaimed', String(reclaimed));

    // Summary by agent
    const byAgent = {};
    rows.forEach(r=>{
        const k = r.agent || 'Sin comercial';
        if(!byAgent[k]) byAgent[k] = { count:0, amount:0 };
        byAgent[k].count += 1;
        byAgent[k].amount += (r.amount||0);
    });
    const summary = document.getElementById('bill-summary');
    if(summary){
        const entries = Object.entries(byAgent).sort((a,b)=> b[1].amount - a[1].amount);
        summary.innerHTML = entries.map(([name, v]) => `
          <div class="bill-summary-row">
            <div class="name">${escapeHtml(name)}</div>
            <div class="meta">${v.count} · ${formatMoney(v.amount)}</div>
          </div>`).join('') || '<div style="color:#64748b;font-weight:700">Sin datos (aplica filtros o carga Excel).</div>';
    }

    // Chart
    try{
        const ctx = document.getElementById('bill-chart');
        if(ctx){
            const labels = Object.keys(byAgent);
            const data = labels.map(k=> byAgent[k].amount);
            if(charts.billChart) charts.billChart.destroy();
            charts.billChart = safeChart(ctx, {
                type: 'bar',
                data: { labels, datasets: [{ label: '€ pendientes', data }] },
                options: { responsive:true, plugins:{ legend:{ display:false } }, scales:{ y:{ ticks:{ callback:(v)=> v>=1000? (v/1000)+'k': v } } } }
            });
        }
    }catch(e){}

    // Table
    const meta = loadBillingMeta();
    tbody.innerHTML = rows.map((r, i)=>{
        const k = billingKey(r);
        const st = meta[k] || {};
        const reclaimedChecked = (typeof st.reclaimed === 'boolean' ? st.reclaimed : r.reclaimed) ? 'checked' : '';
        const pendingChecked = (typeof st.pending === 'boolean' ? st.pending : r.pending) ? 'checked' : '';
        const payApprox = escapeAttr(st.payApprox || r.payApprox || '');
        const status = (typeof st.pending === 'boolean' ? st.pending : r.pending) ? 'Pendiente' : 'Pagada';
        return `
          <tr class="bill-row" data-k="${escapeAttr(k)}">
            <td><button class="bill-expand-btn" type="button" onclick="toggleBillRow(this)"><i class="fa-solid fa-chevron-down"></i></button></td>
            <td style="font-weight:800">${escapeHtml(r.id)}</td>
            <td>${escapeHtml(r.client)}</td>
            <td>${escapeHtml(r.year||'')}</td>
            <td><span class="bill-pill">${escapeHtml(r.serieCategory||r.serie||'')}</span></td>
            <td>${escapeHtml(r.agent||'')}</td>
            <td>${escapeHtml(r.emit||'')}</td>
            <td>${escapeHtml(r.vto||'')}</td>
            <td style="font-weight:900;color:var(--red)">${formatMoney(r.amount||0)}</td>
            <td><input type="checkbox" ${reclaimedChecked} onchange="setBillMeta('${escapeAttr(k)}','reclaimed', this.checked)" /></td>
            <td><input type="date" value="${payApprox}" onchange="setBillMeta('${escapeAttr(k)}','payApprox', this.value)" style="height:34px;border-radius:10px;border:1px solid rgba(148,163,184,.45);padding:0 10px" /></td>
            <td><label class="chip" style="padding:6px 10px"><input type="checkbox" ${pendingChecked} onchange="setBillMeta('${escapeAttr(k)}','pending', this.checked); renderBilling();" /><span>${status}</span></label></td>
          </tr>
          <tr class="bill-row-details">
            <td colspan="12">
              <div class="bill-details-box">
                <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;justify-content:space-between">
                  <div style="font-weight:900">${escapeHtml(r.client)} · <span style="color:#64748b;font-weight:800">${escapeHtml(r.id)}</span></div>
                  <div style="display:flex;gap:8px;flex-wrap:wrap">
                    <span class="bill-pill">Serie: ${escapeHtml(r.serie||'-')}</span>
                    <span class="bill-pill">Vto: ${escapeHtml(r.vto||'-')}</span>
                    <span class="bill-pill">€: ${formatMoney(r.amount||0)}</span>
                  </div>
                </div>
                <div style="margin-top:10px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
                  <span class="bill-pill">1ª recl.: ${escapeHtml(st.reclaim1||'') || '-'}</span>
                  <label style="display:flex;gap:6px;align-items:center;color:#334155;font-weight:800">
                    2ª recl.
                    <input type="date" value="${escapeAttr(st.reclaim2||'')}" onchange="setBillReclaim2('${escapeAttr(k)}', this.value)" style="height:34px;border-radius:10px;border:1px solid rgba(148,163,184,.45);padding:0 10px" />
                  </label>
                  <button class="btn-back-black" type="button" onclick="emitBillReclamation('${escapeAttr(k)}')" style="height:34px;padding:0 12px;font-size:12px">📧 Emitir reclamación</button>
                </div>
                ${r.notes ? `<div style="margin-top:10px;color:#334155"><b>Detalle:</b> ${escapeHtml(r.notes)}</div>` : ''}
              </div>
            </td>
          </tr>
        `;
    }).join('') || `<tr><td colspan="12" style="padding:16px;color:#64748b;font-weight:700">Carga el Excel/CSV para ver facturas pendientes.</td></tr>`;
}

window.toggleBillRow = function(btn){
    const tr = btn.closest('tr');
    if(!tr) return;
    tr.classList.toggle('bill-expanded');
};

window.setBillMeta = function(key, field, value){
    const meta = loadBillingMeta();
    meta[key] = meta[key] || {};
    meta[key][field] = value;

    // Si marca "Reclamada", guardamos fecha 1ª reclamación automáticamente (si no existía)
    if(field === 'reclaimed'){
        if(value && !meta[key].reclaim1){
            meta[key].reclaim1 = new Date().toISOString().slice(0,10);
        }
    }

    saveBillingMeta();

    // Sincroniza en memoria para que KPIs / gráficas se actualicen al instante
    try{
        (STATE.billing||[]).forEach(r=>{
            if(billingKey(r) === key){
                if(field === 'pending') r.pending = !!value;
                if(field === 'reclaimed') r.reclaimed = !!value;
                if(field === 'payApprox') r.payApprox = value || '';
            }
        });
        saveState();
    }catch(e){}

    // Re-render inmediato
    try{ renderBilling(); }catch(e){}
};

window.emitBillReclamation = function(key){
    const rows = STATE.billing || [];
    const meta = loadBillingMeta();
    const st = meta[key] || {};
    const r = rows.find(x => billingKey(x) === key);
    if(!r){ alert('No se encontró la factura.'); return; }

    const subj = `Reclamación factura ${r.id} · ${r.client}`;
    const lines = [];
    lines.push('Buenos días,');
    lines.push('');
    lines.push(`Les rogamos revisen el pago de la factura ${r.id} correspondiente a ${r.client}.`);
    lines.push(`Importe pendiente: ${formatMoney(r.amount||0)}`);
    if(r.vto) lines.push(`Vencimiento: ${r.vto}`);
    if(st.payApprox) lines.push(`Fecha aproximada de pago indicada: ${st.payApprox}`);
    if(st.reclaim1) lines.push(`1ª reclamación: ${st.reclaim1}`);
    if(st.reclaim2) lines.push(`2ª reclamación programada: ${st.reclaim2}`);
    lines.push('');
    lines.push('Quedamos a la espera de confirmación.');
    lines.push('');
    lines.push('Gracias y un saludo,');

        // Join with explicit newline. Fixed broken literal newline in previous build.
    const body = encodeURIComponent(lines.join('\n'));
    const subject = encodeURIComponent(subj);
    window.location.href = `mailto:?subject=${subject}&body=${body}`;
};

window.setBillReclaim2 = function(key, value){
    const meta = loadBillingMeta();
    meta[key] = meta[key] || {};
    meta[key].reclaim2 = value || '';
    saveBillingMeta();
    try{ renderBilling(); }catch(e){}
};

async function loadCSV(event, type) {
    const file = event.target.files[0];
    if(!file) return;

    // XLSX soportado (si el usuario lo sube)
    if(file.name.toLowerCase().endsWith('.xlsx')) {
        const ok = await ensureXLSX();
        if(!ok){ alert('No se pudo cargar el lector XLSX. Guarda como CSV o revisa conexión.'); event.target.value=''; return; }
        const buf = await file.arrayBuffer();
        const wb = XLSX.read(buf, { type: 'array' });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
        const data = parseRowsObject(rows, type);
        STATE[type] = data; saveState(); updateUI(); alert(`✅ ${data.length} registros.`);
        event.target.value = '';
        return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
        const text = e.target.result || '';
        try{
            // Blindaje Chrome: algunos CSV (sobre todo PEDIDOS) traen saltos de línea dentro de campos entrecomillados.
            // Eso rompe el split por líneas y aparenta "CSV roto". Sanitizamos SOLO a nivel de lectura.
            const clean = sanitizeCSVForChrome(text);
            // PEDIDOS (ERP): además de la vista simplificada, necesitamos el detalle para Producción y Lista Pedidos.
            if(String(type)==='orders'){
                const detailed = parseOrdersDetailed(clean);
                STATE.production = detailed.productionRows;
                STATE.productionMeta = { loadedAt: new Date().toISOString(), source: file.name };
                hydrateProductionFiltersSafe();
                renderGanttSafe();

                STATE.orders = detailed.simpleRows;
                saveState();
                updateUI();
                alert(`✅ ${STATE.orders.length} pedidos cargados (detalle listo para Producción/Lista Pedidos).`);
            }else{
                const data = parseRobust(clean, type);
                STATE[type] = data; saveState(); updateUI(); alert(`✅ ${data.length} registros.`);
            }
        }catch(err){
            console.error('loadCSV error', err);
            alert(`⚠️ No se pudo leer el CSV de ${String(type||'').toUpperCase()}. Revisa el formato.`);
        }
        event.target.value = '';
    };
    // Primero intentamos UTF-8; si hay caracteres raros el parser seguirá funcionando
    reader.readAsText(file, 'utf-8');
}

// Helpers seguros (no deben romper si la vista no está montada)
function renderGanttSafe(){ try{ renderGantt(); }catch(e){} }
function hydrateProductionFiltersSafe(){ try{ hydrateProductionFilters(); }catch(e){} }

// CSV -> array de objetos (detecta delimitador a partir de cabecera y respeta comillas)
function csvToObjectsSmart(text){
    const safe = String(text||'').replace(/^\uFEFF/, '').replace(/\r\n/g,'\n').replace(/\r/g,'\n');
    const lines = safe.split(/\n/);
    if(lines.length < 2) return [];
    const headerLine = lines[0];
    const delimScores = [
        { d:';', n:(headerLine.match(/;/g)||[]).length },
        { d:'\t', n:(headerLine.match(/\t/g)||[]).length },
        { d:',', n:(headerLine.match(/,/g)||[]).length },
        { d:'|', n:(headerLine.match(/\|/g)||[]).length },
    ].sort((a,b)=>b.n-a.n);
    const delim = (delimScores[0] && delimScores[0].n>0) ? delimScores[0].d : (headerLine.includes(';')?';':',');
    const headers = splitCSVLine(headerLine, delim).map(h => String(h||'').trim().replace(/^"|"$/g,''));
    const out = [];
    for(let i=1;i<lines.length;i++){
        const line = lines[i];
        if(!line || !String(line).trim()) continue;
        const cols = splitCSVLine(line, delim);
        if(cols.length<=1 && !String(cols[0]||'').trim()) continue;
        const obj = {};
        for(let j=0;j<headers.length;j++){
            const key = headers[j] || ('C'+j);
            obj[key] = (cols[j] ?? '').toString();
        }
        out.push(obj);
    }
    return out;
}

// PEDIDOS (ERP) ->
//  - simpleRows: para Inicio y KPIs
//  - productionRows: para Producción + Lista Pedidos
function parseOrdersDetailed(cleanCsvText){
    const rowsObj = csvToObjectsSmart(cleanCsvText);
    const prod = [];
    const simple = [];
    for(const r of rowsObj){
        const g = (k1, k2, k3) => (r[k1] ?? r[k2] ?? r[k3] ?? '').toString().trim();

        const idPedido = g('Id Pedido','IdPedido','ID Pedido');
        const serie = g('Serie','serie','SERIE');
        const numero = g('Número','Numero','NÚMERO');
        const obs = g('Observaciones','Observacion','OBSERVACIONES');
        const desc = g('Descripción Pedido','Descripcion Pedido','Descripción') || g('Descripcion','Descripcion','DESCRIPCION');
        const estadoDesc = g('EstadoDescrip','Estado Descrip','Estado') || g('Estado','EstadoDescrip','');
        const fechaPed = parseDateEU(g('Fecha Pedido','FechaPedido','Fecha'));
        const fechaSal = parseDateEU(g('Fecha Salida','FechaSalida',''));
        const cliente = g('Cliente','cliente','');
        const sociedad = g('Razón Social','Razon Social','Sociedad');
        const comercial = g('Comercial','Agente','Vendedor','Agente comercial','Agente Comercial','Comercial pedido','Comercial Pedido','Vendedor pedido','Vendedor Pedido','Salesperson','Agent') || '';

        const total = eurosToNumber(g('Total','Total (sin IVA)','Importe'));
        const totalIVA = eurosToNumber(g('TotalIVA','Total IVA','Importe con IVA'));

        if(!serie && !numero && !sociedad && !cliente && !desc) continue;

        const st = statusGroupFromText(estadoDesc);
        const start = fechaPed || '2026-01-01';
        let end = fechaSal || start;
        try{
            const ds = new Date(start+'T00:00:00');
            const de = new Date(end+'T00:00:00');
            if(de < ds) end = start;
        }catch(e){}

        const prodRow = {
            id: `PED-${idPedido||serie+numero}`,
            nombre: `${serie} ${numero} · ${sociedad || cliente || 'Pedido'}`.trim(),
            serie, numero, cliente, sociedad,
            desc: String(desc||'').trim(),
            obs: String(obs||'').trim(),
            estado: String(estadoDesc||'').trim(),
            statusGroup: st,
            start, end,
            total, totalIVA,
            agente: String(comercial||'').trim(),
            notas: '',
            alarm: (st==='INC')
        };
        prod.push(prodRow);

        simple.push({
            client: (sociedad || cliente || 'S/N').trim(),
            amount: (Number.isFinite(totalIVA) && totalIVA) ? totalIVA : total,
            date: fechaPed ? fechaPed.split('-').reverse().join('/') : '01/01/2026',
            agent: String(comercial||'S/A').trim() || 'S/A',
            status: String(estadoDesc||'Pendiente').trim() || 'Pendiente',
            desc: String(desc||'-').trim() || '-',
            id: (idPedido || `${serie} ${numero}`).trim(),
            notes: ''
        });
    }
    return { simpleRows: simple, productionRows: prod };
}

// --- SANITIZADO CSV (Chrome-safe) ---
// - Quita BOM
// - Normaliza CRLF/CR -> \n
// - Convierte \n dentro de comillas a espacios (mantiene el texto pero evita romper filas)
function sanitizeCSVForChrome(input){
    let s = String(input || '');
    // BOM
    s = s.replace(/^\uFEFF/, '');
    // normalizar saltos
    s = s.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

    let out = '';
    let inQ = false;
    for(let i=0;i<s.length;i++){
        const ch = s[i];
        if(ch === '"'){
            // comillas escapadas "" dentro de un campo
            if(inQ && s[i+1] === '"'){
                out += '"';
                i++;
            }else{
                inQ = !inQ;
                out += ch;
            }
        }else if(ch === '\n' && inQ){
            out += ' '; // sustituimos salto por espacio dentro de campo
        }else{
            out += ch;
        }
    }
    return out;
}


function splitCSVLine(line, delim){
    // Split CSV/SSV line respecting quotes. Handles doubled quotes.
    const out = [];
    let cur = '';
    let inQ = false;
    for(let i=0;i<line.length;i++){
        const ch = line[i];
        if(ch === '"'){
            if(inQ && line[i+1] === '"'){ cur += '"'; i++; }
            else { inQ = !inQ; }
        } else if(!inQ && ch === delim){
            out.push(cur); cur = '';
        } else {
            cur += ch;
        }
    }
    out.push(cur);
    return out;
}

async function ensureXLSX(){
    if(window.XLSX) return true;
    return new Promise((resolve) => {
        const s = document.createElement('script');
        s.src = 'https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js';
        s.onload = () => resolve(true);
        s.onerror = () => resolve(false);
        document.head.appendChild(s);
    });
}
function parseRobust(text, type) {
    const safe = String(text || '').trim();
    if(!safe) return [];
    const lines = safe.split(/\n/);
    if (lines.length < 2) return [];

    // Delimitador: usar cabecera para detectar mejor (;, tab, , , |)
    const headerLine = lines[0];
    const delimScores = [
        { d:';', n:(headerLine.match(/;/g)||[]).length },
        { d:'\t', n:(headerLine.match(/\t/g)||[]).length },
        { d:',', n:(headerLine.match(/,/g)||[]).length },
        { d:'|', n:(headerLine.match(/\|/g)||[]).length },
    ].sort((a,b)=>b.n-a.n);
    const delim = (delimScores[0] && delimScores[0].n>0) ? delimScores[0].d : (headerLine.includes(';')?';':',');

    const headers = splitCSVLine(headerLine, delim).map(h => h.trim().replace(/"/g, '').toLowerCase());
    const find = (k) => headers.findIndex(h => k.some(x => h.includes(x)));
    const idx = { client: find(['cliente','nombre']), amount: find(['total','importe']), date: find(['fecha','inicio']), agent: find(['agente','comercial']), status: find(['estado','situacion']), desc: find(['descrip']) };
    return lines.slice(1).map((line, i) => {
        const row = splitCSVLine(line, delim);
        if(row.length < 2) return null;
        const val = (c) => (c > -1 && row[c]) ? row[c].trim().replace(/"/g, '') : '';
        return { client: val(idx.client)||'S/N', amount: parseFloat(val(idx.amount).replace(/\./g,'').replace(',','.'))||0, date: val(idx.date)||'01/01/2026', agent: val(idx.agent)||'S/A', status: val(idx.status)||'Pendiente', desc: val(idx.desc)||'-', id: i, notes: (STATE[type]&&STATE[type][i])?STATE[type][i].notes:'' };
    }).filter(x => x);
}


function parseRowsObject(rows, type){
    // rows: array of objects from XLSX. Normalize header keys.
    if(!rows || !rows.length) return [];
    const normKey = (k) => String(k||'').trim().replace(/"/g,'').toLowerCase();
    const keys = Object.keys(rows[0]||{}).map(normKey);
    const findKey = (cands) => {
        for(const k of Object.keys(rows[0]||{})){
            const nk = normKey(k);
            if(cands.some(c => nk.includes(c))) return k;
        }
        return null;
    };
    const kClient = findKey(['cliente','nombre']);
    const kAmount = findKey(['total','importe']);
    const kDate   = findKey(['fecha','inicio']);
    const kAgent  = findKey(['agente','comercial']);
    const kStatus = findKey(['estado','situacion']);
    const kDesc   = findKey(['descrip','concepto','detalle']);
    return rows.map((r,i) => {
        const v = (k) => k ? String(r[k] ?? '').trim() : '';
        const amount = parseFloat(v(kAmount).replace(/\./g,'').replace(',','.')) || 0;
        return { client: v(kClient)||'S/N', amount, date: v(kDate)||'01/01/2026', agent: v(kAgent)||'S/A',
                 status: v(kStatus)||'Pendiente', desc: v(kDesc)||'-', id: i, notes: (STATE[type]&&STATE[type][i])?STATE[type][i].notes:'' };
    });
}
function updateUI() {
    const q = String(window.__CCH_SEARCH_Q || '').toLowerCase().trim();
    const fSerie = String(window.__CCH_FILTER_SERIE || 'all');
    const fAgent = String(window.__CCH_FILTER_AGENT || 'Todos');

    const matchSearch = (x) => {
        if(!q) return true;
        return (String(x.client||'').toLowerCase().includes(q) ||
                String(x.desc||'').toLowerCase().includes(q) ||
                String(x.agent||'').toLowerCase().includes(q) ||
                String(x.serie||'').toLowerCase().includes(q) ||
                String(x.id||'').toLowerCase().includes(q));
    };
    const matchSerie = (x) => {
        if(!fSerie || fSerie === 'all') return true;
        const s = String(x.bucket||x.serie||x.section||'').toLowerCase();
        return s.includes(String(fSerie).toLowerCase());
    };
    const matchAgent = (x) => {
        if(!fAgent || fAgent === 'Todos') return true;
        return String(x.agent||'').toLowerCase().includes(String(fAgent).toLowerCase());
    };
    const applyFilters = (arr) => (arr||[]).filter(x => matchSearch(x) && matchSerie(x) && matchAgent(x));

    // KPIs: por defecto tiran de Analítica (2026), y si no hay datos, de las listas cargadas.
    const anOrders26 = STATE.analytics?.ordersByYear?.['2026'];
    const anOffers26 = STATE.analytics?.offersByYear?.['2026'];
    const kpiOrdCount = (anOrders26 && Number.isFinite(anOrders26.count)) ? anOrders26.count : (STATE.orders||[]).length;
    const kpiOrdTotal = (anOrders26 && Number.isFinite(anOrders26.total)) ? anOrders26.total : (STATE.orders||[]).reduce((a,b)=>a+(b.amount||0),0);
    const kpiOffCount = (anOffers26 && Number.isFinite(anOffers26.count)) ? anOffers26.count : (STATE.offers||[]).length;
    const kpiOffTotal = (anOffers26 && Number.isFinite(anOffers26.total)) ? anOffers26.total : (STATE.offers||[]).reduce((a,b)=>a+(b.amount||0),0);
    const elOffC = document.getElementById('kpi-off-count');
    const elOffT = document.getElementById('kpi-off-amount');
    const elOrdC = document.getElementById('kpi-ord-count');
    const elOrdT = document.getElementById('kpi-ord-amount');
    if(elOffC) elOffC.innerText = String(kpiOffCount);
    if(elOffT) elOffT.innerText = formatMoney(kpiOffTotal);
    if(elOrdC) elOrdC.innerText = String(kpiOrdCount);
    if(elOrdT) elOrdT.innerText = formatMoney(kpiOrdTotal);
    const miniOff = document.getElementById('mini-off');
    const miniOrd = document.getElementById('mini-ord');
    if(miniOff) miniOff.innerText = formatMoney(kpiOffTotal);
    if(miniOrd) miniOrd.innerText = formatMoney(kpiOrdTotal);

    // Listas: sí aplican filtros del dashboard
    ['offers','orders'].forEach(t => {
        const arr = applyFilters(STATE[t]);
        const list = document.getElementById(`list-${t}`);
        if(list){
            if(arr.length){
                list.innerHTML = arr.slice(0,30).map((item, i) => `<div class="item-card" onclick="openDetail('${t}', ${i})"><div class="item-tit"><span>${item.client}</span> <strong>${formatMoney(item.amount)}</strong></div><div class="item-desc">${item.desc||'-'}</div></div>`).join('');
            } else {
                // No tocamos el placeholder de subida si no hay datos
                if(STATE[t].length){ list.innerHTML = `<div style="padding:18px;opacity:.7">Sin resultados con los filtros actuales.</div>`; }
            }
        }
    });
    if(STATE.rfqs.length) {
        document.getElementById('kpi-rfq-count').innerText = STATE.rfqs.length;
        const listRfq = document.getElementById('list-rfqs');
        if(listRfq) listRfq.innerHTML = STATE.rfqs.slice(0,30).map((item, i) => `<div class="item-card" onclick="openDetail('rfqs', ${i})"><div class="item-tit"><span>${item.client}</span> <span class="status-pill">${item.status}</span></div><div class="item-desc">${item.desc}</div></div>`).join('');
    }
    ['nac','exp','hot','car'].forEach(k => {
        const elReal = document.getElementById(`val-${k}`);
        const elObj  = document.getElementById(`obj-${k}`);
        const data = (STATE.objectives && STATE.objectives.data && STATE.objectives.data[k]) ? STATE.objectives.data[k] : null;
        const realVal = (data && Number.isFinite(data.realTotal)) ? data.realTotal : STATE.objectives[k];
        const objVal  = (data && Number.isFinite(data.objTotal))  ? data.objTotal  : null;

        if(elReal) elReal.innerText = formatMoney(realVal);
        if(elObj && objVal !== null) elObj.innerText = formatMoney(objVal);
    });
    renderBilling();

    try{ updateStrategicPanel(); }catch(e){}
}

// GANTT
// ================================
// PRODUCCIÓN · Gantt (modo local)
// - NO puede dejar la pantalla en blanco
// - Contenedor se autocrea si falta
// - Edición solo para Alejandro + David/Verónica/Rafa
// ================================

function canEditProduction(){
    const u = USERS[currentUser];
    if(!u) return false;
    return (u.role === 'admin' || u.role === 'produccion');
}

// ================================
// PRODUCCIÓN · Carga CSV Pedidos 2026 + filtros
// CSV esperado (separador ;) con cabeceras:
// Id Pedido, Serie, Número, Observaciones, Descripción Pedido, EstadoDescrip, Fecha Pedido, Fecha Salida, Cliente, Razón Social, ...
// ================================

function parseDateEU(v){
    // "02/01/2026 0:00" -> "2026-01-02"
    const s = String(v||'').trim();
    if(!s) return '';
    const m = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if(!m) return '';
    const dd = m[1].padStart(2,'0');
    const mm = m[2].padStart(2,'0');
    const yy = m[3];
    return `${yy}-${mm}-${dd}`;
}

function statusGroupFromText(t){
    const s = String(t||'').toUpperCase();
    if(s.includes('INCID') || s.includes('DEMOR') || s.includes('RETRAS')) return 'INC';
    if(s.includes('PROCES') || s.includes('EN CURSO') || s.includes('CURSO')) return 'PROC';
    if(s.includes('COMPLET') || s.includes('FINAL')) return 'FIN';
    if(s.includes('PEND')) return 'PEND';
    return 'PEND';
}

async function loadProductionCSVFile(file){
    const raw = await file.text();
    const clean = sanitizeCSVForChrome(raw);
    const detailed = parseOrdersDetailed(clean);
    STATE.production = detailed.productionRows;
    // también actualiza la vista simplificada si el usuario usa el cargador de Producción
    if(Array.isArray(detailed.simpleRows) && detailed.simpleRows.length){
        STATE.orders = detailed.simpleRows;
    }
    STATE.productionMeta = { loadedAt: new Date().toISOString(), source: file.name };
    saveState();
    // refresca selects
    setTimeout(()=>{ hydrateProductionFiltersSafe(); renderGanttSafe(); }, 0);
}

function hydrateProductionFilters(){
    const selSerie = document.getElementById('prod-serie');
    const selAg = document.getElementById('prod-agente');
    const selSoc = document.getElementById('prod-sociedad');
    if(!selSerie || !selAg || !selSoc) return;

    const keepSerie = selSerie.value;
    const keepAg = selAg.value;
    const keepSoc = selSoc.value;

    const uniq = (arr) => Array.from(new Set(arr.filter(Boolean))).sort((a,b)=>String(a).localeCompare(String(b),'es'));

    const series = uniq((STATE.production||[]).map(x=>x.serie));
    const sociedades = uniq((STATE.production||[]).map(x=>x.sociedad||x.cliente));

    // agentes: combinamos asignados + usuarios (comerciales/producción)
    const agentsFromRows = (STATE.production||[]).map(x=>x.agente).filter(Boolean);
    const agentsFromUsers = Object.keys(USERS||{}).filter(u => {
        const ru = USERS[u]?.role;
        return ru==='admin' || ru==='produccion' || ru==='comercial';
    });
    const agentes = uniq([...agentsFromUsers, ...agentsFromRows]);

    const fill = (sel, baseLabel, items) => {
        sel.innerHTML = `<option value="">${baseLabel}</option>` + items.map(v=>`<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('');
    };
    fill(selSerie, 'Serie (todas)', series);
    fill(selAg, 'Agente (todos)', agentes);
    fill(selSoc, 'Sociedad (todas)', sociedades);

    selSerie.value = keepSerie;
    selAg.value = keepAg;
    selSoc.value = keepSoc;
}

function getProductionFilters(){
    const q = String(document.getElementById('prod-search')?.value||'').toLowerCase().trim();
    const serie = String(document.getElementById('prod-serie')?.value||'').trim();
    const agente = String(document.getElementById('prod-agente')?.value||'').trim();
    const sociedad = String(document.getElementById('prod-sociedad')?.value||'').trim();
    const stPend = !!document.getElementById('prod-st-pend')?.checked;
    const stProc = !!document.getElementById('prod-st-proc')?.checked;
    const stInc  = !!document.getElementById('prod-st-inc')?.checked;
    const stFin  = !!document.getElementById('prod-st-fin')?.checked;
    const allowed = new Set([
        ...(stPend?['PEND']:[]),
        ...(stProc?['PROC']:[]),
        ...(stInc?['INC']:[]),
        ...(stFin?['FIN']:[]),
    ]);
    return { q, serie, agente, sociedad, allowed };
}

function applyProductionFilters(arr){
    const f = getProductionFilters();
    return (arr||[]).filter(p => {
        if(f.serie && String(p.serie||'')!==f.serie) return false;
        if(f.agente && String(p.agente||'')!==f.agente) return false;
        if(f.sociedad){
            const s = String(p.sociedad||p.cliente||'');
            if(s!==f.sociedad) return false;
        }
        if(f.allowed.size && !f.allowed.has(p.statusGroup||'PEND')) return false;
        if(f.q){
            const hay = [p.numero,p.serie,p.cliente,p.sociedad,p.desc,p.obs,p.agente,p.estado].join(' ').toLowerCase();
            if(!hay.includes(f.q)) return false;
        }
        return true;
    });
}
function ensureProductionSeed(){
    if(Array.isArray(STATE.production) && STATE.production.length) return;
    STATE.production = [
        { id: 'PROD-0001', nombre: 'Producción X', start: '2026-02-01', end: '2026-04-01', notas: '', alarm: true },
        { id: 'PROD-0002', nombre: 'Producción Y', start: '2026-03-05', end: '2026-04-20', notas: '', alarm: false },
    ];
    saveState();
}

function pctFromISO(iso){
    const d = new Date(iso + 'T00:00:00');
    const startY = new Date('2026-01-01T00:00:00');
    const endY = new Date('2026-12-31T00:00:00');
    const total = (endY - startY) || 1;
    const val = (d - startY);
    const pct = (val / total) * 100;
    return Math.max(0, Math.min(100, pct));
}

function pctWidth(startIso, endIso){
    const s = new Date(startIso + 'T00:00:00');
    const e = new Date(endIso + 'T00:00:00');
    const startY = new Date('2026-01-01T00:00:00');
    const endY = new Date('2026-12-31T00:00:00');
    const total = (endY - startY) || 1;
    const w = ((e - s) / total) * 100;
    return Math.max(1, Math.min(100, w));
}

function toast(msg){
    const el = document.getElementById('gantt-toast');
    if(!el) return;
    el.textContent = msg;
    el.classList.remove('hidden');
    setTimeout(() => el.classList.add('hidden'), 2200);
}

function playBeep(){
    try{
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const o = ctx.createOscillator();
        const g = ctx.createGain();
        o.type = 'sine';
        o.frequency.value = 880;
        g.gain.value = 0.06;
        o.connect(g); g.connect(ctx.destination);
        o.start();
        setTimeout(()=>{ o.stop(); ctx.close(); }, 220);
    }catch(e){}
}

let _ganttNoteIndex = null;

window.closeGanttModal = () => {
    const ov = document.getElementById('gantt-modal-overlay');
    if(ov) ov.classList.add('hidden');
    _ganttNoteIndex = null;
};

window.saveGanttNotes = () => {
    if(_ganttNoteIndex === null) return;
    if(!canEditProduction()) { alert('Sin permisos para editar Producción.'); return; }
    const txt = document.getElementById('gantt-notes-text')?.value ?? '';
    STATE.production[_ganttNoteIndex].notas = txt;
    saveState();
    playBeep();
    toast('Notas guardadas.');
    window.closeGanttModal();
    renderGantt();
};

function openGanttNotes(i){
    const ov = document.getElementById('gantt-modal-overlay');
    const ta = document.getElementById('gantt-notes-text');
    const tt = document.getElementById('gantt-modal-title');
    if(!ov || !ta) return;
    _ganttNoteIndex = i;
    if(tt) tt.textContent = `Notas · ${STATE.production[i].nombre}`;
    ta.value = STATE.production[i].notas || '';
    ov.classList.remove('hidden');
    ta.focus();
}

function updateBarFromDates(i){
    const item = STATE.production[i];
    const bar = document.getElementById(`bar-${i}`);
    if(!bar) return;
    bar.style.left = pctFromISO(item.start) + '%';
    bar.style.width = pctWidth(item.start, item.end) + '%';
}

function setDatesFromInputs(i){
    const s = document.getElementById(`start-${i}`)?.value;
    const e = document.getElementById(`end-${i}`)?.value;
    if(!s || !e) return;
    // coherencia
    const ds = new Date(s+'T00:00:00');
    const de = new Date(e+'T00:00:00');
    if(de < ds){
        alert('La fecha fin no puede ser anterior al inicio.');
        return;
    }
    if(!canEditProduction()){
        // vuelve a pintar valores originales
        renderGantt();
        alert('Sin permisos para editar Producción.');
        return;
    }
    const prev = { start: STATE.production[i].start, end: STATE.production[i].end };
    if(!confirm(`Confirmar cambio de fechas\n\n${STATE.production[i].nombre}\nInicio: ${prev.start} → ${s}\nFin: ${prev.end} → ${e}`)){
        renderGantt();
        return;
    }
    STATE.production[i].start = s;
    STATE.production[i].end = e;
    saveState();
    playBeep();
    toast('Fechas actualizadas.');
    updateBarFromDates(i);
}

// Drag simple (mueve toda la barra)
function attachDrag(bar, i){
    let startX = 0;
    let baseLeftPct = 0;
    let baseStart = '';
    let baseEnd = '';
    const area = bar.parentElement;
    if(!area) return;
    const isoAddDays = (iso, days) => {
        const d = new Date(iso+'T00:00:00');
        d.setDate(d.getDate() + days);
        return d.toISOString().slice(0,10);
    };

    bar.addEventListener('mousedown', (e) => {
        if(!canEditProduction()) return;
        startX = e.clientX;
        baseLeftPct = parseFloat(bar.style.left || '0');
        baseStart = STATE.production[i].start;
        baseEnd = STATE.production[i].end;
        document.body.style.userSelect = 'none';
        const onMove = (ev) => {
            const rect = area.getBoundingClientRect();
            const dx = ev.clientX - startX;
            const dpct = (dx / (rect.width || 1)) * 100;
            const nextLeft = Math.max(0, Math.min(100, baseLeftPct + dpct));
            bar.style.left = nextLeft + '%';
        };
        const onUp = (ev) => {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            document.body.style.userSelect = '';

            // convertir desplazamiento a días aproximados
            const rect = area.getBoundingClientRect();
            const dx = (ev.clientX - startX);
            const dpct = (dx / (rect.width || 1)) * 100;
            const days = Math.round((dpct / 100) * 365);

            if(!days) { updateBarFromDates(i); return; }
            const ns = isoAddDays(baseStart, days);
            const ne = isoAddDays(baseEnd, days);

            if(!confirm(`Confirmar movimiento\n\n${STATE.production[i].nombre}\n${baseStart} → ${ns}\n${baseEnd} → ${ne}`)){
                // revert visual
                updateBarFromDates(i);
                return;
            }
            STATE.production[i].start = ns;
            STATE.production[i].end = ne;
            saveState();
            playBeep();
            toast('Producción movida.');
            renderGantt();
        };
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    });
}

function ensureGanttContainer(){
    let c = document.getElementById('gantt-container');
    if(c) return c;
    // si el HTML no lo trae (por error), lo creamos dentro de Producción
    const vp = document.getElementById('view-produccion');
    if(!vp) return null;
    const wrap = document.createElement('div');
    wrap.className = 'gantt-wrapper';
    wrap.innerHTML = '<div class="gantt-timeline-header"><div>Ene</div><div>Feb</div><div>Mar</div><div>Abr</div><div>May</div><div>Jun</div><div>Jul</div><div>Ago</div><div>Sep</div><div>Oct</div><div>Nov</div><div>Dic</div></div><div id="gantt-container" class="gantt-container"></div>';
    vp.appendChild(wrap);
    return document.getElementById('gantt-container');
}

function renderGantt() {
    try{
        ensureProductionSeed();
        const c = ensureGanttContainer();
        if(!c){
            console.error('Gantt: no se pudo crear el contenedor');
            return;
        }
        const editable = canEditProduction();
        // Filtros UI (si existen)
        const withIdx = (STATE.production||[]).map((p, idx) => ({...p, __idx: idx}));
        const viewArr = applyProductionFilters(withIdx);

        // KPIs
        const kpiCount = document.getElementById('prod-kpi-count');
        const kpiInc = document.getElementById('prod-kpi-inc');
        const incCount = viewArr.filter(x => (x.statusGroup||'PEND')==='INC').length;
        if(kpiCount) kpiCount.textContent = String(viewArr.length);
        if(kpiInc) kpiInc.textContent = String(incCount);

        // Opciones de agentes
        const agentsFromUsers = Object.keys(USERS||{}).filter(u => {
            const ru = USERS[u]?.role;
            return ru==='admin' || ru==='produccion' || ru==='comercial';
        });
        const agentsFromRows = (STATE.production||[]).map(x=>x.agente).filter(Boolean);
        const agentSet = Array.from(new Set([...agentsFromUsers, ...agentsFromRows])).sort((a,b)=>String(a).localeCompare(String(b),'es'));
        const agentOptions = (selected) => `<option value="">-</option>` + agentSet.map(a => `<option value="${escapeHtml(a)}" ${String(selected||'')===String(a)?'selected':''}>${escapeHtml(a)}</option>`).join('');

        c.innerHTML = viewArr.map((p) => {
            const i = p.__idx;
            const noteLbl = p.notas ? 'Ver nota' : 'Añadir nota';
            const badge = (p.statusGroup==='INC') ? '⚠' : (p.statusGroup==='FIN' ? '✅' : (p.statusGroup==='PROC' ? '⏳' : '🕒'));
            const barCls = (p.statusGroup==='INC') ? 'inc' : (p.statusGroup==='FIN' ? 'fin' : (p.statusGroup==='PROC' ? 'proc' : ''));
            const st = (p.statusGroup||'PEND');
            return `
            <div class="gantt-row">
              <div class="gantt-controls">
                <strong>${badge} ${escapeHtml(p.nombre||'Producción')}</strong>
                <div style="font-size:12px;opacity:.75;margin-top:2px">
                  <div><strong>Pedido:</strong> ${escapeHtml((p.serie||'') + ' ' + (p.numero||''))} · <strong>Sociedad:</strong> ${escapeHtml(p.sociedad||p.cliente||'-')}</div>
                  <div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis"><strong>Descripción:</strong> ${escapeHtml(p.desc||'-')}</div>
                  <div><strong>Estado:</strong> ${escapeHtml(p.estado||'-')}</div>
                </div>

                <div class="prod-status" title="Marcar estado de la producción">
                  <span class="prod-status-label">Marcar:</span>
                  <label class="prod-status-chip"><input type="radio" name="st-${i}" value="PROC" ${st==='PROC'?'checked':''} ${editable?'':'disabled'}> En proceso</label>
                  <label class="prod-status-chip"><input type="radio" name="st-${i}" value="INC" ${st==='INC'?'checked':''} ${editable?'':'disabled'}> Incidencia</label>
                  <label class="prod-status-chip"><input type="radio" name="st-${i}" value="FIN" ${st==='FIN'?'checked':''} ${editable?'':'disabled'}> Completado</label>
                </div>
                <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;align-items:center">
                  <input type="date" id="start-${i}" value="${p.start}" ${editable?'':'disabled'}>
                  <input type="date" id="end-${i}" value="${p.end}" ${editable?'':'disabled'}>
                  <button class="btn-primary-blue" style="height:30px;padding:0 10px;font-size:12px" ${editable?'':'disabled'} onclick="window.__pg_set_dates(${i})">OK</button>
                  <button class="btn-back-black" style="height:30px;padding:0 10px;font-size:12px" onclick="window.__pg_note(${i})">📝 ${noteLbl}</button>
                  <label style="display:flex;gap:6px;align-items:center;font-size:12px;opacity:.85">
                    Agente
                    <select id="agent-${i}" class="input" style="height:30px;min-width:160px" ${editable?'':'disabled'}>
                      ${agentOptions(p.agente)}
                    </select>
                  </label>
                </div>
              </div>
              <div class="gantt-bar-area">
                <div id="bar-${i}" class="gantt-bar-vis ${barCls}" style="left:0%;width:1%;cursor:${editable?'grab':'default'}">${escapeHtml(p.serie||'')} ${escapeHtml(p.numero||'')}</div>
                <i class="fa-solid fa-bell gantt-bell ${p.alarm?'active':''}" title="Alarma" onclick="window.__pg_alarm(${i})"></i>
              </div>
            </div>`;
        }).join('');

        // enlaces
        window.__pg_set_dates = (i) => setDatesFromInputs(i);
        window.__pg_note = (i) => openGanttNotes(i);
        window.__pg_alarm = (i) => {
            if(!canEditProduction()) { alert('Sin permisos para editar Producción.'); return; }
            STATE.production[i].alarm = !STATE.production[i].alarm;
            saveState();
            playBeep();
            toast(STATE.production[i].alarm ? 'Alarma activada.' : 'Alarma desactivada.');
            renderGantt();
        };

        // estado: guardar (PROC / INC / FIN)
        window.__pg_set_status = (i, v) => {
            if(!canEditProduction()) { alert('Sin permisos para editar Producción.'); return; }
            const val = String(v||'').toUpperCase();
            const p = STATE.production[i];
            if(!p) return;
            p.statusGroup = (val==='INC'||val==='PROC'||val==='FIN') ? val : 'PEND';
            // Texto visible "Estado:" (etiqueta rápida)
            if(p.statusGroup==='INC') p.estado = 'INCIDENCIA / DEMORA';
            else if(p.statusGroup==='PROC') p.estado = 'EN PROCESO';
            else if(p.statusGroup==='FIN') p.estado = 'COMPLETADO';
            else p.estado = 'PENDIENTE';

            // Si hay incidencia, pedimos nota mínima y activamos alarma por defecto (se puede apagar)
            if(p.statusGroup==='INC'){
                if(!p.notas || !String(p.notas).trim()){
                    const msg = prompt('Motivo incidencia / demora (recomendado):', '');
                    if(msg && String(msg).trim()) p.notas = String(msg).trim();
                }
                if(!p.alarm) p.alarm = true;
            }

            saveState();
            playBeep();
            renderGantt();
        };

        // agente: guardar asignación
        viewArr.forEach((p)=>{
            const i = p.__idx;
            const sel = document.getElementById(`agent-${i}`);
            if(sel){
                sel.onchange = () => {
                    if(!canEditProduction()) return;
                    STATE.production[i].agente = String(sel.value||'');
                    saveState();
                    try{ hydrateProductionFilters(); }catch(e){}
                };
            }
        });

        // engancha radios de estado
        viewArr.forEach((p)=>{
            const i = p.__idx;
            const radios = document.querySelectorAll(`input[name="st-${i}"]`);
            radios.forEach(r => {
                r.onchange = () => {
                    if(!r.checked) return;
                    window.__pg_set_status(i, r.value);
                };
            });
        });

        // pinta barras y activa drag
        STATE.production.forEach((p, i) => {
            updateBarFromDates(i);
            const bar = document.getElementById(`bar-${i}`);
            if(bar) attachDrag(bar, i);
        });

        // Alarma por incidencias/demoras
        const incActive = viewArr.filter(x => (x.statusGroup||'PEND')==='INC' && !!x.alarm).length;
        window.__pg_inc_active = window.__pg_inc_active ?? 0;
        if(incActive && incActive !== window.__pg_inc_active){
            window.__pg_inc_active = incActive;
            playBeep();
            toast(`⚠ Incidencias activas: ${incActive}`);
        }
        if(!incActive) window.__pg_inc_active = 0;

        if(!editable){
            toast('Producción en solo lectura (sin permisos).');
        }
    }catch(err){
        console.error('renderGantt error', err);
        const vp = document.getElementById('view-produccion');
        if(vp){
            vp.innerHTML = `<div class="section-header"><h2>Producción</h2><span style="color:#c00">Error al cargar Gantt. Revisa consola (F12).</span></div>`;
        }
    }
}

window.startVoiceSearch = () => {
    const input = document.getElementById('global-search');
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    if(!SpeechRecognition){
        alert('Micrófono no disponible en este navegador (SpeechRecognition).');
        return;
    }
    try{
        const rec = new SpeechRecognition();
        rec.lang = 'es-ES';
        rec.interimResults = false;
        rec.maxAlternatives = 1;
        rec.onresult = (ev) => {
            const text = ev.results && ev.results[0] && ev.results[0][0] ? ev.results[0][0].transcript : '';
            if(input){
                input.value = text;
                window.__CCH_SEARCH_Q = String(text||'').toLowerCase().trim();
                updateUI();
            }
        };
        rec.onerror = (e) => {
            console.warn('SpeechRecognition error', e);
            alert('No se pudo iniciar el micrófono (permiso o compatibilidad).');
        };
        rec.start();
    }catch(err){
        console.error(err);
        alert('No se pudo iniciar el micrófono (permiso o compatibilidad).');
    }
};

function filterGlobal() {
    const input = document.getElementById('global-search');
    const q = String(input ? input.value : '').toLowerCase().trim();
    window.__CCH_SEARCH_Q = q;
    updateUI();
    // Si estamos en listas (tabla), repintamos
    const view = location.hash.replace('#','') || '';
    if(view.includes('rfq') || view.includes('offer') || view.includes('order')){
        try{ renderTable(view); }catch(e){}
    }
}

// ================================
// ANALÍTICA · Pedidos multi-año (xlsx/csv)
// Reglas:
// - Solo EstadoDescrip = COMPLETADO (incluye 'COMPLET')
// - Solo totalIVA > 0
// - Serie: EA=EXPORT, FV/FN=NACIONAL, resto=HOTELES, (heurística) CA*/CAR*=CARAMELOS
// Colores por año (fijos): 2024 azul, 2025 verde, 2026 naranja
// ================================

function initAnalyticsHandlers(){
    // Toggle Pedidos / Ofertas
    const tabOrders = document.getElementById('an2-tab-orders');
    const tabOffers = document.getElementById('an2-tab-offers');
    const yearLabel = document.getElementById('an2-year-label');
    const setModeUI = (mode) => {
        STATE.analytics.mode = mode;
        if(tabOrders) tabOrders.classList.toggle('active', mode==='orders');
        if(tabOffers) tabOffers.classList.toggle('active', mode==='offers');
        const uploadBtn = document.getElementById('an2-upload-btn');
        if(yearLabel) yearLabel.textContent = mode==='offers' ? 'AÑO OFERTAS' : 'AÑO PEDIDOS';
        if(uploadBtn) uploadBtn.innerHTML = mode==='offers'
            ? '<i class="fa-solid fa-cloud-arrow-up"></i> Subir OFERTAS (año)'
            : '<i class="fa-solid fa-cloud-arrow-up"></i> Subir PEDIDOS (año)';
        const status = document.getElementById('an2-status');
        if(status){
            status.textContent = mode==='offers'
                ? 'Sin datos. Sube ofertas por año (xlsx/csv) → se comparan automáticamente.'
                : 'Sin datos. Sube pedidos por año (xlsx/csv) → se comparan automáticamente.';
        }
    };
    if(tabOrders) tabOrders.onclick = () => { setModeUI('orders'); saveState(); renderAnalytics(); updateUI(); };
    if(tabOffers) tabOffers.onclick = () => { setModeUI('offers'); saveState(); renderAnalytics(); updateUI(); };

    const btn = document.getElementById('an2-upload-btn');
    const file = document.getElementById('an2-file');
    const yearSel = document.getElementById('an2-year');
    const addYear = document.getElementById('an2-add-year');
    const clear = document.getElementById('an2-clear');
    const expBtn = document.getElementById('an2-export-btn');
    // Inicializa UI segun estado
    setModeUI(STATE.analytics.mode || 'orders');

    if(btn && file){
        btn.onclick = () => file.click();
        file.onchange = async () => {
            const files = Array.from(file.files || []);
            if(!files.length) return;
            const forcedYear = yearSel ? String(yearSel.value || '').trim() : '';
            try{
                for (const f of files){
                    const mode = STATE.analytics.mode || 'orders';
                    const rows = (mode==='offers') ? await readOffersFile(f) : await readOrdersFile(f);

                    // OFERTAS: si el archivo trae varios años, lo partimos SIEMPRE por año
                    // para poder comparar histórico completo en Analítica (aunque el selector tenga un año elegido).
                    if(mode==='offers'){
                        const yearsInFile = collectOfferYears(rows);
                        if(yearsInFile.length > 1){
                            const bucket = STATE.analytics.offersByYear;
                            yearsInFile.forEach(y => {
                                const sub = rows.filter(r => offerRowYear(r) === y);
                                const normY = normalizeOffers(sub, y);
                                const existingY = bucket[y];
                                bucket[y] = mergeNormalizedOrders(existingY, normY);
                            });
                            // Mantén el selector donde estaba, pero ya quedan TODOS los años en memoria
                            toast(`OFERTAS: cargados ${yearsInFile.length} años (${yearsInFile.join(', ')}) desde ${f.name}`,'success');
                            continue;
                        }
                    }
                    const detectedYear = (mode==='offers')
                        ? detectYearFromOffers(rows, forcedYear, f && f.name)
                        : detectYearFromOrders(rows, forcedYear, f && f.name);
                    const norm = (mode==='offers') ? normalizeOffers(rows, detectedYear) : normalizeOrders(rows, detectedYear);

                    const bucket = (mode==='offers') ? STATE.analytics.offersByYear : STATE.analytics.ordersByYear;
                    const existing = bucket[detectedYear];
                    bucket[detectedYear] = mergeNormalizedOrders(existing, norm);
                }
                saveState();
                renderAnalytics();
                updateUI();
	            }catch(err){
	                console.error(err);
	                const msg = (err && err.message) ? err.message : String(err || 'Error desconocido');
	                alert('No se ha podido leer el fichero.\n\nDetalle: ' + msg + '\n\nSi es XLSX, asegúrate de que no está protegido. Si es CSV, revisa el separador (;) o (,) y la cabecera.');
            }finally{
                file.value = '';
            }
        };
    }
    if(addYear && yearSel){
        addYear.onclick = () => {
            const y = prompt('Añadir año (ej: 2019):', '').trim();
            if(!y) return;
            if(!/^\d{4}$/.test(y)) return alert('Año no válido. Usa formato YYYY (ej: 2019).');
            const exists = [...yearSel.options].some(o => o.value === y);
            if(!exists){
                const opt = document.createElement('option');
                opt.value = y;
                opt.textContent = y;
                yearSel.appendChild(opt);
            }
            yearSel.value = y;
        };
    }
    if(clear){
        clear.onclick = () => {
            if(!confirm('¿Borrar los años cargados en Analítica?')) return;
            if((STATE.analytics.mode||'orders') === 'offers') STATE.analytics.offersByYear = {};
            else STATE.analytics.ordersByYear = {};
            saveState();
            renderAnalytics();
            updateUI();
        };
    }

    if(expBtn){
        expBtn.onclick = () => {
            try{ exportHighValueClients(); }
            catch(e){ console.error(e); alert('No se ha podido exportar.'); }
        };
    }
}

// Detecta el año desde un archivo de pedidos.
// Prioridad: columna de Año -> columna Fecha -> fallbackYear (selector)
function detectYearFromOrders(rows, fallbackYear, filename){
    // 0) Nombre de archivo
    const name = String(filename||'');
    const mName = name.match(/(19|20)\d{2}/);
    if(mName) return mName[0];

    const sample = (Array.isArray(rows) && rows.length) ? rows.slice(0, 200) : [];
    // Intento 1: columnas con año
    const yearKeysRe = /(\baño\b|year)/i;
    for(const r of sample){
        const k = Object.keys(r||{}).find(x => yearKeysRe.test(String(x)));
        if(k){
            const v = String(r[k]??'').trim();
            const m = v.match(/(19|20)\d{2}/);
            if(m) return m[0];
        }
    }
    // Intento 2: fecha
    const dateKeyRe = /(fecha|date)/i;
    for(const r of sample){
        const k = Object.keys(r||{}).find(x => dateKeyRe.test(String(x)));
        if(k){
            const d = parseDateFlexible(r[k]);
            if(d && !Number.isNaN(d.getTime())) return String(d.getFullYear());
        }
    }
    // 3) fallback (selector)
    const fb = String(fallbackYear||'').trim();
    if(/^[12]\d{3}$/.test(fb)) return fb;

    // Si no podemos detectarlo, usamos el año actual
    return String(new Date().getFullYear());
}

// Ofertas: misma heurística que pedidos
function detectYearFromOffers(rows, fallbackYear, filename){
    return detectYearFromOrders(rows, fallbackYear, filename);
}

// --- MULTI-AÑO (OFERTAS) ---
// CSV de ofertas con varios años mezclados: necesitamos detectar años y asignar cada fila.
function offerRowYear(row){
    if(!row) return null;
    const raw = (
        row['Fecha'] ?? row['fecha'] ??
        row['Fecha oferta'] ?? row['FechaOferta'] ??
        row['F. Oferta'] ?? row['FechaEmision'] ?? row['Emisión'] ?? row['Emision'] ??
        ''
    );
    const y = yearFromAnyDate(raw);
    return y ? String(y) : null;
}

function collectOfferYears(rows){
    const years = new Set();
    for(const r of (rows||[])) {
        const y = offerRowYear(r);
        if(y) years.add(y);
    }
    return Array.from(years).sort((a,b)=>Number(a)-Number(b));
}

function mergeNormalizedOrders(existing, incoming){
    const toObj = (x) => {
        if(!x) return null;
        if(Array.isArray(x)){
            // Compatibilidad con versiones anteriores
            return x.reduce((acc,it)=> mergeNormalizedOrders(acc, it), null);
        }
        if(x && typeof x === 'object' && Array.isArray(x.rows)) return x;
        return null;
    };

    const a = toObj(existing);
    const b = toObj(incoming);
    if(!a) return b;
    if(!b) return a;

    const year = String(b.year || a.year || new Date().getFullYear());
    const rows = [...(a.rows||[]), ...(b.rows||[])];

    // Recalcula agregados a partir de filas normalizadas
    const byMonth = Array(12).fill(0);
    const bySerie = {};
    const byClient = {};
    let total = 0;
    let count = 0;

    rows.forEach(r => {
        const imp = Number(r.importe)||0;
        if(imp<=0) return;
        total += imp;
        count += 1;
        const dt = r.fecha ? new Date(r.fecha) : null;
        if(dt && !Number.isNaN(dt.getTime())) byMonth[dt.getMonth()] += imp;
        const s = String(r.serie||'').trim() || 'SIN_SERIE';
        bySerie[s] = (bySerie[s]||0) + imp;
        const c = String(r.cliente||'').trim() || 'SIN_CLIENTE';
        byClient[c] = (byClient[c]||0) + imp;
    });

    return { year, rows, total, count, byMonth, bySerie, byClient };
}

function exportHighValueClients(){
  // Section filter (pills). If none selected, we export ALL.
  const secSel = new Set();
  const secN = document.getElementById('an2-sec-nacional');
  const secH = document.getElementById('an2-sec-hoteles');
  const secE = document.getElementById('an2-sec-export');
  const secC = document.getElementById('an2-sec-caramelos');
  if(secN && secN.checked) secSel.add('NACIONAL');
  if(secH && secH.checked) secSel.add('HOTELES');
  if(secE && secE.checked) secSel.add('EXPORT');
  if(secC && secC.checked) secSel.add('CARAMELOS');
    const byYear = (STATE.analytics && STATE.analytics.ordersByYear) ? STATE.analytics.ordersByYear : {};
    const years = Object.keys(byYear).filter(Boolean);
    if(!years.length) return alert('No hay años cargados en Analítica.');

    const fromEl = document.getElementById('an2-from');
    const toEl = document.getElementById('an2-to');
    const minEl = document.getElementById('an2-min');
    const min = parseFloat(minEl && minEl.value ? minEl.value : '3000') || 3000;

    const from = fromEl && fromEl.value ? new Date(fromEl.value + 'T00:00:00') : null;
    const to = toEl && toEl.value ? new Date(toEl.value + 'T23:59:59') : null;

    // 1) Filtra filas por rango de fechas
    const all = [];
    years.forEach(y => {
        const d = byYear[y];
        (d && d.rows ? d.rows : []).forEach(r => {
            const dt = r.fecha ? new Date(r.fecha) : null;
            if(from && (!dt || dt < from)) return;
            if(to && (!dt || dt > to)) return;
            const _sec = String(r.bucket || '').toUpperCase();
            if(secSel.size && _sec && !secSel.has(_sec)) return;
            all.push({
                Año: y,
                Cliente: r.cliente || 'Sin cliente',
                Serie: r.serie || '',
                Sección: r.bucket || '',
                Fecha: dt ? dt.toISOString().slice(0,10) : '',
                Importe: r.amount || 0,
                Observaciones: r.desc || ''
            });
        });
    });
    if(!all.length) return alert('No hay pedidos en el rango seleccionado.');

    // 2) Totales por cliente dentro del rango
    const totByClient = {};
    all.forEach(r => { totByClient[r.Cliente] = (totByClient[r.Cliente]||0) + (r.Importe||0); });
    const allowed = new Set(Object.keys(totByClient).filter(c => totByClient[c] >= min));
    if(!allowed.size) return alert(`No hay clientes por encima de ${min.toLocaleString('es-ES')} € en el rango.`);

    // 3) Filas finales
    const rows = all.filter(r => allowed.has(r.Cliente)).map(r => ({
        Año: r.Año,
        Cliente: r.Cliente,
        Serie: r.Serie,
        Sección: r.Sección,
        Fecha: r.Fecha,
        Importe: r.Importe,
        Observaciones: r.Observaciones
    }));

    // 4) Descarga XLSX (más operativa: columnas anchas, no se solapa el texto)
    const stamp = new Date().toISOString().slice(0,10);
    const baseName = `clientes_mayor_${Math.round(min)}_${stamp}`;

    if(window.XLSX){
        const ws = XLSX.utils.json_to_sheet(rows, {header:['Año','Cliente','Serie','Sección','Fecha','Importe','Observaciones']});
        // Columnas: ancho razonable para que "Observaciones" se lea
        ws['!cols'] = [
            {wch:6},   // Año
            {wch:38},  // Cliente
            {wch:10},  // Serie
            {wch:14},  // Sección
            {wch:14},  // Fecha
            {wch:12},  // Importe
            {wch:90}   // Observaciones
        ];
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, 'Clientes>MIN');
        XLSX.writeFile(wb, `${baseName}.xlsx`);
    } else {
        // Fallback CSV si por cualquier motivo no carga XLSX
        const header = Object.keys(rows[0]);
        const csv = [header.join(';')].concat(rows.map(r => header.map(h => {
            const v = r[h];
            if(h === 'Importe') return String((v||0).toFixed(2)).replace('.',',');
            return String(v ?? '').replace(/\r?\n/g,' ').replace(/;/g,',');
        }).join(';'))).join('\n');
        const blob = new Blob(["\uFEFF"+csv], {type:'text/csv;charset=utf-8'});
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${baseName}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
    }
}

async function readOrdersFile(file){
    const name = (file.name || '').toLowerCase();
    if(name.endsWith('.csv')){
        const text = await file.text();
        // Prefer SheetJS for CSV too (better delimiter handling). Fallback to internal parser.
        if(window.XLSX){
            try {
                const wb = XLSX.read(text, {type:'string'});
                const sheetName = wb.SheetNames?.[0];
                const ws = wb.Sheets[sheetName];
                const rows = XLSX.utils.sheet_to_json(ws, {defval:'', raw:false});
                if(Array.isArray(rows) && rows.length) return rows;
            } catch(e){
                // ignore and fallback
            }
        }
        return csvToRows(text);
    }
    // XLSX
    const buf = await file.arrayBuffer();
    if(!window.XLSX) throw new Error('XLSX library not loaded');
    const wb = XLSX.read(buf, { type:'array' });
    const ws = wb.Sheets[wb.SheetNames[0]];
	// Robust header detection: some XLSX have blank rows or meta rows before the real header.
	const aoa = XLSX.utils.sheet_to_json(ws, { header:1, defval:'' });
	const want = ['cliente','serie','fecha','totaliva','observaciones','observacion','importe','seccion'];
	let headerRow = 0;
	for(let i=0;i<Math.min(10, aoa.length);i++){
	    const row = (aoa[i]||[]).map(x => String(x||'').trim().toLowerCase());
	    const hit = row.filter(v => want.includes(v)).length;
	    const hasClient = row.includes('cliente');
	    const hasDate = row.includes('fecha');
	    if(hit>=2 && hasClient && hasDate){ headerRow=i; break; }
	}
	const headers = (aoa[headerRow]||[]).map(h => String(h||'').trim());
	const out = [];
	for(let r=headerRow+1;r<aoa.length;r++){
	    const row = aoa[r]||[];
	    if(!row.some(v => String(v||'').trim()!=='') ) continue;
	    const obj = {};
	    for(let c=0;c<headers.length;c++){
	        const key = headers[c] || `__col_${c}`;
	        obj[key] = row[c] ?? '';
	    }
	    out.push(obj);
	}
	return out;
}

async function readOffersFile(file){
    // Misma lógica que pedidos pero sin heurística de cabecera tan estricta
    const name = (file.name || '').toLowerCase();
    if(name.endsWith('.csv')){
        const text = await file.text();
        // CSV -> lo tratamos SIEMPRE como CSV (XLSX.read sobre CSV da resultados inconsistentes)
        return csvToRows(text);
    }
    const buf = await file.arrayBuffer();
    if(!window.XLSX) throw new Error('XLSX library not loaded');
    const wb = XLSX.read(buf, {type:'array'});
    const ws = wb.Sheets[wb.SheetNames[0]];
    // Detecta cabecera buscando una fila que contenga al menos Cliente+Fecha o Cliente+Importe
    const aoa = XLSX.utils.sheet_to_json(ws, {header:1, defval:''});
    const want = ['cliente','fecha','importe','total','serie','seccion','observaciones','descripcion'];
    let headerRow = 0;
    for(let i=0;i<Math.min(12, aoa.length);i++){
        const row = (aoa[i]||[]).map(x => String(x||'').trim().toLowerCase());
        const hit = row.filter(v => want.some(w => v.includes(w))).length;
        const hasClient = row.some(v => v.includes('cliente') || v.includes('nombre'));
        const hasDate = row.some(v => v.includes('fecha') || v.includes('date'));
        const hasImp = row.some(v => v.includes('importe') || v.includes('total'));
        if(hit>=2 && hasClient && (hasDate || hasImp)){ headerRow=i; break; }
    }
    const headers = (aoa[headerRow]||[]).map(h => String(h||'').trim());
    const out = [];
    for(let r=headerRow+1;r<aoa.length;r++){
        const row = aoa[r]||[];
        if(!row.some(v => String(v||'').trim()!=='')) continue;
        const obj = {};
        for(let c=0;c<headers.length;c++){
            const key = headers[c] || `__col_${c}`;
            obj[key] = row[c] ?? '';
        }
        out.push(obj);
    }
    return out;
}

// CSV parser (supports quotes + delimiters inside quotes + embedded newlines)
function csvToRows(text){
    const clean = String(text || '').replace(/^\uFEFF/, '');
    if(!clean.trim()) return [];

    // pick delimiter by first line heuristic (supports ; , \t)
    const firstLine = clean.split(/\r\n|\n/)[0] || '';
    const counts = {
        ';': (firstLine.match(/;/g) || []).length,
        ',': (firstLine.match(/,/g) || []).length,
        '\t': (firstLine.match(/\t/g) || []).length,
    };
    const delim = Object.entries(counts)
        .sort((a,b) => b[1]-a[1] || ({';':0, ',':1, '\t':2}[a[0]] - ({';':0, ',':1, '\t':2}[b[0]])))
        [0][0];

    const rows = [];
    let row = [];
    let cur = '';
    let inQ = false;
    for(let i=0;i<clean.length;i++){
        const ch = clean[i];
        const next = clean[i+1];

        if(ch === '"'){
            if(inQ && next === '"'){ // escaped quote
                cur += '"';
                i++;
            }else{
                inQ = !inQ;
            }
            continue;
        }

        if(!inQ && (ch === delim)){
            row.push(cur);
            cur = '';
            continue;
        }

        if(!inQ && (ch === '\n' || ch === '\r')){
            // handle CRLF
            if(ch === '\r' && next === '\n') i++;
            row.push(cur);
            cur = '';
            // ignore empty trailing lines
            if(row.some(c => String(c).trim() !== '')) rows.push(row);
            row = [];
            continue;
        }

        cur += ch;
    }
    // last cell
    if(cur.length || row.length){
        row.push(cur);
        if(row.some(c => String(c).trim() !== '')) rows.push(row);
    }
    if(!rows.length) return [];

    // Strip BOM and normalize header whitespace
    const headers = rows[0].map(h => String(h ?? '').replace(/^\uFEFF/, '').trim());
    return rows.slice(1).map(r => {
        const o = {};
        headers.forEach((h, idx) => o[h] = String(r[idx] ?? '').trim());
        return o;
    });
}

function parseEuroNumber(v){
    if(v === null || v === undefined) return 0;
    let s = String(v).trim();
    if(!s) return 0;
    // Support suffixes like "k" / "mil" / "m" (e.g. "334k€" => 334.000)
    let mult = 1;
    {
        const raw = s.toLowerCase().replace(/\s/g,'').replace(/€/g,'');
        if(/(\d)(k)$/.test(raw) || /(k€)$/.test(raw)) mult = 1000;
        if(/(\d)(m)$/.test(raw) || /(m€)$/.test(raw)) mult = 1000000;
        if(/mil$/.test(raw)) mult = 1000;
        if(/mill(ones)?$/.test(raw) || /millon(es)?$/.test(raw)) mult = 1000000;
    }

    s = s.replace(/\s/g,'').replace(/€/g,'').replace(/"/g,'');
    // remove textual suffixes
    s = s.replace(/mil/ig,'').replace(/millones/ig,'').replace(/millon(es)?/ig,'');
    s = s.replace(/[kKmM]$/,'');
    // Spanish/Euro formats:
    //  - 1.234.567,89  -> 1234567.89
    //  - 334.000       -> 334000
    //  - 334,00        -> 334.00
    if(s.includes('.') && s.includes(',')){
        // '.' thousands, ',' decimal
        s = s.replace(/\./g,'').replace(',','.');
    }else if(s.includes(',')){
        // ',' decimal
        s = s.replace(',','.');
    }else if(s.includes('.')){
        // If we only have dots, in this dataset it's almost always thousands separators.
        // Treat 334.000, 1.234.567 as thousands.
        const looksLikeThousands = /\.(\d{3})(\.|$)/.test(s);
        if(looksLikeThousands) s = s.replace(/\./g,'');
    }
    const n = parseFloat(s);
    return isNaN(n) ? 0 : (n * mult);
}

function parseDateAny(v){
    if(!v) return null;
    if(v instanceof Date && !isNaN(v)) return v;
    // Excel serial
    if(typeof v === 'number' && v > 25000){
        // Excel date serial (rough)
        const epoch = new Date(Date.UTC(1899, 11, 30));
        return new Date(epoch.getTime() + v*86400000);
    }
    const s = String(v).trim();
    // dd/mm/yyyy or dd-mm-yyyy
    const m = s.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
    if(m){
        const d = parseInt(m[1],10);
        const mo = parseInt(m[2],10)-1;
        let y = parseInt(m[3],10);
        if(y < 100) y += 2000;
        const dt = new Date(y, mo, d);
        return isNaN(dt) ? null : dt;
    }
    const dt = new Date(s);
    return isNaN(dt) ? null : dt;
}

function pickKey(obj, rxList){
    const keys = Object.keys(obj || {});
    for(const rx of rxList){
        const k = keys.find(kk => rx.test(String(kk)));
        if(k) return k;
    }
    return null;
}

function serieToBucket(serieRaw){
    const s = String(serieRaw || '').trim().toUpperCase();
    if(s === 'EA') return 'EXPORT';
    if(s === 'FV' || s === 'FN') return 'NACIONAL';
    if(s.startsWith('CA') || s.includes('CAR')) return 'CARAMELOS';
    return 'HOTELES';
}

function normalizeOrders(rows, year){
    const out = {
        year: String(year || '').trim(),
        rows: [],
        total: 0,
        count: 0,
        monthly: new Array(12).fill(0),
        monthlyCount: new Array(12).fill(0),
        byBucket: { NACIONAL:0, EXPORT:0, HOTELES:0, CARAMELOS:0 },
        clients: {}
    };
    if(!Array.isArray(rows) || !rows.length) return out;

    // Importante: en algunos XLSX la cabecera no está completa en la primera fila (o viene con espacios).
    // Por eso detectamos columnas usando la unión de claves de las primeras filas.
    const unionKeys = Array.from(new Set(
        rows.slice(0, 60).flatMap(r => Object.keys(r || {})).filter(Boolean)
    ));
    const keyProbe = {}; unionKeys.forEach(k => keyProbe[k] = 1);

    // columnas confirmadas por ti (con fallback)
    const kAmount = pickKey(keyProbe, [/^totaliva$/i, /total\s*iva/i, /importe/i, /total/i]);
    const kSerie  = pickKey(keyProbe, [/^serie$/i, /serie/i]);
    const kEstado = pickKey(keyProbe, [/estadodescrip/i, /^estado$/i, /situaci/i]);
    const kFecha  = pickKey(keyProbe, [/fechasalida/i, /fecha\s*salida/i, /fecha/i]);
    const kCliente= pickKey(keyProbe, [/^cliente$/i, /cliente/i]);
    // La descripción real suele estar en "Observaciones" (según tus pedidos ERP).
    // Priorizamos Observaciones por encima de otras columnas tipo Descripción.
    const kDesc   = pickKey(keyProbe, [/observaci/i, /observaci[oó]n/i, /descrip/i, /art[ií]cul/i, /pedido/i]);

    rows.forEach(r => {
        const estado = String(r[kEstado] ?? '').toUpperCase();
        if(!estado.includes('COMPLET')) return;
        const amount = parseEuroNumber(r[kAmount]);
        if(!(amount > 0)) return;
        const serie = r[kSerie];
        const bucket = serieToBucket(serie);

        const dt = parseDateAny(r[kFecha]);
        const mi = dt ? dt.getMonth() : null;

        out.total += amount;
        out.count += 1;
        out.byBucket[bucket] = (out.byBucket[bucket]||0) + amount;
        if(mi !== null){
            out.monthly[mi] += amount;
            out.monthlyCount[mi] += 1;
        }
        const cliente = String(r[kCliente] ?? 'Sin cliente').trim() || 'Sin cliente';
        out.clients[cliente] = (out.clients[cliente]||0) + amount;
        // OJO: Observaciones suele venir cargadísimo (comas, saltos, etc). Lo exportaremos tal cual.
        let desc = String(kDesc ? (r[kDesc] ?? '') : '').trim();
        // Fallback robusto: si la cabecera viene con espacios/BOM o variantes
        if(!desc){
            for(const kk of Object.keys(r)){
                if(/observaci/i.test(kk) || /descrip/i.test(kk) || /detalle/i.test(kk)){
                    const vv = String(r[kk] ?? '').trim();
                    if(vv){ desc = vv; break; }
                }
            }
        }
        out.rows.push({ amount, serie: String(serie||''), bucket, cliente, fecha: dt ? dt.toISOString() : '', desc });
    });
    return out;
}

// Normaliza OFERTAS para Analítica (mismo formato que pedidos, sin filtro COMPLETADO)
function normalizeOffers(rows, year){
    const out = {
        year: String(year || '').trim(),
        rows: [],
        total: 0,
        count: 0,
        monthly: new Array(12).fill(0),
        monthlyCount: new Array(12).fill(0),
        byBucket: { NACIONAL:0, EXPORT:0, HOTELES:0, CARAMELOS:0 },
        clients: {}
    };
    if(!Array.isArray(rows) || !rows.length) return out;

    const unionKeys = Array.from(new Set(
        rows.slice(0, 60).flatMap(r => Object.keys(r || {})).filter(Boolean)
    ));
    const keyProbe = {}; unionKeys.forEach(k => keyProbe[k] = 1);

    // Ofertas típicas: Importe/Total, Fecha, Cliente, Serie/Sección, Observaciones/Descripción
    const kAmount = pickKey(keyProbe, [/importe/i, /total/i, /neto/i, /base/i, /subtotal/i]);
    const kSerie  = pickKey(keyProbe, [/^serie$/i, /serie/i, /secci/i, /area/i]);
    const kFecha  = pickKey(keyProbe, [/fecha/i, /creaci/i, /date/i]);
    const kCliente= pickKey(keyProbe, [/^cliente$/i, /cliente/i, /nombre/i]);
    const kDesc   = pickKey(keyProbe, [/observaci/i, /descrip/i, /detalle/i, /concepto/i]);

    rows.forEach(r => {
        const amount = parseEuroNumber(r[kAmount]);
        if(!(amount > 0)) return;
        const serie = r[kSerie];
        const bucket = serieToBucket(serie);
        const dt = parseDateAny(r[kFecha]);
        const mi = dt ? dt.getMonth() : null;

        out.total += amount;
        out.count += 1;
        out.byBucket[bucket] = (out.byBucket[bucket]||0) + amount;
        if(mi !== null){
            out.monthly[mi] += amount;
            out.monthlyCount[mi] += 1;
        }
        const cliente = String(r[kCliente] ?? 'Sin cliente').trim() || 'Sin cliente';
        out.clients[cliente] = (out.clients[cliente]||0) + amount;
        let desc = String(kDesc ? (r[kDesc] ?? '') : '').trim();
        if(!desc){
            for(const kk of Object.keys(r)){
                if(/observaci/i.test(kk) || /descrip/i.test(kk) || /detalle/i.test(kk)){
                    const vv = String(r[kk] ?? '').trim();
                    if(vv){ desc = vv; break; }
                }
            }
        }
        out.rows.push({ amount, serie: String(serie||''), bucket, cliente, fecha: dt ? dt.toISOString() : '', desc });
    });
    return out;
}

function renderAnalytics(){
    const status = document.getElementById('an2-status');
    const kpiWrap = document.getElementById('an2-kpis');
    const mode = (STATE.analytics && STATE.analytics.mode) ? STATE.analytics.mode : 'orders';
    const byYear = (mode==='offers')
        ? (STATE.analytics && STATE.analytics.offersByYear ? STATE.analytics.offersByYear : {})
        : (STATE.analytics && STATE.analytics.ordersByYear ? STATE.analytics.ordersByYear : {});
    const years = Object.keys(byYear).filter(Boolean).sort();

    if(status){
        if(!years.length){
            status.textContent = mode==='offers'
                ? 'Sin datos. Sube ofertas por año (xlsx/csv) → se comparan automáticamente.'
                : 'Sin datos. Sube pedidos por año (xlsx/csv) → se comparan automáticamente.';
        } else {
            status.textContent = mode==='offers'
                ? `Años cargados: ${years.join(', ')} · Datos: Ofertas (importe>0)`
                : `Años cargados: ${years.join(', ')} · Filtro: Estado=COMPLETADO · totalIVA>0`;
        }
    }

    // KPIs por año
    if(kpiWrap){
        kpiWrap.innerHTML = years.map(y => {
            const d = byYear[y];
            const money = formatMoney(d.total || 0);
            const cnt = (d.count || 0).toLocaleString('es-ES');
            // % vs LY
            const idx = years.indexOf(y);
            let pill = '';
            if(idx > 0){
                const prev = byYear[years[idx-1]];
                const base = prev && prev.total ? prev.total : 0;
                const pct = base ? ((d.total - base)/base)*100 : null;
                if(pct !== null){
                    const sign = pct >= 0 ? '+' : '';
                    pill = `<span class="an2-pill">${sign}${pct.toFixed(1)}% vs LY</span>`;
                }
            }
            const sub = mode==='offers' ? `${cnt} ofertas` : `${cnt} pedidos completados`;
            return `
                <div class="an2-kpi">
                    <div class="an2-kpi-head">
                        <div class="an2-kpi-year">${y}</div>
                        ${pill}
                    </div>
                    <div class="an2-kpi-val">${money}</div>
                    <div class="an2-kpi-sub">${sub}</div>
                </div>
            `;
        }).join('');
    }

    // charts
    renderAnalyticsMonthly(years, byYear);
    renderAnalyticsYearly(years, byYear);
    renderAnalyticsSeries(years, byYear);
    renderAnalyticsTopClients(years, byYear);
}

function yearColor(y){
    const yy = String(y);
    if(yy === '2024') return '#3b82f6';
    if(yy === '2025') return '#22c55e';
    if(yy === '2026') return '#f97316';
    // fallback
    return '#6366f1';
}

function withAlpha(hex, a){
    // hex #RRGGBB
    const h = hex.replace('#','');
    const r = parseInt(h.substring(0,2),16);
    const g = parseInt(h.substring(2,4),16);
    const b = parseInt(h.substring(4,6),16);
    return `rgba(${r},${g},${b},${a})`;
}

function destroyChart(id){
    if(charts[id] && charts[id].destroy) charts[id].destroy();
    charts[id] = null;
}

function renderAnalyticsMonthly(years, byYear){
    const el = document.getElementById('an2-monthly');
    if(!el) return;
    destroyChart('an2-monthly');
    const labels = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
    const datasets = years.map(y => {
        const c = yearColor(y);
        const d = byYear[y] || {};
        return {
            label: y,
            data: (d.monthly || new Array(12).fill(0)),
            borderColor: c,
            backgroundColor: withAlpha(c, 0.20),
            fill: true,
            tension: 0.35,
            pointRadius: 2,
        };
    });
    charts['an2-monthly'] = safeChart(el, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: {
                y: { ticks: { callback: v => Number(v).toLocaleString('es-ES') } }
            }
        }
    });
}

function renderAnalyticsYearly(years, byYear){
    const el = document.getElementById('an2-yearly');
    if(!el) return;
    destroyChart('an2-yearly');
    const amounts = years.map(y => (byYear[y]?.total || 0));
    const counts = years.map(y => (byYear[y]?.count || 0));
    const colors = years.map(y => withAlpha(yearColor(y), 0.75));
    charts['an2-yearly'] = safeChart(el, {
        data: {
            labels: years,
            datasets: [
                { type:'bar', label:'Importe (totalIVA)', data: amounts, backgroundColor: colors, yAxisID:'y' },
                { type:'line', label:'Nº pedidos', data: counts, borderColor:'#111827', backgroundColor:'#111827', yAxisID:'y1', tension:0.25, pointRadius:2 }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'top' } },
            scales: {
                y: { position:'left', ticks: { callback: v => Number(v).toLocaleString('es-ES') } },
                y1: { position:'right', grid: { drawOnChartArea: false } }
            }
        }
    });
}

function renderAnalyticsSeries(years, byYear){
    const el = document.getElementById('an2-series');
    if(!el) return;
    destroyChart('an2-series');
    const labels = ['NACIONAL','EXPORT','HOTELES','CARAMELOS'];
    const datasets = years.map(y => {
        const c = yearColor(y);
        const byB = byYear[y]?.byBucket || {};
        return {
            label: y,
            data: labels.map(l => byB[l] || 0),
            backgroundColor: withAlpha(c, 0.80)
        };
    });
    charts['an2-series'] = safeChart(el, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position:'top' } },
            scales: {
                y: { ticks: { callback: v => Number(v).toLocaleString('es-ES') } }
            }
        }
    });
}

function renderAnalyticsTopClients(years, byYear){
    const el = document.getElementById('an2-top');
    if(!el) return;
    destroyChart('an2-top');

    // top 10 global (sum across years)
    const global = {};
    years.forEach(y => {
        const c = byYear[y]?.clients || {};
        Object.entries(c).forEach(([k,v]) => global[k] = (global[k]||0) + v);
    });
    const topClients = Object.entries(global)
        .sort((a,b)=>b[1]-a[1])
        .slice(0,10)
        .map(e=>e[0]);

    const datasets = years.map(y => {
        const c = yearColor(y);
        const m = byYear[y]?.clients || {};
        return {
            label: y,
            data: topClients.map(cl => m[cl] || 0),
            backgroundColor: withAlpha(c, 0.80)
        };
    });
    charts['an2-top'] = safeChart(el, {
        type: 'bar',
        data: { labels: topClients, datasets },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position:'top' } },
            scales: {
                x: { ticks: { callback: v => Number(v).toLocaleString('es-ES') } }
            }
        }
    });
}

function renderTable(view) {
    const t = view.includes('rfq') ? 'rfqs' : (view.includes('offer') ? 'offers' : 'orders');
    const mapTitle = {rfqs:'Lista RFQs', offers:'Lista Ofertas', orders:'Lista Pedidos'};
    document.getElementById('table-title').innerText = mapTitle[t] || t.toUpperCase();

    // Controles dinámicos
    const ctr = document.getElementById('table-controls');
    const sum = document.getElementById('table-summary');
    if(ctr) { ctr.style.display = 'none'; ctr.innerHTML=''; }
    if(sum) { sum.style.display = 'none'; sum.innerHTML=''; }

    // Lista Pedidos: siempre usa el detalle de pedidos (Producción).
    // Si aún no hay datos, mostrará el botón de subida.
    if(t==='orders'){
        renderOrdersFromProduction();
        return;
    }

    if(t==='offers'){
        renderOffersList();
        return;
    }

    document.getElementById('thead').innerHTML = '<th>Cliente</th><th>Fecha</th><th>Importe</th><th>Estado</th><th>Agente</th>';
    const q = (window.__CCH_SEARCH_Q||'').toLowerCase().trim();
    const rows = q ? STATE[t].filter(x => (x.client||'').toLowerCase().includes(q) || (x.desc||'').toLowerCase().includes(q) || String(x.id||'').includes(q)) : STATE[t];
    document.getElementById('tbody').innerHTML = rows.map(i => `<tr><td>${escapeHtml(i.client||'')}</td><td>${escapeHtml(i.date||'')}</td><td>${formatMoney(i.amount||0)}</td><td>${escapeHtml(i.status||'')}</td><td>${escapeHtml(i.agent||'')}</td></tr>`).join('');
}


// ================================
// LISTA OFERTAS (CSV/XLSX robusto)
// ================================

function _cch_safeNum(v){
  if(v==null) return 0;
  if(typeof v==='number' && isFinite(v)) return v;
  const s=String(v).replace(/\s/g,'').replace(/\./g,'').replace(',', '.');
  const n=parseFloat(s);
  return isFinite(n)?n:0;
}

function _cch_normAgent(a){
  const s=String(a||'').trim();
  const up=s.toUpperCase();
  if(/EXPORT/.test(up)) return 'Export';
  if(up==='ALEJANDRO' || /ALEJANDRO/.test(up)){
    if(/EXPORT/.test(up) || /EXPORTACI/.test(up)) return 'Export';
  }
  // normaliza algunos nombres frecuentes
  if(/ALEJANDRO\s+NACIONAL/.test(up)) return 'Alejandro';
  return s || 'Sin agente';
}

function _cch_yearFromDate(d){
  const s=String(d||'').trim();
  const m=s.match(/(20\d{2})/);
  return m?m[1]:'';
}

function _cch_toIsoDate(d){
  if(d==null || d==='') return '';
  if(typeof d==='number' && window.XLSX && XLSX.SSF){
    // Excel serial
    try{
      const dt = XLSX.SSF.parse_date_code(d);
      if(dt && dt.y){
        const mm=String(dt.m).padStart(2,'0');
        const dd=String(dt.d).padStart(2,'0');
        return `${dt.y}-${mm}-${dd}`;
      }
    }catch(e){}
  }
  const s=String(d).trim();
  // yyyy-mm-dd
  if(/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  // dd/mm/yyyy
  const m=s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if(m){
    const dd=String(m[1]).padStart(2,'0');
    const mm=String(m[2]).padStart(2,'0');
    return `${m[3]}-${mm}-${dd}`;
  }
  // fallback: try Date
  const dt=new Date(s);
  if(!isNaN(dt.getTime())){
    const y=dt.getFullYear();
    const mo=String(dt.getMonth()+1).padStart(2,'0');
    const da=String(dt.getDate()).padStart(2,'0');
    return `${y}-${mo}-${da}`;
  }
  return s;
}

function _cch_parseCsvRobusto(text){
  // Mantengo nombre por compatibilidad, pero ahora:
  // - detecta UN delimitador (cabecera)
  // - respeta comillas
  // - asume que el texto ya viene sanitizado (sin \n dentro de comillas)
  const safe = String(text||'');
  const lines = safe.split(/\n/);
  if(lines.length<1) return [];
  const headerLine = lines[0] || '';
  const delimScores = [
    { d:';', n:(headerLine.match(/;/g)||[]).length },
    { d:'\t', n:(headerLine.match(/\t/g)||[]).length },
    { d:',', n:(headerLine.match(/,/g)||[]).length },
    { d:'|', n:(headerLine.match(/\|/g)||[]).length },
  ].sort((a,b)=>b.n-a.n);
  const delim = (delimScores[0] && delimScores[0].n>0) ? delimScores[0].d : (headerLine.includes(';')?';':',');
  return lines.map(line => splitCSVLine(line, delim));
}

async function _cch_readOffersFile(file){
  const name=(file&&file.name||'').toLowerCase();
  if(name.endsWith('.xlsx')){
    if(!window.XLSX) throw new Error('XLSX no está cargado');
    const data = await file.arrayBuffer();
    const wb = XLSX.read(data, {type:'array'});
    const ws = wb.Sheets[wb.SheetNames[0]];
    const json = XLSX.utils.sheet_to_json(ws, {defval:''});
    return json;
  }
  // csv
  const text = await file.text();
  const clean = sanitizeCSVForChrome(text);
  // Convertimos directamente a objetos usando el parser común
  return csvToObjectsSmart(clean);
}

function _cch_mapOfferRow(o){
  const keys = Object.keys(o||{}).reduce((a,k)=>{a[k.toLowerCase().trim()]=k;return a;},{});
  const pick = (...cands)=>{
    for(const c of cands){
      const k=keys[String(c).toLowerCase()];
      if(k!=null) return o[k];
    }
    return '';
  };

  const serie = String(pick('serie','SERIE','SERIE ')||'').trim();
  const numero = String(pick('numoferta','nº oferta','nºoferta','oferta','nº pedido','pedido','numero','número')||'').trim();
  const sociedad = String(pick('sociedad','empresa','razon social','razón social')||'').trim();
  const cliente = String(pick('cliente','id cliente','idcliente','nombre cliente')||'').trim();
  const desc = String(pick('descripcion','descripción','descripcion oferta','descripción oferta')||'').trim();
  const obs = String(pick('observaciones','observacion','obs')||'').trim();
  const agentRaw = pick('comercial','agente','vendedor','responsable');
  const agent = _cch_normAgent(agentRaw);
  const dateIso = _cch_toIsoDate(pick('fecha','fecha oferta','emision','emisión','fecha emisión'));
  const year = _cch_yearFromDate(dateIso) || _cch_yearFromDate(numero) || '';
  const status = String(pick('estadodescrip','estado','situacion','situación')||'').trim();
  const amount = _cch_safeNum(pick('totaliva','importe','total','total iva','importe total'));

  return { serie, numero, sociedad, client: cliente, desc, obs, agent, date: dateIso, year: year||'', status, amount };
}

function renderOffersList(){
  const ctr = document.getElementById('table-controls');
  const sum = document.getElementById('table-summary');
  const thead = document.getElementById('thead');
  const tbody = document.getElementById('tbody');
  if(!thead || !tbody) return;

  STATE.__offers_list = Array.isArray(STATE.__offers_list) ? STATE.__offers_list : [];

  const uniq = (arr)=>Array.from(new Set(arr.filter(Boolean))).sort((a,b)=>String(a).localeCompare(String(b),'es'));
  const years = uniq(STATE.__offers_list.map(r=>String(r.year||'').trim()).filter(Boolean));
  const agents = uniq(STATE.__offers_list.map(r=>r.agent).filter(Boolean));
  const series = uniq(STATE.__offers_list.map(r=>r.serie).filter(Boolean));
  const months = uniq(STATE.__offers_list.map(r=>String(r.date||'').slice(0,7)).filter(Boolean));

  const defaultYear = years.includes('2026') ? '2026' : (years.length?years[years.length-1]:'2026');
  if(!STATE.__offers_year) STATE.__offers_year = defaultYear;

  const getMulti = (id)=>{
    const el=document.getElementById(id);
    if(!el) return [];
    return Array.from(el.selectedOptions||[]).map(o=>String(o.value||'').trim()).filter(Boolean);
  };

  if(ctr){
    ctr.style.display='block';
    ctr.innerHTML = `<div style="display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end">
  <div style="min-width:260px;flex:1">
    <label style="font-size:12px;color:#666">Buscar</label>
    <input id="off-search" placeholder="Oferta, cliente, descripción, observación…" style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px" />
  </div>

  <div style="min-width:220px">
    <label style="font-size:12px;color:#666">Agente (múltiple)</label>
    <select id="off-agent" multiple style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px;min-height:44px">${agents.map(a=>`<option value="${escapeHtml(a)}">${escapeHtml(a)}</option>`).join('')}</select>
    <div style="font-size:11px;color:#888;margin-top:4px">Ctrl/Cmd para seleccionar varios</div>
  </div>

  <div style="min-width:200px">
    <label style="font-size:12px;color:#666">Serie (múltiple)</label>
    <select id="off-serie" multiple style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px;min-height:44px">${series.map(s=>`<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('')}</select>
  </div>

  <div style="min-width:140px">
    <label style="font-size:12px;color:#666">Mes</label>
    <select id="off-month" style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px"><option value="">Todos</option>${months.map(m=>`<option value="${escapeHtml(m)}">${escapeHtml(m)}</option>`).join('')}</select>
  </div>

  <div style="min-width:120px">
    <label style="font-size:12px;color:#666">Año</label>
    <select id="off-year" style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px">${years.map(y=>`<option value="${escapeHtml(y)}">${escapeHtml(y)}</option>`).join('')}</select>
  </div>

  <div style="display:flex;gap:10px;align-items:center">
    <label style="display:flex;gap:8px;align-items:center;border:1px solid #e6e6e6;padding:10px 12px;border-radius:999px;background:#fff"><input type="checkbox" id="off-other-years" ${STATE.__offers_other_years?'checked':''}/> Otros años</label>
    <div style="min-width:160px">
      <label style="font-size:12px;color:#666">Años extra (múltiple)</label>
      <select id="off-years-extra" multiple style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px;min-height:44px">${years.map(y=>`<option value="${escapeHtml(y)}">${escapeHtml(y)}</option>`).join('')}</select>
    </div>
  </div>

  <div style="display:flex;gap:10px;align-items:center;margin-left:auto">
    <button id="off-upload" class="btn-primary"><i class="fa-solid fa-upload"></i> Subir OFERTAS</button>
    <button id="off-export-view" class="btn-primary"><i class="fa-solid fa-file-csv"></i> Exportar CSV (vista)</button>
    <button id="off-export-agent" class="btn-secondary"><i class="fa-solid fa-user"></i> Exportar CSV (por agente)</button>
    <button id="off-reset" class="btn-back-black">Reset</button>
    <input id="off-file" type="file" accept=".csv,.xlsx" style="display:none" />
  </div>
</div>`;
  }

  // defaults
  const sEl=document.getElementById('off-search');
  const aEl=document.getElementById('off-agent');
  const seEl=document.getElementById('off-serie');
  const mEl=document.getElementById('off-month');
  const yEl=document.getElementById('off-year');
  const oyEl=document.getElementById('off-other-years');
  const exEl=document.getElementById('off-years-extra');
  if(sEl) sEl.value = STATE.__offers_q || '';
  if(yEl) yEl.value = STATE.__offers_year || defaultYear;
  if(oyEl) oyEl.checked = !!STATE.__offers_other_years;
  if(exEl){
    const vals=Array.isArray(STATE.__offers_years_extra)?STATE.__offers_years_extra:[];
    Array.from(exEl.options).forEach(o=>o.selected=vals.includes(o.value));
    exEl.disabled = !oyEl.checked;
  }
  if(aEl){
    const vals=Array.isArray(STATE.__offers_agents)?STATE.__offers_agents:[];
    Array.from(aEl.options).forEach(o=>o.selected=vals.includes(o.value));
  }
  if(seEl){
    const vals=Array.isArray(STATE.__offers_series)?STATE.__offers_series:[];
    Array.from(seEl.options).forEach(o=>o.selected=vals.includes(o.value));
  }
  if(mEl) mEl.value = STATE.__offers_month || '';

  const yearsAllowed = ()=>{
    const base=String(yEl?.value||defaultYear).trim();
    const other=!!oyEl?.checked;
    const extra=other?getMulti('off-years-extra'):[];
    return new Set([base, ...extra].filter(Boolean));
  };

  const readFilters = ()=>{
    STATE.__offers_q = String(sEl?.value||'').toLowerCase().trim();
    STATE.__offers_agents = getMulti('off-agent');
    STATE.__offers_series = getMulti('off-serie');
    STATE.__offers_month = String(mEl?.value||'').trim();
    STATE.__offers_year = String(yEl?.value||defaultYear).trim();
    STATE.__offers_other_years = !!oyEl?.checked;
    STATE.__offers_years_extra = getMulti('off-years-extra');
    saveState();
    return {q:STATE.__offers_q, agents:STATE.__offers_agents, series:STATE.__offers_series, month:STATE.__offers_month, yset:yearsAllowed()};
  };

  const filterRows = ()=>{
    const f=readFilters();
    return (STATE.__offers_list||[]).filter(r=>{
      // Ofertas a 0€: no las usamos para nada
      if((r.amount||0) <= 0) return false;
      if(f.yset.size && !f.yset.has(String(r.year||''))) return false;
      if(f.agents.length && !f.agents.includes(r.agent||'')) return false;
      if(f.series.length && !f.series.includes(r.serie||'')) return false;
      if(f.month && String(r.date||'').slice(0,7)!==f.month) return false;
      if(f.q){
        const hay = `${r.serie||''} ${r.numero||''} ${r.sociedad||''} ${r.client||''} ${r.desc||''} ${r.obs||''} ${r.status||''} ${r.agent||''}`.toLowerCase();
        if(!hay.includes(f.q)) return false;
      }
      return true;
    });
  };

  const render = ()=>{
    const rows=filterRows();
    const total=rows.reduce((a,b)=>a+(b.amount||0),0);

    if(sum){
      sum.style.display='block';
      const byAgent={};
      rows.forEach(r=>{ const k=r.agent||'Sin agente'; byAgent[k]=(byAgent[k]||0)+(r.amount||0); });
      const max=Math.max(1, ...Object.values(byAgent));
      const bars=Object.entries(byAgent).sort((a,b)=>b[1]-a[1]).map(([k,v])=>{
        const w=Math.max(4, Math.round((v/max)*100));
        return `<div style="display:flex;align-items:center;gap:10px;margin:6px 0">
          <div style="width:160px;font-weight:700">${escapeHtml(k)}</div>
          <div style="flex:1;background:#f1f3f5;border-radius:999px;height:12px;overflow:hidden"><div style="width:${w}%;height:12px;background:#4c6ef5"></div></div>
          <div style="width:140px;text-align:right;font-weight:800">${fmtEUR(v)}</div>
        </div>`;
      }).join('');

      sum.innerHTML = `<div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between">
        <div style="font-weight:900">Ofertas (filtrado): ${rows.length}</div>
        <div style="font-weight:900">Importe (filtrado): ${fmtEUR(total)}</div>
      </div>
      <div style="margin-top:10px;border:1px solid #eee;border-radius:16px;padding:12px;background:#fff">
        <div style="font-weight:900;margin-bottom:8px">Resumen por agente</div>
        ${bars || '<div style="color:#777">Sin datos</div>'}
      </div>`;
    }

    thead.innerHTML = '<th>Serie</th><th>Nº pedido</th><th>Sociedad</th><th>Cliente</th><th>Descripción</th><th>Observaciones</th><th>Agente</th><th>Fecha</th><th>Estado</th><th style="text-align:right">Importe</th>';
    tbody.innerHTML = rows.map(r=>`<tr>
      <td>${escapeHtml(r.serie||'')}</td>
      <td style="font-weight:900">${escapeHtml(r.numero||'')}</td>
      <td>${escapeHtml(r.sociedad||'')}</td>
      <td>${escapeHtml(r.client||'')}</td>
      <td>${escapeHtml(r.desc||'')}</td>
      <td>${escapeHtml(r.obs||'')}</td>
      <td>${escapeHtml(r.agent||'')}</td>
      <td>${escapeHtml(r.date||'')}</td>
      <td>${escapeHtml(r.status||'')}</td>
      <td style="text-align:right;font-weight:900">${fmtEUR(r.amount||0)}</td>
    </tr>`).join('');
  };

  const rowsToCsv = (rows)=>{
    const headers=['Serie','Pedido','Sociedad','Cliente','Descripción','Agente','Fecha','Estado','Importe'];
    const esc=(v)=>{ const s=String(v??''); const needs=/[",\n;]/.test(s); const out=s.replace(/"/g,'""'); return needs?`"${out}"`:out; };
    const lines=[headers.join(';')];
    rows.forEach(r=>{
      const vals=[r.serie,r.numero,r.sociedad,r.client,r.desc,r.agent,r.date,r.status,(r.amount??0)];
      lines.push(vals.map(esc).join(';'));
    });
    return lines.join('\n');
  };

  const downloadCsv = (content, filename)=>{
    const blob=new Blob([content],{type:'text/csv;charset=utf-8'});
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a'); a.href=url; a.download=filename; document.body.appendChild(a); a.click(); a.remove();
    setTimeout(()=>URL.revokeObjectURL(url), 1500);
  };

  const bind=()=>{
    const rer=()=>{ try{ render(); }catch(e){ console.error(e); } };
    ['off-search','off-agent','off-serie','off-month','off-year','off-other-years','off-years-extra'].forEach(id=>{
      const el=document.getElementById(id);
      if(!el) return;
      el.oninput=()=>{ if(id==='off-other-years' && exEl) exEl.disabled = !oyEl.checked; rer(); };
      el.onchange=()=>{ if(id==='off-other-years' && exEl) exEl.disabled = !oyEl.checked; rer(); };
    });

    const uploadBtn=document.getElementById('off-upload');
    const fileInp=document.getElementById('off-file');
    if(uploadBtn && fileInp){
      uploadBtn.onclick=()=> fileInp.click();
      fileInp.onchange=async (ev)=>{
        const f=ev.target.files && ev.target.files[0];
        if(!f) return;
        try{
          const raw=await _cch_readOffersFile(f);
          const norm=raw.map(_cch_mapOfferRow).filter(r=>r.serie || r.numero || r.client);
          STATE.__offers_list = norm;
          // reset filtros al año por defecto
          const yrs=uniq(norm.map(r=>String(r.year||'').trim()).filter(Boolean));
          const def=yrs.includes('2026')?'2026':(yrs.length?yrs[yrs.length-1]:'2026');
          STATE.__offers_year = def;
          STATE.__offers_other_years = false;
          STATE.__offers_years_extra = [];
          saveState();
          renderOffersList();
        }catch(e){
          console.error(e);
          alert('No se pudo leer el archivo. Prueba con XLSX si el CSV tiene saltos de línea.');
        }finally{
          fileInp.value='';
        }
      };
    }

    const expV=document.getElementById('off-export-view');
    if(expV) expV.onclick=()=>{
      const rows=filterRows();
      downloadCsv(rowsToCsv(rows), `ofertas_filtrado_${(STATE.__offers_year||'')}.csv`);
    };

    const expA=document.getElementById('off-export-agent');
    if(expA) expA.onclick=()=>{
      const rows=filterRows();
      const by={};
      rows.forEach(r=>{ const k=r.agent||'Sin agente'; (by[k]=by[k]||[]).push(r); });
      Object.entries(by).forEach(([k,rr],idx)=>{
        setTimeout(()=>downloadCsv(rowsToCsv(rr), `ofertas_${k.replace(/\s+/g,'_')}.csv`), idx*150);
      });
    };

    const resetBtn=document.getElementById('off-reset');
    if(resetBtn) resetBtn.onclick=()=>{
      STATE.__offers_q=''; STATE.__offers_agents=[]; STATE.__offers_series=[]; STATE.__offers_month='';
      STATE.__offers_other_years=false; STATE.__offers_years_extra=[];
      saveState();
      renderOffersList();
    };
  };

  render();
  bind();
}


// ================================
// LISTA PEDIDOS (desde Producción)
// ================================

function renderOrdersFromProduction(){
    const ctr = document.getElementById('table-controls');
    const sum = document.getElementById('table-summary');
    const thead = document.getElementById('thead');
    const tbody = document.getElementById('tbody');
    if(!thead || !tbody) return;

    // --- Controls UI ---
    const uniq = (arr)=>Array.from(new Set(arr.filter(Boolean))).sort((a,b)=>String(a).localeCompare(String(b),'es'));
    const agentes = uniq((STATE.production||[]).map(x=>x.agente).filter(Boolean));
    const series = uniq((STATE.production||[]).map(x=>x.serie));
    const months = uniq((STATE.production||[]).map(x => (x.start||'').slice(0,7)).filter(Boolean));

    const getMulti = (id)=>{
        const el = document.getElementById(id);
        if(!el) return [];
        return Array.from(el.selectedOptions||[]).map(o=>String(o.value||'').trim()).filter(Boolean);
    };

    const stPend = (STATE.__orders_stPend ?? true);
    const stFin  = (STATE.__orders_stFin  ?? true);

    if(ctr){
        ctr.style.display = 'block';
        ctr.innerHTML = `<div style="display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end">
  <div style="min-width:220px;flex:1">
    <label style="font-size:12px;color:#666">Buscar</label>
    <input id="ord-search" placeholder="Pedido, cliente, descripción, observación…" style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px" />
  </div>

  <div style="min-width:220px">
    <label style="font-size:12px;color:#666">Agente (múltiple)</label>
    <select id="ord-agent" multiple style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px;min-height:44px">
      ${agentes.map(a=>`<option value="${escapeHtml(a)}">${escapeHtml(a)}</option>`).join('')}
    </select>
    <div style="font-size:11px;color:#888;margin-top:4px">Ctrl/Cmd para seleccionar varios</div>
  </div>

  <div style="min-width:200px">
    <label style="font-size:12px;color:#666">Serie (múltiple)</label>
    <select id="ord-serie" multiple style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px;min-height:44px">
      ${series.map(s=>`<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`).join('')}
    </select>
  </div>

  <div style="min-width:140px">
    <label style="font-size:12px;color:#666">Mes</label>
    <select id="ord-month" style="width:100%;padding:10px 12px;border:1px solid #e6e6e6;border-radius:12px">
      <option value="">Todos</option>
      ${months.map(m=>`<option value="${escapeHtml(m)}">${escapeHtml(m)}</option>`).join('')}
    </select>
  </div>

  <div style="display:flex;gap:10px;align-items:center">
    <label style="display:flex;gap:8px;align-items:center;border:1px solid #e6e6e6;padding:10px 12px;border-radius:999px;background:#fff">
      <input type="checkbox" id="ord-st-pend" ${stPend?'checked':''}/> Pendiente
    </label>
    <label style="display:flex;gap:8px;align-items:center;border:1px solid #e6e6e6;padding:10px 12px;border-radius:999px;background:#fff">
      <input type="checkbox" id="ord-st-fin" ${stFin?'checked':''}/> Completado
    </label>
  </div>

  <div style="display:flex;gap:10px;align-items:center;margin-left:auto">
    <button id="ord-upload" class="btn-primary"><i class="fa-solid fa-upload"></i> Subir PEDIDOS</button>
    <button id="ord-export-view" class="btn-primary"><i class="fa-solid fa-file-csv"></i> Exportar CSV (vista)</button>
    <button id="ord-export-agent" class="btn-secondary"><i class="fa-solid fa-user"></i> Exportar CSV (por agente)</button>
    <button id="ord-reset" class="btn-back-black">Reset</button>
    <input id="ord-file" type="file" accept=".csv" style="display:none" />
  </div>
</div>`;
    }

    const setCtrDefaults = ()=>{
        // recuerda últimos filtros
        const s = document.getElementById('ord-search');
        const a = document.getElementById('ord-agent');
        const se= document.getElementById('ord-serie');
        const mo= document.getElementById('ord-month');
        const p = document.getElementById('ord-st-pend');
        const f = document.getElementById('ord-st-fin');
        if(s) s.value = STATE.__orders_q || '';
        if(a){
            const vals = Array.isArray(STATE.__orders_agents)?STATE.__orders_agents: (STATE.__orders_agent?[STATE.__orders_agent]:[]);
            Array.from(a.options).forEach(o=>o.selected = vals.includes(o.value));
        }
        if(se){
            const vals = Array.isArray(STATE.__orders_series)?STATE.__orders_series: (STATE.__orders_serie?[STATE.__orders_serie]:[]);
            Array.from(se.options).forEach(o=>o.selected = vals.includes(o.value));
        }
        if(mo) mo.value = STATE.__orders_month || '';
        if(p) p.checked = (STATE.__orders_stPend ?? true);
        if(f) f.checked = (STATE.__orders_stFin ?? true);
    };
    setCtrDefaults();

    const readFilters = ()=>{
        const q = String(document.getElementById('ord-search')?.value||'').toLowerCase().trim();
        const agentsSel = getMulti('ord-agent');
        const seriesSel = getMulti('ord-serie');
        const month = String(document.getElementById('ord-month')?.value||'').trim();
        const pend = !!document.getElementById('ord-st-pend')?.checked;
        const fin  = !!document.getElementById('ord-st-fin')?.checked;
        STATE.__orders_q = q; STATE.__orders_agents = agentsSel; STATE.__orders_series = seriesSel; STATE.__orders_month=month; STATE.__orders_stPend=pend; STATE.__orders_stFin=fin;
        saveState();
        return {q, agentsSel, seriesSel, month, pend, fin};
    };

    const filterRows = ()=>{
        const f = readFilters();
        const allowed = new Set([
            ...(f.pend?['PEND','PROC','INC']:[]),
            ...(f.fin?['FIN']:[])
        ]);
        return (STATE.production||[]).filter(r=>{
            if(!allowed.has(r.statusGroup||'PEND')) return false;
            if(f.agentsSel && f.agentsSel.length && !f.agentsSel.includes((r.agente||''))) return false;
            if(f.seriesSel && f.seriesSel.length && !f.seriesSel.includes((r.serie||''))) return false;
            if(f.month && (r.start||'').slice(0,7) !== f.month) return false;
            if(f.q){
                const hay = `${r.serie||''} ${r.numero||''} ${r.sociedad||''} ${r.cliente||''} ${r.desc||''} ${r.obs||''} ${r.estado||''}`.toLowerCase();
                if(!hay.includes(f.q)) return false;
            }
            return true;
        });
    };

    const render = ()=>{
        const rows = filterRows();
        const total = rows.reduce((a,b)=>a+(b.totalIVA||b.total||0),0);
        const pendCount = rows.filter(r=>r.statusGroup!=='FIN').length;
        const finCount  = rows.filter(r=>r.statusGroup==='FIN').length;

        // Summary + barras por agente
        if(sum){
            sum.style.display = 'block';
            const byAgent = {};
            rows.forEach(r=>{
                const k = r.agente || 'Sin agente';
                byAgent[k] = (byAgent[k]||0) + (r.totalIVA||r.total||0);
            });
            const max = Math.max(1, ...Object.values(byAgent));
            const bars = Object.entries(byAgent)
                .sort((a,b)=>b[1]-a[1])
                .map(([k,v])=>{
                    const w = Math.max(4, Math.round((v/max)*100));
                    return `
                    <div style="display:flex;align-items:center;gap:10px;margin:6px 0">
                      <div style="width:140px;font-weight:700">${escapeHtml(k)}</div>
                      <div style="flex:1;background:#f1f3f5;border-radius:999px;height:12px;overflow:hidden">
                        <div style="width:${w}%;height:12px;background:#4c6ef5"></div>
                      </div>
                      <div style="width:140px;text-align:right;font-weight:800">${fmtEUR(v)}</div>
                    </div>`;
                }).join('');

            sum.innerHTML = `
            <div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between">
              <div style="font-weight:900">Pedidos (filtrado): ${rows.length} · Pendientes: ${pendCount} · Completados: ${finCount}</div>
              <div style="font-weight:900">Importe (filtrado): ${fmtEUR(total)}</div>
            </div>
            <div style="margin-top:10px;border:1px solid #eee;border-radius:16px;padding:12px;background:#fff">
              <div style="font-weight:900;margin-bottom:8px">Resumen por agente</div>
              ${bars || '<div style="color:#777">Sin datos</div>'}
            </div>`;
        }

        // Table
        thead.innerHTML = '<th>Serie</th><th>Nº pedido</th><th>Sociedad</th><th>Cliente</th><th>Descripción</th><th>Observaciones</th><th>Agente</th><th>Fecha pedido</th><th>Entrega</th><th>Estado</th><th style="text-align:right">Importe</th>';
        tbody.innerHTML = rows.map(r=>{
            const imp = (r.totalIVA||r.total||0);
            const st = r.statusGroup==='FIN' ? 'COMPLETADO' : (r.statusGroup==='INC' ? 'INCIDENCIA' : (r.statusGroup==='PROC' ? 'EN PROCESO' : 'PENDIENTE'));
            const stStyle = r.statusGroup==='INC' ? 'color:#c92a2a;font-weight:900' : (r.statusGroup==='FIN' ? 'color:#2f9e44;font-weight:900' : 'font-weight:800');
            return `<tr>
              <td>${escapeHtml(r.serie||'')}</td>
              <td style="font-weight:900">${escapeHtml(r.numero||'')}</td>
              <td>${escapeHtml(r.sociedad||'')}</td>
              <td>${escapeHtml(r.cliente||'')}</td>
              <td>${escapeHtml(r.desc||'')}</td>
              <td>${escapeHtml(r.obs||'')}</td>
              <td>${escapeHtml(r.agente||'')}</td>
              <td>${escapeHtml(r.start||'')}</td>
              <td>${escapeHtml(r.end||'')}</td>
              <td style="${stStyle}">${escapeHtml(st)}</td>
              <td style="text-align:right;font-weight:900">${fmtEUR(imp)}</td>
            </tr>`;
        }).join('');
    };

    // Upload
    const upBtn = document.getElementById('ord-upload');
    const upInp = document.getElementById('ord-file');
    if(upBtn && upInp){
        upBtn.onclick = () => upInp.click();
        upInp.onchange = async (e)=>{
            const f = e.target.files?.[0];
            if(!f) return;
            try{
                await loadProductionCSVFile(f);
                // refresca opciones por si entran agentes/series nuevas
                renderOrdersFromProduction();
            }catch(err){
                console.error(err);
                alert('No se pudo leer el CSV de PEDIDOS.');
            }finally{
                upInp.value='';
            }
        };
    }


const rowsToCsv = (rows)=>{
    const headers = ['Serie','Pedido','Sociedad','Cliente','Descripción','Agente','Fecha pedido','Entrega','Estado','Importe'];
    const esc = (v)=>{
        const s = String(v??'');
        const needs = /[",\n;]/.test(s);
        const out = s.replace(/"/g,'""');
        return needs ? `"${out}"` : out;
    };
    const line = (arr)=>arr.map(esc).join(';');
    const out = [line(headers)];
    rows.forEach(r=>{
        const imp = (r.totalIVA||r.total||0);
        const st = r.statusGroup==='FIN' ? 'COMPLETADO' : (r.statusGroup==='INC' ? 'INCIDENCIA' : (r.statusGroup==='PROC' ? 'EN PROCESO' : 'PENDIENTE'));
        out.push(line([
            r.serie||'',
            r.numero||'',
            r.sociedad||'',
            r.cliente||'',
            r.desc||'',
            r.agente||'',
            r.start||'',
            r.end||'',
            st,
            (Number(imp)||0).toFixed(2).replace('.',',')
        ]));
    });
    return out.join('\n');
};

const downloadCsv = (filename, csvText)=>{
    try{
        const blob = new Blob(["\ufeff"+csvText], {type:'text/csv;charset=utf-8;'});
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(()=>URL.revokeObjectURL(url), 1000);
    }catch(e){
        console.warn('csv export', e);
        alert('No se pudo exportar CSV.');
    }
};

    // hooks
    const hook = (id, ev='input')=>{
        const el = document.getElementById(id);
        if(!el) return;
        el.oninput = null; el.onchange = null;
        if(ev==='change') el.onchange = ()=>render();
        else el.oninput = ()=>render();
    };
    hook('ord-search','input');
    hook('ord-agent','change');
    hook('ord-serie','change');
    hook('ord-month','change');
    hook('ord-st-pend','change');
    hook('ord-st-fin','change');
    const btnR = document.getElementById('ord-reset');
    if(btnR) btnR.onclick = ()=>{
        STATE.__orders_q=''; STATE.__orders_agents=[]; STATE.__orders_series=[]; STATE.__orders_month=''; STATE.__orders_stPend=true; STATE.__orders_stFin=true;
        saveState();
        renderOrdersFromProduction();
    };


const btnEV = document.getElementById('ord-export-view');
if(btnEV) btnEV.onclick = ()=>{
    const rows = filterRows();
    const f = readFilters();
    const parts = [];
    if(f.month) parts.push(f.month);
    if(f.seriesSel && f.seriesSel.length) parts.push('serie-'+f.seriesSel.join('-'));
    if(f.agentsSel && f.agentsSel.length) parts.push('agente-'+f.agentsSel.join('-'));
    const name = 'pedidos_' + (parts.join('_') || 'vista') + '.csv';
    downloadCsv(name, rowsToCsv(rows));
};

const btnEA = document.getElementById('ord-export-agent');
if(btnEA) btnEA.onclick = ()=>{
    const base = filterRows();
    const selected = getMulti('ord-agent');
    // Si no hay selección, exporta por todos los agentes presentes en el filtrado actual
    const agents = (selected.length ? selected : Array.from(new Set(base.map(r=>r.agente||'Sin agente'))));
    if(!agents.length){ alert('No hay agentes para exportar.'); return; }
    agents.forEach((ag, idx)=>{
        const rows = base.filter(r => (r.agente||'Sin agente') === ag);
        const safeAg = String(ag).replace(/[^a-z0-9áéíóúüñ _-]/gi,'').trim().replace(/\s+/g,'_') || 'Sin_agente';
        const name = `pedidos_${safeAg}.csv`;
        setTimeout(()=>downloadCsv(name, rowsToCsv(rows)), idx*250);
    });
};

    render();
}

// --- MODAL DETALLE EN ESPAÑOL v55 ---
window.openDetail = (t, i) => {
    const item = STATE[t][i];
    document.getElementById('modal-body').innerHTML = `
        <div class="detail-row"><span class="detail-label">CLIENTE</span><span>${item.client}</span></div>
        <div class="detail-row"><span class="detail-label">IMPORTE</span><span style="font-weight:900">${formatMoney(item.amount)}</span></div>
        <div class="detail-row"><span class="detail-label">FECHA</span><span>${item.date}</span></div>
        <div class="detail-row"><span class="detail-label">AGENTE</span><span>${item.agent}</span></div>
        <div class="detail-row"><span class="detail-label">ESTADO</span><span class="status-pill">${item.status}</span></div>
        <div style="margin-top:15px"><span class="detail-label">ASUNTO / NOTAS</span><textarea style="width:100%;height:80px;border:1px solid #ccc;padding:5px;margin-top:5px">${item.desc}</textarea></div>
    `;
    document.getElementById('modal-overlay').classList.remove('hidden');
};
window.closeModal = () => document.getElementById('modal-overlay').classList.add('hidden');
function formatMoney(n) { return new Intl.NumberFormat('es-ES', { style: 'currency', currency: 'EUR' }).format(n); }
function initMocks() { if(!STATE.rfqs.length) STATE.rfqs = [{client:'Test RFQ', amount:0, date:'09/02/2026', status:'Nuevo', agent:'Alejandro', desc:'Simulación'}]; }

// ------------------------------
// FACTURACIÓN (Cobros) v90 PRO
// ------------------------------
let BILLING = {
    rows: [],
    meta: { importedAt: null }
};

function loadBilling(){
    // Fuente única: STATE.billing (se sincroniza con servidor via saveState()).
    if(Array.isArray(STATE.billing) && STATE.billing.length){
        BILLING.rows = STATE.billing;
        return;
    }
    // Legacy: versiones antiguas guardaban pg_billing.
    try{
        const raw = localStorage.getItem('pg_billing');
        if (!raw) return;
        const obj = JSON.parse(raw);
        if (obj?.rows){
            BILLING = obj;
            STATE.billing = BILLING.rows || [];
            saveState();
        }
    }catch(e){ console.warn('billing load', e); }
}

function saveBilling(){
    // Mantener compatibilidad con la UI existente (BILLING) pero persistir en STATE.
    STATE.billing = Array.isArray(BILLING.rows) ? BILLING.rows : [];
    saveState();
    // Legacy: también lo dejamos en pg_billing por si vuelves atrás de versión.
    try{ localStorage.setItem('pg_billing', JSON.stringify(BILLING)); }
    catch(e){ console.warn('billing legacy save', e); }
}

function parseDateES(s){
    if (!s) return null;
    const t = String(s).trim();
    // dd/mm/yyyy
    const m = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
    if (m){
        const dd = parseInt(m[1],10);
        const mm = parseInt(m[2],10)-1;
        let yy = parseInt(m[3],10);
        if (yy<100) yy += 2000;
        const d = new Date(yy,mm,dd);
        return isNaN(d.getTime()) ? null : d;
    }
    // yyyy-mm-dd
    const d2 = new Date(t);
    return isNaN(d2.getTime()) ? null : d2;
}

function eurosToNumber(v){
    if (v==null) return 0;
    const s = String(v).replace(/\s/g,'').replace('€','').replace(/\./g,'').replace(',', '.');
    const n = parseFloat(s);
    return isNaN(n) ? 0 : n;
}

function fmtEUR(n){
    try{ return (n||0).toLocaleString('es-ES',{style:'currency',currency:'EUR'}); }
    catch{ return `${n||0} €`; }
}

function statusFromDue(due){
    if (!due) return 'Pendiente';
    const today = new Date();
    const t0 = new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime();
    const dd = new Date(due.getFullYear(), due.getMonth(), due.getDate()).getTime();
    if (dd < t0) return 'Vencido';
    if (dd === t0) return 'Hoy';
    return 'Pendiente';
}

function parseBillingCSV_OLD(text){
    const lines = text.split(/\r?\n/).filter(l=>l.trim().length);
    if (!lines.length) return [];
    // Detect delimiter
    const head = lines[0];
    const delim = (head.match(/;/g)||[]).length >= (head.match(/,/g)||[]).length ? ';' : ',';
    const split = (line)=>{
        // Simple CSV split with quotes
        const out=[]; let cur=''; let q=false;
        for (let i=0;i<line.length;i++){
            const ch=line[i];
            if (ch==='"') { q=!q; continue; }
            if (!q && ch===delim){ out.push(cur); cur=''; continue; }
            cur+=ch;
        }
        out.push(cur);
        return out.map(s=>s.trim());
    };
    const cols = split(lines[0]).map(c=>c.toLowerCase());
    const idx = {
        factura: cols.findIndex(c=>c.includes('factura')),
        cliente: cols.findIndex(c=>c.includes('cliente')),
        emision: cols.findIndex(c=>c.includes('emisi')),
        venc: cols.findIndex(c=>c.includes('venc')),
        pend: cols.findIndex(c=>c.includes('pend')),
    };
    let currentComercial = 'Sin asignar';
    const rows=[];
    for (let i=1;i<lines.length;i++){
        const cells = split(lines[i]);
        // Línea intercalada con comercial: una celda y resto vacío
        const nonEmpty = cells.filter(c=>c && c.trim()).length;
        if (nonEmpty===1 && (cells.length<3 || (cells[1]||'')==='' )){
            currentComercial = cells.find(c=>c && c.trim())?.trim() || currentComercial;
            continue;
        }
        const factura = cells[idx.factura] ?? '';
        const cliente = cells[idx.cliente] ?? '';
        const emision = parseDateES(cells[idx.emision]);
        const venc = parseDateES(cells[idx.venc]);
        const pendiente = eurosToNumber(cells[idx.pend]);
        if (!factura && !cliente) continue;
        rows.push({
            comercial: currentComercial,
            factura: factura.trim(),
            cliente: cliente.trim(),
            emision: emision ? emision.toISOString() : null,
            vencimiento: venc ? venc.toISOString() : null,
            pendiente,
            reclamada: false,
            nota: ''
        });
    }
    return rows;
}

function initBillingUI_OLD(){
    loadBilling();
    const file = document.getElementById('file-bill');
    const btnClear = document.getElementById('btn-billing-clear');
    const selCom = document.getElementById('billing-comercial');
    const selSt = document.getElementById('billing-estado');
    const q = document.getElementById('billing-q');
    const min = document.getElementById('billing-min');

    if (file && !file.dataset.bound){
        file.dataset.bound='1';
        file.addEventListener('change', async (e)=>{
            const f=e.target.files?.[0];
            if(!f) return;
            const txt = await f.text();
            const rows = parseBillingCSV_OLD(txt);
            BILLING = { rows, meta:{ importedAt: new Date().toISOString(), filename: f.name } };
            saveBilling();
            hydrateBillingFilters_OLD();
            renderBilling_OLD();
            e.target.value='';
        });
    }

    if (btnClear && !btnClear.dataset.bound){
        btnClear.dataset.bound='1';
        btnClear.onclick = ()=>{
            if(!confirm('¿Borrar cartera de cobros cargada?')) return;
            BILLING = { rows: [], meta:{ importedAt: null } };
            saveBilling();
            hydrateBillingFilters_OLD();
            renderBilling_OLD();
        };
    }

    const onFilter = ()=>renderBilling_OLD();
    [selCom, selSt, q, min].forEach(el=>{ if(el && !el.dataset.bound){ el.dataset.bound='1'; el.addEventListener('input',onFilter); el.addEventListener('change',onFilter);} });

    hydrateBillingFilters_OLD();
    renderBilling_OLD();
}

function hydrateBillingFilters_OLD(){
    const selCom = document.getElementById('billing-comercial');
    if (!selCom) return;
    const current = selCom.value || 'Todos';
    const commercials = Array.from(new Set(BILLING.rows.map(r=>r.comercial).filter(Boolean))).sort((a,b)=>a.localeCompare(b,'es'));
    selCom.innerHTML = '<option value="Todos">Todos</option>' + commercials.map(c=>`<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`).join('');
    selCom.value = commercials.includes(current) ? current : 'Todos';
}

function getBillingFiltered_OLD(){
    const selCom = document.getElementById('billing-comercial');
    const selSt = document.getElementById('billing-estado');
    const q = document.getElementById('billing-q');
    const min = document.getElementById('billing-min');
    const com = selCom?.value || 'Todos';
    const st = selSt?.value || 'Todos';
    const query = (q?.value||'').trim().toLowerCase();
    const minVal = parseFloat((min?.value||'0').toString().replace(',','.')) || 0;

    return BILLING.rows.filter(r=>{
        if (com !== 'Todos' && r.comercial !== com) return false;
        const due = r.vencimiento ? new Date(r.vencimiento) : null;
        const status = statusFromDue(due);
        if (st !== 'Todos' && status !== st) return false;
        if (r.pendiente < minVal) return false;
        if (query){
            const hay = `${r.factura} ${r.cliente}`.toLowerCase();
            if (!hay.includes(query)) return false;
        }
        return true;
    });
}

function renderBilling_OLD(){
    const rows = getBillingFiltered_OLD();
    const today = new Date();
    const t0 = new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime();

    let total=0, vencido=0, hoy=0, pendiente=0;
    const byDebtor = new Map();
    const byStatus = {Vencido:0,Hoy:0,Pendiente:0};

    rows.forEach(r=>{
        total += r.pendiente;
        const due = r.vencimiento ? new Date(r.vencimiento) : null;
        const st = statusFromDue(due);
        byStatus[st] = (byStatus[st]||0) + r.pendiente;
        if (st==='Vencido') vencido += r.pendiente;
        if (st==='Hoy') hoy += r.pendiente;
        if (st==='Pendiente') pendiente += r.pendiente;
        const key = (r.cliente||'').trim() || 'Sin cliente';
        byDebtor.set(key, (byDebtor.get(key)||0) + r.pendiente);
    });

    const kTotal = document.getElementById('kpi-bill-total');
    const kOver  = document.getElementById('kpi-bill-overdue');
    const kFlow  = document.getElementById('kpi-bill-flow');
    const kTop   = document.getElementById('kpi-bill-top');
    if (kTotal) kTotal.textContent = fmtEUR(total);
    if (kOver)  kOver.textContent  = fmtEUR(vencido);

    // Cash flow 30D: hoy + próximos 30 días
    const flow30 = rows.reduce((acc,r)=>{
        const due = r.vencimiento ? new Date(r.vencimiento) : null;
        if (!due) return acc;
        const dd = new Date(due.getFullYear(),due.getMonth(),due.getDate()).getTime();
        const days = Math.floor((dd - t0) / (24*3600*1000));
        if (days>=0 && days<=30) return acc + r.pendiente;
        return acc;
    },0);
    if (kFlow) kFlow.textContent = fmtEUR(flow30);

    // Top debtor
    let topName='-'; let topVal=0;
    for (const [name,val] of byDebtor.entries()){
        if (val>topVal){ topVal=val; topName=name; }
    }
    if (kTop) kTop.textContent = topName==='-'?'-':`${topName} · ${fmtEUR(topVal)}`;

    // Table
    const tbody = document.getElementById('tbody-billing');
    if (tbody){
        const sorted = rows.slice().sort((a,b)=>{
            const da = a.vencimiento ? new Date(a.vencimiento).getTime() : 0;
            const db = b.vencimiento ? new Date(b.vencimiento).getTime() : 0;
            return da - db;
        });
        tbody.innerHTML = sorted.map((r,idx)=>{
            const due = r.vencimiento ? new Date(r.vencimiento) : null;
            const st = statusFromDue(due);
            const em = r.emision ? new Date(r.emision) : null;
            const age = due ? Math.floor((t0 - new Date(due.getFullYear(),due.getMonth(),due.getDate()).getTime())/(24*3600*1000)) : '';
            const ageTxt = (age==='') ? '-' : (age<=0 ? `${Math.abs(age)}d` : `${age}d`);
            const stClass = st==='Vencido'?'st-bad':(st==='Hoy'?'st-warn':'st-ok');
            return `
            <tr>
              <td><span class="st ${stClass}">${st}</span></td>
              <td>${escapeHtml(r.factura||'')}</td>
              <td>
                <div class="cell-main">${escapeHtml(r.cliente||'')}</div>
                <div class="cell-sub">${escapeHtml(r.comercial||'')}</div>
              </td>
              <td>${em ? em.toLocaleDateString('es-ES') : '-'}</td>
              <td>${due ? due.toLocaleDateString('es-ES') : '-'}</td>
              <td>${ageTxt}</td>
              <td class="t-num">${fmtEUR(r.pendiente)}</td>
              <td>
                <button class="mini-btn" data-bill-act="recl" data-idx="${idx}">${r.reclamada?'Reclamada':'Reclamar'}</button>
              </td>
            </tr>`;
        }).join('');
    }

    // Bind action buttons (event delegation)
    const table = document.getElementById('table-billing');
    if (table && !table.dataset.bound){
        table.dataset.bound='1';
        table.addEventListener('click',(e)=>{
            const btn = e.target.closest('[data-bill-act]');
            if (!btn) return;
            const idx = parseInt(btn.getAttribute('data-idx'),10);
            const act = btn.getAttribute('data-bill-act');
            const filtered = getBillingFiltered_OLD();
            const r = filtered[idx];
            if (!r) return;
            // Toggle reclamada
            if (act==='recl') r.reclamada = !r.reclamada;
            saveBilling();
            renderBilling_OLD();
        });
    }
}
function fullReset() { if(confirm("¿Borrar?")) { localStorage.clear(); location.reload(); } }

function renderMiniCharts() { 
    ['nac','exp','hot','car'].forEach(k => { 
        const ctx = document.getElementById(`c-mini-${k}`);
        if(ctx) {
            // Mini barras: Objetivo (azul claro) vs Real (rosa suave) por cuatrimestre
            const data = (STATE.objectives && STATE.objectives.data && STATE.objectives.data[k]) ? STATE.objectives.data[k] : null;
            const obj = (data && data.objCuatr) ? data.objCuatr : {c1:0,c2:0,c3:0};
            const real = (data && data.realCuatr) ? data.realCuatr : ((STATE.objectives.breakdown && STATE.objectives.breakdown[k]) ? STATE.objectives.breakdown[k] : {c1:0,c2:0,c3:0});
            if(charts[k]) charts[k].destroy();
            charts[k] = safeChart(ctx, {
                type:'bar', 
                data:{
                    labels:['C1 (Ene-Abr)', 'C2 (May-Ago)', 'C3 (Sep-Dic)'], 
                    datasets:[
                        { label:'Objetivo', data:[obj.c1, obj.c2, obj.c3], backgroundColor:'#93c5fd' },
                        { label:'Real', data:[real.c1, real.c2, real.c3], backgroundColor:'#fda4af' }
                    ]
                }, 
                options:{
                    plugins:{legend:false, tooltip:{enabled:true}},
                    scales:{x:{display:false}, y:{display:false}},
                    maintainAspectRatio:false
                }
            });
        }
    }); 
}

// v56 · Gráficas Objetivos (solo Nacional)
function renderObjectiveCharts(){
    try{
        const monthsLbl = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];

        const getSeries = (key) => {
            const data = (STATE.objectives && STATE.objectives.data && STATE.objectives.data[key]) ? STATE.objectives.data[key] : null;

            const realMonthly = data && data.realMonthly ? data.realMonthly : ((STATE.objectives.monthly && STATE.objectives.monthly[key]) ? STATE.objectives.monthly[key] : new Array(12).fill(0));
            const objMonthly  = data && data.objMonthly  ? data.objMonthly  : new Array(12).fill(0);

            const realCuatr = data && data.realCuatr ? data.realCuatr : ((STATE.objectives.breakdown && STATE.objectives.breakdown[key]) ? STATE.objectives.breakdown[key] : {c1:0,c2:0,c3:0});
            const objCuatr  = data && data.objCuatr  ? data.objCuatr  : {c1:0,c2:0,c3:0};

            return { realMonthly, objMonthly, realCuatr, objCuatr };
        };

        const renderPair = (key, mesId, cuatrId, chartKeyMes, chartKeyCuatr) => {
            const mesCanvas = document.getElementById(mesId);
            const cuatrCanvas = document.getElementById(cuatrId);
            if(!mesCanvas || !cuatrCanvas) return;

            const s = getSeries(key);

            charts[chartKeyMes] && charts[chartKeyMes].destroy && charts[chartKeyMes].destroy();
            charts[chartKeyCuatr] && charts[chartKeyCuatr].destroy && charts[chartKeyCuatr].destroy();

            charts[chartKeyMes] = safeChart(mesCanvas, {
                type: 'bar',
                data: {
                    labels: monthsLbl,
                    datasets: [
                        { label: 'Objetivo (€)', data: s.objMonthly },
                        { label: 'Real (€)', data: s.realMonthly }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: true } },
                    scales: { y: { ticks: { callback: (v) => Number(v).toLocaleString('es-ES') } } }
                }
            });

            charts[chartKeyCuatr] = safeChart(cuatrCanvas, {
                type: 'bar',
                data: {
                    labels: ['1º Cuatr.','2º Cuatr.','3º Cuatr.'],
                    datasets: [
                        { label: 'Objetivo (€)', data: [s.objCuatr.c1||0, s.objCuatr.c2||0, s.objCuatr.c3||0] },
                        { label: 'Real (€)', data: [s.realCuatr.c1||0, s.realCuatr.c2||0, s.realCuatr.c3||0] }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: true } },
                    scales: { y: { ticks: { callback: (v) => Number(v).toLocaleString('es-ES') } } }
                }
            });
        };

        renderPair('nac','c-nac-mes','c-nac-cuatr','objNacMes','objNacCuatr');
        renderPair('exp','c-exp-mes','c-exp-cuatr','objExpMes','objExpCuatr');
        renderPair('hot','c-hot-mes','c-hot-cuatr','objHotMes','objHotCuatr');
        renderPair('car','c-car-mes','c-car-cuatr','objCarMes','objCarCuatr');
    }catch(err){
        console.warn('renderObjectiveCharts error', err);
    }
}

// v65 · Panel Estratégico (Objetivos): resumen 4 series + global
function updateStrategicPanel(){
    const d = (STATE.objectives && STATE.objectives.data) ? STATE.objectives.data : {};
    const series = ['nac','exp','hot','car'];
    let objG = 0, realG = 0;

    const mapId = {nac:'sp-obj-nac', exp:'sp-obj-exp', hot:'sp-obj-hot', car:'sp-obj-car'};
    series.forEach(k=>{
        const item = d[k] || {};
        const obj = Number(item.objTotal||0);
        const real = Number(item.realTotal||0);
        objG += obj; realG += real;
        const el = document.getElementById(mapId[k]);
        if(el) el.textContent = formatMoney(obj);
    });

    const gap = realG - objG;
    const elObj = document.getElementById('sp-obj-global');
    const elReal = document.getElementById('sp-real-global');
    const elGap = document.getElementById('sp-gap-global');
    if(elObj) elObj.textContent = formatMoney(objG);
    if(elReal) elReal.textContent = formatMoney(realG);
    if(elGap){
        elGap.textContent = formatMoney(gap);
        elGap.classList.toggle('text-danger', gap < 0);
    }
}


function initGlobalSearchAndMic(){
    // Buscador: soporta distintos IDs típicos
    const input = document.getElementById('buscador') || document.getElementById('search') || document.getElementById('global-search') || document.getElementById('search-input') || document.getElementById('q');
    if(input && !input.__cchBound){
        input.__cchBound = true;
        const runSearch = debounce(() => {
            const q = String(input.value||'').toLowerCase().trim();
            window.__CCH_SEARCH_Q = q;
            updateUI();
            const view = location.hash.replace('#','') || '';
            if(view.includes('rfq')||view.includes('offer')||view.includes('order')){
                requestAnimationFrame(() => renderTable(view));
            }
        }, 140);
        input.addEventListener('input', runSearch);
    }

    // Micrófono (si existe). Nunca debe romper la app si el navegador no lo soporta.
    const micBtn = document.getElementById('mic-btn') || document.getElementById('mic') || document.querySelector('.mic-btn') || document.querySelector('.mic-icon') || document.querySelector('[data-mic]');
    if(micBtn && !micBtn.__cchBound){
        micBtn.__cchBound = true;
        micBtn.addEventListener('click', async () => {
                const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
            if(!SpeechRecognition){ alert('Micrófono no disponible en este navegador.'); return; }
            try{
                const rec = new SpeechRecognition();
                rec.lang = 'es-ES';
                rec.interimResults = false;
                rec.maxAlternatives = 1;
                rec.onresult = (ev) => {
                    const text = ev.results && ev.results[0] && ev.results[0][0] ? ev.results[0][0].transcript : '';
                    if(input){ input.value = text; input.dispatchEvent(new Event('input')); }
                };
                rec.onerror = () => {};
                rec.start();
            }catch(err){
                alert('No se pudo iniciar el micrófono (permiso o compatibilidad).');
            }
        });
    }
}


function escapeHtml(str){ return String(str||'').replace(/[&<>"']/g, s=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[s])); }
function escapeAttr(str){ return escapeHtml(str).replace(/`/g,'&#96;'); }

/* =========================
   INFORMES (v130_final)
   - Sube informes semanales (xls/xlsx/csv)
   - Emite por semana / mes / serie / agente
   - Mini-gráficas por agente
   ========================= */

const REPORT_AGENTS = ['Juana','Sergio','Alejandro','Export','Jorge','Pilar'];
const REPORTS_STORAGE_KEY = 'pg_reports_v1';
let __infCharts = { weekly: null, monthly: null };

function initInformes(){
    const btn = document.getElementById('inf-upload-btn');
    const input = document.getElementById('inf-file');
    if(btn && input && !btn.__cchBound){
        btn.__cchBound = true;
        btn.addEventListener('click', ()=> input.click());
        input.addEventListener('change', async (e)=>{
            const files = Array.from(e.target.files || []);
            if(!files.length) return;
            try{
                await ingestReportFiles(files);
            }catch(err){
                console.error(err);
                toastInf('No se pudo leer el informe. Revisa formato o prueba exportar a CSV.');
            }
            input.value = '';
        });
    }

    const periodSel = document.getElementById('inf-period');
    const weekWrap = document.getElementById('inf-week-wrap');
    const monthWrap = document.getElementById('inf-month-wrap');
    if(periodSel && !periodSel.__cchBound){
        periodSel.__cchBound = true;
        periodSel.addEventListener('change', ()=>{
            const v = periodSel.value;
            if(v === 'month'){
                weekWrap?.classList.add('hidden');
                monthWrap?.classList.remove('hidden');
            }else{
                monthWrap?.classList.add('hidden');
                weekWrap?.classList.remove('hidden');
            }
            refreshInformes();
        });
    }

    ['inf-agent','inf-serie','inf-week','inf-month','inf-search'].forEach(id=>{
        const el = document.getElementById(id);
        if(el && !el.__cchBound){
            el.__cchBound = true;
            el.addEventListener('input', refreshInformes);
            el.addEventListener('change', refreshInformes);
        }
    });

    const btnDl = document.getElementById('inf-download');
    if(btnDl && !btnDl.__cchBound){
        btnDl.__cchBound = true;
        btnDl.addEventListener('click', ()=>downloadInformeActual());
    }

    const btnClear = document.getElementById('inf-clear');
    if(btnClear && !btnClear.__cchBound){
        btnClear.__cchBound = true;
        btnClear.addEventListener('click', ()=>{
            if(!confirm('¿Resetear informes (se borran del navegador)?')) return;
            localStorage.removeItem(REPORTS_STORAGE_KEY);
            refreshInformes(true);
        });
    }

    // Set defaults (última semana/mes si existen)
    refreshInformes(true);
}

function toastInf(msg){
    // Reutilizamos el toast existente (producción) si está
    if(typeof showGanttToast === 'function'){ showGanttToast(String(msg||'')); return; }
    alert(msg);
}

function loadReportsStore(){
    try{
        const raw = localStorage.getItem(REPORTS_STORAGE_KEY);
        const arr = raw ? JSON.parse(raw) : [];
        return Array.isArray(arr) ? arr : [];
    }catch(e){ return []; }
}
function saveReportsStore(arr){
    localStorage.setItem(REPORTS_STORAGE_KEY, JSON.stringify(arr||[]));
}

async function ingestReportFiles(files){
    const status = document.getElementById('inf-status');
    const existing = loadReportsStore();
    let added = 0;

    for(const f of files){
        const name = (f.name||'').toLowerCase();
        const serie = name.includes('export') ? 'EXPORT' : (name.includes('nacional') ? 'NACIONAL' : '');
        const reportDate = inferDateFromFilename(f.name) || new Date();
        const weekKey = isoWeekKey(reportDate);
        const monthKey = monthKeyFromDate(reportDate);

        let rows = [];
        if(name.endsWith('.csv')){
            const text = await f.text();
            rows = csvToRows(text);
        }else{
            // XLS/XLSX con SheetJS
            const buf = await f.arrayBuffer();
            if(!window.XLSX){
                toastInf('No está cargado el lector XLSX. Recarga la página y prueba de nuevo.');
                continue;
            }
            const wb = XLSX.read(buf, { type:'array' });
            const sheetName = wb.SheetNames[0];
            const ws = wb.Sheets[sheetName];
            rows = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true, defval: '' });
        }

        const entries = extractReportEntries(rows, { serie, reportDate, weekKey, monthKey, sourceFile: f.name });

        // Deduplicación simple: por sourceFile+weekKey+agent+amount+cliente+pedido
        for(const it of entries){
            const sig = `${it.sourceFile}||${it.weekKey}||${it.agent}||${it.amount}||${it.client||''}||${it.order||''}`;
            if(existing.some(x => (x.__sig || '') === sig)) continue;
            it.__sig = sig;
            existing.push(it);
            added++;
        }
    }

    saveReportsStore(existing);
    if(status){
        status.textContent = `Cargados ${added} registros nuevos. Total en informes: ${existing.length}.`;
    }
    refreshInformes(true);
}

function inferDateFromFilename(fn){
    // Busca ddmmyy o yymmdd en el nombre
    const s = String(fn||'');
    const m = s.match(/(\d{2})(\d{2})(\d{2})/); // 230126
    if(!m) return null;
    const dd = parseInt(m[1],10), mm = parseInt(m[2],10), yy = parseInt(m[3],10);
    if(!(dd>=1 && dd<=31 && mm>=1 && mm<=12)) return null;
    const year = yy < 70 ? (2000+yy) : (1900+yy);
    try{ return new Date(year, mm-1, dd); }catch(e){ return null; }
}

function isoWeekKey(d){
    const dt = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
    const dayNum = dt.getUTCDay() || 7;
    dt.setUTCDate(dt.getUTCDate() + 4 - dayNum);
    const yearStart = new Date(Date.UTC(dt.getUTCFullYear(),0,1));
    const weekNo = Math.ceil((((dt - yearStart) / 86400000) + 1) / 7);
    const y = dt.getUTCFullYear();
    const w = String(weekNo).padStart(2,'0');
    return `${y}-W${w}`;
}

function monthKeyFromDate(d){
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
}

function csvToRows(text){
    const t = String(text||'');
    const delim = t.includes(';') ? ';' : ',';
    const lines = t.split(/\r?\n/).filter(l=>l.trim().length);
    return lines.map(line => line.split(delim));
}

function parseAmount(v){
    if(typeof v === 'number') return isFinite(v) ? v : 0;
    let s = String(v||'').trim();
    if(!s) return 0;
    s = s.replace(/€/g,'').replace(/\s/g,'');
    // miles con . y decimales con ,
    if(/\d+\.\d{3},\d{2}/.test(s)) s = s.replace(/\./g,'').replace(',', '.');
    else if(/\d+,\d{2}/.test(s) && !/\d+\.\d{2}/.test(s)) s = s.replace(',', '.');
    s = s.replace(/[^0-9.\-]/g,'');
    const n = parseFloat(s);
    return isFinite(n) ? n : 0;
}

function normalizeText(v){
    return String(v||'').replace(/\s+/g,' ').trim();
}

function extractReportEntries(rows, meta){
    const entries = [];
    const agentsLC = REPORT_AGENTS.map(a=>a.toLowerCase());

    for(const r of rows){
        if(!r || !r.length) continue;
        const joined = r.map(x=>String(x||'')).join(' | ').toLowerCase();
        let agent = '';
        // Caso especial: 'Alejandro Exportación' debe contar como agente 'Export'
        if((joined.includes('export') || joined.includes('exportación') || joined.includes('exportacion')) && joined.includes('alejandro')){
            agent = 'Export';
        }else{
            for(let i=0;i<agentsLC.length;i++){
                if(joined.includes(agentsLC[i])){ agent = REPORT_AGENTS[i]; break; }
            }
        }
        if(!agent) continue;

        // heurística: importe = último valor numérico relevante de la fila
        let amount = 0;
        for(let i=r.length-1;i>=0;i--){
            const a = parseAmount(r[i]);
            if(a && Math.abs(a) > 0){ amount = a; break; }
        }
        if(!amount) continue;

        // Intenta capturar campos típicos: pedido, cliente, descripción
        const order = findLikelyOrder(r);
        const client = findLikelyClient(r);
        const desc = findLikelyDesc(r);

        entries.push({
            period: meta.weekKey,
            weekKey: meta.weekKey,
            monthKey: meta.monthKey,
            serie: meta.serie || '',
            agent,
            client,
            order,
            desc,
            amount,
            reportDate: meta.reportDate ? meta.reportDate.toISOString().slice(0,10) : '',
            sourceFile: meta.sourceFile || ''
        });
    }

    // Si no hemos encontrado filas por nombre, intenta modo "tabla" con cabecera
    if(entries.length === 0){
        const headerIdx = rows.findIndex(r => (r||[]).some(c => /comercial|vendedor|agente/i.test(String(c||''))));
        if(headerIdx >= 0){
            const hdr = rows[headerIdx].map(h => String(h||'').toLowerCase());
            const idxAgent = hdr.findIndex(h => /comercial|vendedor|agente/.test(h));
            const idxAmt = hdr.findIndex(h => /importe|total|venta/.test(h));
            const idxClient = hdr.findIndex(h => /cliente|raz(ó|o)n social/.test(h));
            const idxOrder = hdr.findIndex(h => /pedido|n\.?\s*pedido|numero/.test(h));
            const idxDesc = hdr.findIndex(h => /descrip|art(í|i)culo|concepto/.test(h));

            for(let i=headerIdx+1;i<rows.length;i++){
                const r = rows[i] || [];
                const a = normalizeText(r[idxAgent]);
                const agent = REPORT_AGENTS.find(x => x.toLowerCase() === a.toLowerCase()) || (a ? a : '');
                if(!agent) continue;
                const amount = parseAmount(r[idxAmt]);
                if(!amount) continue;
                entries.push({
                    period: meta.weekKey,
                    weekKey: meta.weekKey,
                    monthKey: meta.monthKey,
                    serie: meta.serie || '',
                    agent,
                    client: normalizeText(r[idxClient]),
                    order: normalizeText(r[idxOrder]),
                    desc: normalizeText(r[idxDesc]),
                    amount,
                    reportDate: meta.reportDate ? meta.reportDate.toISOString().slice(0,10) : '',
                    sourceFile: meta.sourceFile || ''
                });
            }
        }
    }

    return entries;
}

function findLikelyOrder(row){
    for(const v of row){
        const s = normalizeText(v);
        if(/^\d{6,}$/.test(s)) return s;
        if(/^\d{3,}\/\d{2,}$/.test(s)) return s;
    }
    return '';
}

function findLikelyClient(row){
    // cliente suele ser el texto más largo que no sea "total" etc.
    let best = '';
    for(const v of row){
        const s = normalizeText(v);
        if(!s) continue;
        if(/total|subtotal|importe|venta|comercial|agente|vendedor/i.test(s)) continue;
        if(s.length >= 8 && s.length > best.length && !/\d{4,}/.test(s)) best = s;
    }
    return best;
}

function findLikelyDesc(row){
    // descripción: segundo texto largo
    const texts = row.map(v=>normalizeText(v)).filter(s=>s && s.length>10 && !/total|subtotal|importe/i.test(s));
    if(texts.length>=2) return texts[1];
    return texts[0] || '';
}

function refreshInformes(resetStatus){
    const data = loadReportsStore();
    const status = document.getElementById('inf-status');
    if(resetStatus && status){
        status.textContent = data.length ? `Informes en memoria: ${data.length} registros.` : 'Sin informes cargados todavía.';
    }

    // Defaults: última semana/mes existentes
    const weeks = Array.from(new Set(data.map(x=>x.weekKey).filter(Boolean))).sort();
    const months = Array.from(new Set(data.map(x=>x.monthKey).filter(Boolean))).sort();
    const lastWeek = weeks[weeks.length-1] || '';
    const lastMonth = months[months.length-1] || '';

    const weekInput = document.getElementById('inf-week');
    const monthInput = document.getElementById('inf-month');
    if(weekInput && !weekInput.value && lastWeek){ weekInput.value = lastWeek; }
    if(monthInput && !monthInput.value && lastMonth){ monthInput.value = lastMonth; }

    // Render charts (weekly/monthly)
    renderInformesCharts(data, lastWeek, lastMonth);
    // Render table based on current filters
    renderInformesTable(data, lastWeek, lastMonth);
}

function renderInformesCharts(data, lastWeek, lastMonth){
    const weekSel = document.getElementById('inf-week')?.value || lastWeek;
    const monthSel = document.getElementById('inf-month')?.value || lastMonth;

    const weekly = aggregateByAgent(data.filter(x=>x.weekKey===weekSel));
    const monthly = aggregateByAgent(data.filter(x=>x.monthKey===monthSel));

    const labels = REPORT_AGENTS.slice();
    const wVals = labels.map(a => weekly[a] || 0);
    const mVals = labels.map(a => monthly[a] || 0);

    const wCtx = document.getElementById('inf-weekly-chart')?.getContext('2d');
    const mCtx = document.getElementById('inf-monthly-chart')?.getContext('2d');

    if(wCtx){
        if(__infCharts.weekly) __infCharts.weekly.destroy();
        __infCharts.weekly = new Chart(wCtx, {
            type:'bar',
            data:{ labels, datasets:[{ label:`Semana ${weekSel || '-'}`, data:wVals }]},
            options:{ responsive:true, maintainAspectRatio:false, plugins:{ legend:{ display:false } }, scales:{ y:{ beginAtZero:true } } }
        });
    }

    if(mCtx){
        if(__infCharts.monthly) __infCharts.monthly.destroy();
        __infCharts.monthly = new Chart(mCtx, {
            type:'bar',
            data:{ labels, datasets:[{ label:`Mes ${monthSel || '-'}`, data:mVals }]},
            options:{ responsive:true, maintainAspectRatio:false, plugins:{ legend:{ display:false } }, scales:{ y:{ beginAtZero:true } } }
        });
    }
}

function aggregateByAgent(items){
    const out = {};
    for(const it of items){
        const a = it.agent || 'Otros';
        out[a] = (out[a] || 0) + (Number(it.amount)||0);
    }
    return out;
}

function renderInformesTable(data, lastWeek, lastMonth){
    const agentSel = document.getElementById('inf-agent')?.value || 'all';
    const serieSel = document.getElementById('inf-serie')?.value || 'all';
    const periodMode = document.getElementById('inf-period')?.value || 'week';
    const weekSel = document.getElementById('inf-week')?.value || lastWeek;
    const monthSel = document.getElementById('inf-month')?.value || lastMonth;
    const q = (document.getElementById('inf-search')?.value || '').toLowerCase().trim();

    const items = data.filter(it => {
        if(agentSel !== 'all' && it.agent !== agentSel) return false;
        if(serieSel !== 'all' && String(it.serie||'').toUpperCase() !== String(serieSel||'').toUpperCase()) return false;
        if(periodMode === 'month'){
            if(monthSel && it.monthKey !== monthSel) return false;
        }else{
            if(weekSel && it.weekKey !== weekSel) return false;
        }
        if(q){
            const blob = `${it.client||''} ${it.order||''} ${it.desc||''} ${it.sourceFile||''}`.toLowerCase();
            if(!blob.includes(q)) return false;
        }
        return true;
    });

    const sub = document.getElementById('inf-detail-sub');
    const total = items.reduce((s,x)=>s+(Number(x.amount)||0),0);
    if(sub){
        const perLabel = (periodMode === 'month') ? `Mes ${monthSel||'-'}` : `Semana ${weekSel||'-'}`;
        sub.textContent = `${perLabel} · ${items.length} filas · Total: ${fmtEUR(total)}`;
    }

    const tbody = document.getElementById('inf-tbody');
    if(!tbody) return;
    tbody.innerHTML = items.slice(0, 500).map(it => {
        const per = (periodMode === 'month') ? (it.monthKey||'') : (it.weekKey||'');
        return `<tr>
          <td>${escapeHtml(per)}</td>
          <td>${escapeHtml(it.serie||'-')}</td>
          <td><strong>${escapeHtml(it.agent||'-')}</strong></td>
          <td>${escapeHtml(it.client||'')}</td>
          <td>${escapeHtml(it.order||'')}</td>
          <td>${escapeHtml(it.desc||'')}</td>
          <td style="text-align:right">${escapeHtml(fmtEUR(it.amount||0))}</td>
        </tr>`;
    }).join('');
}

function downloadInformeActual(){
    const data = loadReportsStore();
    const agentSel = document.getElementById('inf-agent')?.value || 'all';
    const serieSel = document.getElementById('inf-serie')?.value || 'all';
    const periodMode = document.getElementById('inf-period')?.value || 'week';
    const weekSel = document.getElementById('inf-week')?.value || '';
    const monthSel = document.getElementById('inf-month')?.value || '';
    const q = (document.getElementById('inf-search')?.value || '').toLowerCase().trim();

    const items = data.filter(it => {
        if(agentSel !== 'all' && it.agent !== agentSel) return false;
        if(serieSel !== 'all' && String(it.serie||'').toUpperCase() !== String(serieSel||'').toUpperCase()) return false;
        if(periodMode === 'month'){
            if(monthSel && it.monthKey !== monthSel) return false;
        }else{
            if(weekSel && it.weekKey !== weekSel) return false;
        }
        if(q){
            const blob = `${it.client||''} ${it.order||''} ${it.desc||''} ${it.sourceFile||''}`.toLowerCase();
            if(!blob.includes(q)) return false;
        }
        return true;
    });

    if(!items.length){ toastInf('No hay datos con este filtro.'); return; }

    const header = ['periodo','serie','agente','cliente','pedido','descripcion','importe','fecha_informe','archivo'];
    const lines = [header.join(';')].concat(items.map(it => [
        (periodMode==='month' ? (it.monthKey||'') : (it.weekKey||'')),
        (it.serie||''),
        (it.agent||''),
        (it.client||''),
        (it.order||''),
        (it.desc||''),
        String(it.amount||0).replace('.',','),
        (it.reportDate||''),
        (it.sourceFile||'')
    ].map(x=>`"${String(x).replace(/"/g,'""')}"`).join(';')));

    const csv = lines.join('\n');
    const blob = new Blob([csv], { type:'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    const per = (periodMode==='month' ? (monthSel||'mes') : (weekSel||'semana'));
    const fname = `informe_${per}_${agentSel==='all'?'todos':agentSel}_${serieSel==='all'?'todas':serieSel}.csv`;
    a.href = URL.createObjectURL(blob);
    a.download = fname;
    document.body.appendChild(a);
    a.click();
    setTimeout(()=>{ URL.revokeObjectURL(a.href); a.remove(); }, 500);
}
