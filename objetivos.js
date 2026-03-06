
let CHARTS = {};

function showTab(id, ev){
  document.querySelectorAll('.tab-content').forEach(x=>x.classList.remove('active'));
  document.getElementById(id)?.classList.add('active');
  document.querySelectorAll('.ts-link').forEach(x=>x.classList.remove('active'));
  if(ev && ev.currentTarget) ev.currentTarget.classList.add('active');
}

function fmtMoney(v){
  const n = Number(v||0);
  return n.toLocaleString('es-ES', {style:'currency', currency:'EUR', maximumFractionDigits:0});
}
function fmtNum(v){
  const n = Number(v||0);
  return n.toLocaleString('es-ES');
}

function getERPDB(){
  // Preferimos clave estable del panel
  const direct = localStorage.getItem('cch_v55_db');
  if(direct) return safeJSON(direct);

  // fallback: buscar última cch_vXX_db
  let best = null, bestV = -1;
  for(let i=0;i<localStorage.length;i++){
    const k = localStorage.key(i);
    const m = /^cch_v(\d+)_db$/.exec(k||'');
    if(m){
      const v = parseInt(m[1],10);
      if(v>bestV){ bestV=v; best=k; }
    }
  }
  if(best){
    const s = localStorage.getItem(best);
    if(s) return safeJSON(s);
  }

  // fallback legacy
  const legacy = localStorage.getItem('cch_v50_db');
  if(legacy) return safeJSON(legacy);

  return null;
}

function safeJSON(s){ try{ return JSON.parse(s); }catch(e){ return null; } }

function currentSerie(){ return document.getElementById('sel-serie')?.value || 'nac'; }

function serieLabel(k){
  return ({nac:'Nacional', exp:'Export', hot:'Hoteles', car:'Caramelos'})[k] || k;
}

function computeForecast(obj, real){
  // Forecast sencillo: run-rate (si hay real, proyectar al objetivo como referencia)
  // Aquí lo dejamos conservador: forecast = max(real, real*(12/mesesConReal))
  return {forecast: real};
}

function loadSerieInputs(serie){
  const key = `cch_pro_plan_${serie}`;
  const data = safeJSON(localStorage.getItem(key) || '{}') || {};
  const set = (id, val) => { const el=document.getElementById(id); if(el) el.value = (val ?? ''); };
  set('in-newbiz-t', data.newbiz_t ?? 0);
  set('in-newbiz-a', data.newbiz_a ?? 0);
  set('in-newcli-t', data.newcli_t ?? 0);
  set('in-newcli-a', data.newcli_a ?? 0);
  set('in-dorm-t', data.dorm_t ?? 0);
  set('in-dorm-a', data.dorm_a ?? 0);
  set('in-fair-t', data.fair_t ?? 0);
  set('in-act-t', data.act_t ?? 0);
  set('in-impact-eur', data.impact_eur ?? 0);
  const it = document.getElementById('in-impact-time'); if(it) it.value = data.impact_time ?? '';
  const notes = document.getElementById('in-notes'); if(notes) notes.value = data.notes ?? '';

  drawInputsChart();
}

function saveSerieInputs(){
  const serie = currentSerie();
  const key = `cch_pro_plan_${serie}`;
  const getN = id => Number(document.getElementById(id)?.value || 0);
  const getS = id => String(document.getElementById(id)?.value || '');
  const payload = {
    newbiz_t: getN('in-newbiz-t'),
    newbiz_a: getN('in-newbiz-a'),
    newcli_t: getN('in-newcli-t'),
    newcli_a: getN('in-newcli-a'),
    dorm_t: getN('in-dorm-t'),
    dorm_a: getN('in-dorm-a'),
    fair_t: getN('in-fair-t'),
    act_t: getN('in-act-t'),
    impact_eur: getN('in-impact-eur'),
    impact_time: getS('in-impact-time'),
    notes: getS('in-notes')
  };
  localStorage.setItem(key, JSON.stringify(payload));
  drawInputsChart();
}

function loadSMART(serie){
  const key = `cch_pro_smart_${serie}`;
  const d = safeJSON(localStorage.getItem(key)||'{}') || {};
  const map = {
    'smart-s': d.s || '',
    'smart-m': d.m || '',
    'smart-a': d.a || '',
    'smart-r': d.r || '',
    'smart-t': d.t || '',
    'smart-formula': d.formula || '',
    'smart-milestones': d.milestones || ''
  };
  Object.entries(map).forEach(([id,val])=>{
    const el=document.getElementById(id);
    if(el) el.value = val;
  });
}

function saveSMART(){
  const serie = currentSerie();
  const get = id => String(document.getElementById(id)?.value || '');
  const payload = {
    s:get('smart-s'), m:get('smart-m'), a:get('smart-a'), r:get('smart-r'), t:get('smart-t'),
    formula:get('smart-formula'), milestones:get('smart-milestones')
  };
  localStorage.setItem(`cch_pro_smart_${serie}`, JSON.stringify(payload));
}

function loadDAFO(serie){
  const key = `cch_pro_dafo_${serie}`;
  const d = safeJSON(localStorage.getItem(key)||'{}') || {};
  const set = (id, arr) => {
    const ul=document.getElementById(id);
    if(!ul) return;
    ul.innerHTML = (Array.isArray(arr)?arr:[]).map(x=>`<li>${escapeHtml(x)}</li>`).join('');
    if(!ul.innerHTML) ul.innerHTML = '<li></li>';
  };
  set('dafo-f', d.f);
  set('dafo-o', d.o);
  set('dafo-d', d.d);
  set('dafo-a', d.a);
}

function saveDAFO(){
  const serie = currentSerie();
  const read = (id)=>{
    const ul=document.getElementById(id);
    if(!ul) return [];
    return Array.from(ul.querySelectorAll('li')).map(li=>li.textContent.trim()).filter(Boolean);
  };
  const payload = { f:read('dafo-f'), o:read('dafo-o'), d:read('dafo-d'), a:read('dafo-a') };
  localStorage.setItem(`cch_pro_dafo_${serie}`, JSON.stringify(payload));
}

function escapeHtml(s){
  return String(s||'').replace(/[&<>"']/g, m=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[m]));
}

function syncWithERP(){
  const db = getERPDB();
  const pill = document.getElementById('live-pill');
  if(!db){
    if(pill){ pill.textContent = 'En vivo: sin datos del panel (carga CSV en ERP)'; pill.classList.remove('ok'); }
    setKPIs(0,0,0);
    drawObjectiveCharts(null);
    return;
  }

  const serie = currentSerie();
  const o = db.objectives?.data?.[serie];
  if(!o){
    if(pill){ pill.textContent = `En vivo: sin objetivos para ${serieLabel(serie)} (sube CSV Objetivos)`; pill.classList.remove('ok'); }
    setKPIs(0,0,0);
    drawObjectiveCharts(null);
    return;
  }

  if(pill){
    pill.textContent = `En vivo: leyendo panel (serie ${serieLabel(serie)})`;
    pill.classList.add('ok');
  }

  const obj = Number(o.objTotal||0);
  const real = Number(o.realTotal||0);
  const fore = computeForecast(obj, real).forecast;

  setKPIs(obj, real, fore);
  drawObjectiveCharts(o);

  // inputs / smart / dafo por serie
  loadSerieInputs(serie);
  loadSMART(serie);
  loadDAFO(serie);
}

function setKPIs(obj, real, fore){
  const gap = real - obj;
  const elObj = document.getElementById('kpi-obj');
  const elReal = document.getElementById('kpi-real');
  const elFore = document.getElementById('kpi-fore');
  const elGap = document.getElementById('kpi-gap');
  if(elObj) elObj.textContent = fmtMoney(obj);
  if(elReal) elReal.textContent = fmtMoney(real);
  if(elFore) elFore.textContent = fmtMoney(fore);
  if(elGap){
    elGap.textContent = fmtMoney(gap);
    elGap.classList.toggle('text-danger', gap < 0);
  }
}

function ensureChart(id, cfg){
  if(CHARTS[id]) CHARTS[id].destroy();
  const ctx = document.getElementById(id);
  if(!ctx) return;
  CHARTS[id] = new Chart(ctx, cfg);
}

function drawObjectiveCharts(o){
  const months = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
  const objM = o ? (o.objMonthly||Array(12).fill(0)) : Array(12).fill(0);
  const realM = o ? (o.realMonthly||Array(12).fill(0)) : Array(12).fill(0);

  ensureChart('c-mes',{
    type:'bar',
    data:{
      labels:months,
      datasets:[
        {label:'Objetivo (€)', data:objM, backgroundColor:'rgba(96,165,250,.60)'},
        {label:'Real (€)', data:realM, backgroundColor:'rgba(248,113,113,.55)'}
      ]
    },
    options:{
      responsive:true,
      plugins:{legend:{labels:{color:'#cbd5e1', font:{weight:'bold'}}}},
      scales:{
        x:{ticks:{color:'#cbd5e1', font:{weight:'bold'}}, grid:{color:'rgba(148,163,184,.08)'}},
        y:{ticks:{color:'#cbd5e1', font:{weight:'bold'}}, grid:{color:'rgba(148,163,184,.08)'}}
      }
    }
  });

  const cuLabels = ['1º Cuatr.','2º Cuatr.','3º Cuatr.'];
  const objC = o ? [o.objCuatr?.c1||0, o.objCuatr?.c2||0, o.objCuatr?.c3||0] : [0,0,0];
  const realC= o ? [o.realCuatr?.c1||0, o.realCuatr?.c2||0, o.realCuatr?.c3||0] : [0,0,0];

  ensureChart('c-cuatr',{
    type:'bar',
    data:{
      labels:cuLabels,
      datasets:[
        {label:'Objetivo (€)', data:objC, backgroundColor:'rgba(96,165,250,.60)'},
        {label:'Real (€)', data:realC, backgroundColor:'rgba(248,113,113,.55)'}
      ]
    },
    options:{
      responsive:true,
      plugins:{legend:{labels:{color:'#cbd5e1', font:{weight:'bold'}}}},
      scales:{
        x:{ticks:{color:'#cbd5e1', font:{weight:'bold'}}, grid:{color:'rgba(148,163,184,.08)'}},
        y:{ticks:{color:'#cbd5e1', font:{weight:'bold'}}, grid:{color:'rgba(148,163,184,.08)'}}
      }
    }
  });

  // Hist / Mix (simple, por serie)
  const real2025 = Number(o?.realTotal||0);
  const hist2024 = Number(localStorage.getItem(`cch_pro_hist_${currentSerie()}_2024`)||0);
  const forecast2026 = Number(o?.realTotal||0); // placeholder: se actualiza con inputs/forecast más adelante

  ensureChart('c-hist',{
    type:'line',
    data:{labels:['2024','2025','2026 (Proyectado)'], datasets:[{label:'Ventas Reales', data:[hist2024, real2025, forecast2026], fill:true, borderColor:'#3b82f6', backgroundColor:'rgba(59,130,246,.18)', tension:.35}]},
    options:{responsive:true, plugins:{legend:{labels:{color:'#cbd5e1', font:{weight:'bold'}}}},
      scales:{x:{ticks:{color:'#cbd5e1', font:{weight:'bold'}}}, y:{ticks:{color:'#cbd5e1', font:{weight:'bold'}}}}
    }
  });

  ensureChart('c-mix',{
    type:'doughnut',
    data:{labels:['Real','Meta'], datasets:[{data:[Number(o?.realTotal||0), Math.max(0, Number(o?.objTotal||0)-Number(o?.realTotal||0))], backgroundColor:['rgba(59,130,246,.85)','rgba(148,163,184,.20)'], borderWidth:0}]},
    options:{responsive:true, plugins:{legend:{labels:{color:'#cbd5e1', font:{weight:'bold'}}}}}
  });
}

function drawInputsChart(){
  const serie = currentSerie();
  const d = safeJSON(localStorage.getItem(`cch_pro_plan_${serie}`) || '{}') || {};
  const labels = ['Nuevo €','Nuevos cli','Dormidos','Ferias','Acciones'];
  const target = [d.newbiz_t||0, d.newcli_t||0, d.dorm_t||0, d.fair_t||0, d.act_t||0];
  const actual = [d.newbiz_a||0, d.newcli_a||0, d.dorm_a||0, 0, 0];

  ensureChart('c-inputs',{
    type:'bar',
    data:{
      labels,
      datasets:[
        {label:'Objetivo', data:target, backgroundColor:'rgba(34,197,94,.55)'},
        {label:'Actual', data:actual, backgroundColor:'rgba(96,165,250,.55)'}
      ]
    },
    options:{
      responsive:true,
      plugins:{legend:{labels:{color:'#cbd5e1', font:{weight:'bold'}}}},
      scales:{
        x:{ticks:{color:'#cbd5e1', font:{weight:'bold'}}, grid:{color:'rgba(148,163,184,.08)'}},
        y:{ticks:{color:'#cbd5e1', font:{weight:'bold'}}, grid:{color:'rgba(148,163,184,.08)'}}
      }
    }
  });
}

function runScrape(){
  // placeholder bonito
  const p = document.getElementById('scrap-p');
  const v = document.getElementById('scrap-v');
  if(p) p.innerHTML = ['HORECA','Retail','Promos','Travel','Wellness'].map(x=>`<span><i class="fa-solid fa-tag"></i>${x}</span>`).join('');
  if(v) v.innerHTML = ['Subida cacao','Fletes','Plazos','Competencia'].map(x=>`<span><i class="fa-solid fa-chart-line"></i>${x}</span>`).join('');
}

function wireAutoSave(){
  // Inputs
  ['in-newbiz-t','in-newbiz-a','in-newcli-t','in-newcli-a','in-dorm-t','in-dorm-a','in-fair-t','in-act-t','in-notes','in-impact-eur','in-impact-time']
    .forEach(id=>{
      const el=document.getElementById(id);
      if(!el) return;
      el.addEventListener('input', ()=>saveSerieInputs());
    });

  // SMART
  ['smart-s','smart-m','smart-a','smart-r','smart-t','smart-formula','smart-milestones'].forEach(id=>{
    const el=document.getElementById(id);
    if(!el) return;
    el.addEventListener('input', ()=>saveSMART());
  });

  // DAFO
  ['dafo-f','dafo-o','dafo-d','dafo-a'].forEach(id=>{
    const el=document.getElementById(id);
    if(!el) return;
    el.addEventListener('input', ()=>saveDAFO());
    el.addEventListener('blur', ()=>saveDAFO());
  });

  document.getElementById('sel-serie')?.addEventListener('change', ()=>{
    syncWithERP();
  });

  document.getElementById('btn-sync')?.addEventListener('click', ()=>{
    syncWithERP();
  });
}

document.addEventListener('DOMContentLoaded', ()=>{
  wireAutoSave();
  syncWithERP();
});
