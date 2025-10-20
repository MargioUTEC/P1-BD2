/* Mini-DB Studio — Frontend
 * - Consola SQL (index.html)
 * - Búsqueda guiada (search.html)
 * - Explorador de índices (explorer.html)
 * Conectores: /api/run, /api/explain, /api/metadata, /api/search, /api/stats
 * Modo Mock: datos de muestra sin backend.
 */

const App = (() => {
  // ---- Mock data (fallback) ----
  const MOCK_COLUMNS = [
    { name: "restaurant_id", type: "int" },
    { name: "restaurant_name", type: "string", examples: ["Test Restaurant", "Hobing", "KFC"] },
    { name: "city", type: "string", examples: ["Lima", "Taguig City", "Delhi"] },
    { name: "longitude", type: "float" },
    { name: "latitude", type: "float" },
    { name: "aggregate_rating", type: "float", examples: [3.5, 4.2, 4.7] },
    { name: "average_cost_for_two", type: "int", examples: [50, 120, 200] },
    { name: "votes", type: "int", examples: [0, 50, 120] },
  ];

  const MOCK_ROWS = [
    { restaurant_id: 9999988, restaurant_name: "Test Restaurant", city: "Lima", longitude: 77.032, latitude: -12.046, average_cost_for_two: 120, aggregate_rating: 4.5, votes: 120 },
    { restaurant_id: 6152, restaurant_name: "Hobing", city: "Taguig City", longitude: 121.055, latitude: 14.553, average_cost_for_two: 450, aggregate_rating: 4.3, votes: 254 },
    { restaurant_id: 7001, restaurant_name: "KFC", city: "Delhi", longitude: 77.21, latitude: 28.61, average_cost_for_two: 300, aggregate_rating: 3.8, votes: 48 },
  ];

  const MOCK_STATS = {
    hash: { global_depth: 7, dir_size: 128, reads: 20, writes: 4 },
    rtree: { points: 50 },
    avl_count: 50,
    isam_pages: 7
  };

  // ---- Utilities ----
  const $ = (id) => document.getElementById(id);
  const el = (sel, root=document) => root.querySelector(sel);
  const els = (sel, root=document) => Array.from(root.querySelectorAll(sel));
  const fmt = (x) => (x === null || x === undefined) ? "" : x;

  const addLog = (consoleId, level, message) => {
    const host = $(consoleId);
    const row = document.createElement('div');
    row.className = 'log';
    const tag = document.createElement('span');
    tag.className = `tag ${level}`;
    tag.textContent = level;
    const msg = document.createElement('div');
    msg.textContent = message;
    row.appendChild(tag);
    row.appendChild(msg);
    host.appendChild(row);
    host.scrollTop = host.scrollHeight;
  };

  const debounce = (fn, ms=250) => {
    let t;
    return (...args) => { clearTimeout(t); t = setTimeout(() => fn.apply(null, args), ms); };
  };

  const detectOps = (type) => {
    switch(type) {
      case 'int':
      case 'float':
        return [
          {v:'=', t:'='}, {v:'!=', t:'≠'}, {v:'>', t:'>'}, {v:'>=', t:'≥'}, {v:'<', t:'<'}, {v:'<=', t:'≤'},
          {v:'between', t:'BETWEEN'}
        ];
      case 'string':
      default:
        return [
          {v:'=', t:'igual'}, {v:'!=', t:'distinto'}, {v:'contains', t:'contiene'}, {v:'starts', t:'empieza con'}
        ];
    }
  };

  const buildValueInput = (type, examples=[]) => {
    const wrapper = document.createElement('div');
    const input = document.createElement('input');
    input.className = 'input';
    input.placeholder = type === 'int' || type === 'float' ? 'Ej: 100' : 'Ej: Lima';
    input.setAttribute('data-type', type);
    wrapper.appendChild(input);

    if (type === 'int' || type === 'float') {
      // chips de ejemplo + ayuda BETWEEN
      if (examples?.length) {
        const chips = document.createElement('div');
        chips.style.marginTop = '6px';
        examples.slice(0,3).forEach(v => {
          const b = document.createElement('button');
          b.className = 'btn ghost';
          b.style.padding = '4px 8px';
          b.textContent = v;
          b.addEventListener('click', () => input.value = v);
          chips.appendChild(b);
        });
        wrapper.appendChild(chips);
      }
    } else {
      // para texto: ejemplos
      if (examples?.length) {
        const chips = document.createElement('div');
        chips.style.marginTop = '6px';
        examples.slice(0,3).forEach(v => {
          const b = document.createElement('button');
          b.className = 'btn ghost';
          b.style.padding = '4px 8px';
          b.textContent = v;
          b.addEventListener('click', () => input.value = v);
          chips.appendChild(b);
        });
        wrapper.appendChild(chips);
      }
    }
    return wrapper;
  };

  const buildBetweenInputs = (type) => {
    const box = document.createElement('div');
    box.style.display = 'grid';
    box.style.gridTemplateColumns = '1fr 1fr';
    box.style.gap = '8px';
    const a = document.createElement('input');
    a.className = 'input';
    a.placeholder = 'Desde…';
    a.setAttribute('data-type', type);
    const b = document.createElement('input');
    b.className = 'input';
    b.placeholder = 'Hasta…';
    b.setAttribute('data-type', type);
    box.appendChild(a); box.appendChild(b);
    return box;
  };

  const guessIndexAdvice = (cols) => {
    // retorna { ok:[], warn:[], err:[] } según columnas seleccionadas
    const names = cols.map(c => c.name);
    const types = Object.fromEntries(cols.map(c => [c.name, c.type]));
    const advice = { ok: [], warn: [], err: [] };

    // heurística rápida:
    names.forEach(n => {
      const t = types[n] || 'string';
      if (n === 'restaurant_id') advice.ok.push('HASH / B+Tree');
      if (['city','restaurant_name','name'].includes(n)) advice.ok.push('ISAM');
      if (['longitude','latitude'].includes(n)) advice.ok.push('R-Tree');
      if (['aggregate_rating','votes','average_cost_for_two','avg_cost_for_two'].includes(n)) advice.ok.push('AVL');
      if (t === 'string' && ['AVL','HASH','BTREE'].includes('AVL')) advice.warn.push('AVL con texto no aplica');
    });
    return advice;
  };

  const validateForcedIndex = (forced, filters) => {
    if (!forced) return { level:'OK', msg:'Índice automático.' };
    // reglas de compatibilidad
    const usedCols = filters.map(f => f.col);
    const any = (cols) => usedCols.some(c => cols.includes(c));
    switch (forced) {
      case 'ISAM':
        if (any(['city','restaurant_name','name'])) return {level:'OK', msg:'USING ISAM sobre columnas textuales.'};
        return {level:'WARN', msg:'ISAM es ideal para name/city. Revisa compatibilidad.'};
      case 'AVL':
        if (any(['aggregate_rating','votes','average_cost_for_two','avg_cost_for_two'])) return {level:'OK', msg:'USING AVL sobre atributos numéricos.'};
        return {level:'ERROR', msg:'AVL solo aplica a campos numéricos.'};
      case 'HASH':
        if (any(['restaurant_id'])) return {level:'OK', msg:'USING HASH sobre id exacto.'};
        return {level:'WARN', msg:'HASH es óptimo para restaurant_id exacto.'};
      case 'BTREE':
        if (any(['restaurant_id'])) return {level:'OK', msg:'USING B+Tree para rangos por id.'};
        return {level:'WARN', msg:'B+Tree recomendado para rangos por id.'};
      case 'RTREE':
        if (any(['longitude','latitude'])) return {level:'OK', msg:'USING R-Tree sobre lon/lat.'};
        return {level:'ERROR', msg:'R-Tree requiere lon/lat (y radio).'};
      default:
        return {level:'WARN', msg:'Índice no reconocido.'};
    }
  };

  // ---- Tables (sort, paginate, quick search) ----
  function renderTable({tableId, theadId, tbodyId, data, visibleCols, sortBy, sortDir}) {
    const table = $(tableId);
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    const rows = data || [];

    const cols = visibleCols && visibleCols.length
      ? visibleCols
      : (rows[0] ? Object.keys(rows[0]) : []);

    // headers
    thead.innerHTML = '';
    const trh = document.createElement('tr');
    cols.forEach(c => {
      const th = document.createElement('th');
      th.textContent = c;
      th.dataset.col = c;
      if (c === sortBy) th.textContent += sortDir === 'asc' ? ' ▲' : ' ▼';
      th.addEventListener('click', () => {
        const ev = new CustomEvent('table:sort', { detail: { col: c }});
        table.dispatchEvent(ev);
      });
      trh.appendChild(th);
    });
    thead.appendChild(trh);

    // body
    tbody.innerHTML = '';
    rows.forEach(r => {
      const tr = document.createElement('tr');
      cols.forEach(c => {
        const td = document.createElement('td');
        td.textContent = fmt(r[c]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }

  function paginate(arr, page, size) {
    const start = (page-1)*size;
    return arr.slice(start, start+size);
  }

  function attachPager({pagerId, total, page, size, onChange}) {
    const host = $(pagerId);
    host.innerHTML = '';
    const totalPages = Math.max(1, Math.ceil(total / size));

    const mk = (label, disabled, p) => {
      const b = document.createElement('button');
      b.className = 'btn ghost';
      b.textContent = label;
      if (disabled) b.disabled = true;
      b.addEventListener('click', () => onChange(p));
      return b;
    };

    host.appendChild(mk('«', page<=1, 1));
    host.appendChild(mk('‹', page<=1, page-1));
    const info = document.createElement('span');
    info.className = 'muted';
    info.textContent = `Página ${page} de ${totalPages} — ${total} fila(s)`;
    host.appendChild(info);
    host.appendChild(mk('›', page>=totalPages, page+1));
    host.appendChild(mk('»', page>=totalPages, totalPages));
  }

  function quickFilter(rows, query) {
    if (!query) return rows;
    const q = query.toLowerCase();
    return rows.filter(r => Object.values(r).some(v => String(v).toLowerCase().includes(q)));
  }

  // ------- Data sources -------
  async function fetchJSON(url, opts={}, mockData) {
    const mock = el('#mockToggle')?.checked;
    if (mock && mockData !== undefined) {
      // simular latencia
      await new Promise(r => setTimeout(r, 250));
      return mockData;
    }
    const res = await fetch(url, opts);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  async function getMetadata() {
    // /api/metadata -> { columns: [{name,type,examples?}, ...] }
    const mock = { columns: MOCK_COLUMNS };
    try {
      return await fetchJSON('/api/metadata', {}, mock);
    } catch {
      return mock;
    }
  }

  // ------- Pages -------
  function initConsolePage(cfg) {
    const state = {
      rows: [],
      allCols: [],
      visibleCols: [],
      sortBy: null,
      sortDir: 'asc',
      page: 1,
      size: 10
    };

    // Page size toggles
    cfg.pageSizeButtons.forEach(id => {
      $(id).addEventListener('click', (e) => {
        els('.seg').forEach(b => b.classList.remove('active'));
        e.currentTarget.classList.add('active');
        state.size = parseInt(e.currentTarget.dataset.size, 10) || 10;
        state.page = 1;
        render();
      });
    });

    // Column visibility control
    function populateColumnVisibility(cols) {
      const sel = $(cfg.columnVisibilityId);
      sel.innerHTML = '';
      cols.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c; opt.textContent = c; opt.selected = true;
        sel.appendChild(opt);
      });
      sel.addEventListener('change', () => {
        state.visibleCols = Array.from(sel.selectedOptions).map(o => o.value);
        render();
      });
      state.visibleCols = cols.slice();
    }

    function setPlanBadge(plan) {
      const b = $(cfg.planBadgeId);
      if (!plan) { b.hidden = true; return; }
      b.hidden = false;
      b.textContent = `PLAN: ${plan}`;
    }

    function render() {
      const filtered = quickFilter(state.rows, $(cfg.quickSearchId).value);
      const sorted = state.sortBy
        ? filtered.slice().sort((a,b) => {
            const va = a[state.sortBy]; const vb = b[state.sortBy];
            if (va === vb) return 0;
            return (va > vb ? 1 : -1) * (state.sortDir === 'asc' ? 1 : -1);
          })
        : filtered;
      const pageRows = paginate(sorted, state.page, state.size);

      renderTable({
        tableId: cfg.table.tableId,
        theadId: cfg.table.thead,
        tbodyId: cfg.table.tbody,
        data: pageRows,
        visibleCols: state.visibleCols,
        sortBy: state.sortBy,
        sortDir: state.sortDir
      });

      attachPager({
        pagerId: cfg.table.pagerId,
        total: sorted.length,
        page: state.page,
        size: state.size,
        onChange: (p) => { state.page = p; render(); }
      });

      // sortable header
      $(cfg.table.tableId).addEventListener('table:sort', (ev) => {
        const col = ev.detail.col;
        if (state.sortBy === col) {
          state.sortDir = (state.sortDir === 'asc') ? 'desc' : 'asc';
        } else {
          state.sortBy = col; state.sortDir = 'asc';
        }
        render();
      });
    }

    $(cfg.quickSearchId).addEventListener('input', debounce(render, 150));

    // Run buttons
    $(cfg.explainBtnId).addEventListener('click', async () => {
      const sql = el(`#${cfg.editorId}`).innerText.trim();
      if (!sql) { addLog(cfg.consoleId, 'WARN', 'No hay consulta.'); return; }
      addLog(cfg.consoleId, 'INFO', 'Ejecutando EXPLAIN…');
      try {
        const body = { sql, options: { mode: 'EXPLAIN' } };
        const data = await fetchJSON('/api/explain', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)}, {
          plan: "Index Scan using ISAM on restaurants",
          filter: "city = 'Lima'",
          index_used: "ISAM",
          estimated_cost: 0.42,
          rows: 1,
          execution_time_ms: 0.8
        });
        addLog(cfg.consoleId, 'OK', data.plan + ` (index=${data.index_used})`);
        setPlanBadge(data.index_used || 'AUTO');
      } catch (e) {
        addLog(cfg.consoleId, 'ERROR', `EXPLAIN falló: ${e.message}`);
      }
    });

    $(cfg.runBtnId).addEventListener('click', async () => {
      const sqlRaw = el(`#${cfg.editorId}`).innerText.trim();
      const usingHint = $(cfg.indexHintId).value;
      if (!sqlRaw) { addLog(cfg.consoleId, 'WARN', 'No hay consulta.'); return; }
      let sql = sqlRaw;
      if (usingHint && !/USING\s+/i.test(sqlRaw)) {
        // inyectar USING <idx> de ser necesario
        sql = sqlRaw.replace(/;?$/, ` USING ${usingHint};`);
      }

      addLog(cfg.consoleId, 'INFO', 'Ejecutando consulta…');
      try {
        const data = await fetchJSON('/api/run', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ sql })}, {
          columns: MOCK_COLUMNS.map(c => c.name),
          rows: MOCK_ROWS,
          plan_used: usingHint || 'ISAM',
          message: "[OK] 3 fila(s) devueltas."
        });

        // --- NUEVO BLOQUE DE LOGS DETALLADOS ---
if (data.logs && Array.isArray(data.logs)) {
  data.logs.forEach(line => {
    const match = line.match(/^\[(\w+)\]\s*(.*)$/); // detecta "[LEVEL] mensaje"
    if (match) {
      const [, level, msg] = match;
      addLog(cfg.consoleId, level, msg);
    } else {
      addLog(cfg.consoleId, 'INFO', line);
    }
  });
} else if (data.log) {
  // compatibilidad si llega como string
  data.log.split('\n').forEach(line => {
    const match = line.match(/^\[(\w+)\]\s*(.*)$/);
    if (match) {
      const [, level, msg] = match;
      addLog(cfg.consoleId, level, msg);
    } else {
      addLog(cfg.consoleId, 'INFO', line);
    }
  });
} else {
  addLog(cfg.consoleId, 'OK', data.message || 'Consulta completada.');
}

setPlanBadge(data.plan_used || 'AUTO');


        state.rows = data.rows || [];
        const cols = data.columns?.length ? data.columns : (state.rows[0] ? Object.keys(state.rows[0]) : []);
        populateColumnVisibility(cols);
        state.page = 1;
        render();
      } catch (e) {
        addLog(cfg.consoleId, 'ERROR', `Consulta falló: ${e.message}`);
      }
    });

    // init columns for multi-select (metadata)
    (async () => {
      const meta = await getMetadata();
      const cols = meta.columns.map(c => c.name);
      const sel = $(cfg.columnVisibilityId);
      sel.innerHTML = '';
      cols.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c; opt.textContent = c; opt.selected = true;
        sel.appendChild(opt);
      });
      state.visibleCols = cols.slice();
    })();
  }

  function initSearchPage(cfg) {
    const state = {
      rows: [],
      allCols: [],
      visibleCols: [],
      sortBy: null, sortDir: 'asc',
      page: 1, size: 10,
      filters: [] // { col, op, value } (o {col, op:'between', value:[a,b]})
    };

    // Load metadata and prime one filter
    (async () => {
      const meta = await getMetadata();
      state.allCols = meta.columns;
      addFilterRow(); // crea 1 por defecto
      populateColumnVisibility(meta.columns.map(c => c.name));
    })();

    function populateColumnVisibility(cols) {
      const sel = $(cfg.columnVisibilityId);
      sel.innerHTML = '';
      cols.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c; opt.textContent = c; opt.selected = true;
        sel.appendChild(opt);
      });
      sel.addEventListener('change', () => {
        state.visibleCols = Array.from(sel.selectedOptions).map(o => o.value);
        render();
      });
      state.visibleCols = cols.slice();
    }

    // Add/clear filters
    $(cfg.addFilterBtnId).addEventListener('click', addFilterRow);
    $(cfg.clearFiltersBtnId).addEventListener('click', () => {
      state.filters = [];
      $(cfg.filterGridId).innerHTML = '';
      addFilterRow();
      setStatus([{type:'ok', text:'Filtros reiniciados.'}]);
    });

    function addFilterRow() {
      const tpl = $(cfg.filterRowTplId).content.cloneNode(true);
      const row = tpl.querySelector('.filter-row');
      const colSel = row.querySelector('.col-select');
      const opSel = row.querySelector('.op-select');
      const valueSlot = row.querySelector('.value-slot');
      const tips = row.querySelector('.filter-tips');

      // columnas
      state.allCols.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.name; opt.textContent = c.name;
        colSel.appendChild(opt);
      });

      // init operator/value por defecto
      function refreshOpsAndValue() {
        const colName = colSel.value;
        const cmeta = state.allCols.find(c => c.name === colName) || {type:'string'};
        opSel.innerHTML = '';
        detectOps(cmeta.type).forEach(o => {
          const opt = document.createElement('option');
          opt.value = o.v; opt.textContent = o.t;
          opSel.appendChild(opt);
        });
        valueSlot.innerHTML = '';
        valueSlot.appendChild(buildValueInput(cmeta.type, cmeta.examples||[]));
        tips.textContent = `Tipo: ${cmeta.type}. Ejemplos: ${ (cmeta.examples || []).join(', ') || '—' }`;
      }

      colSel.addEventListener('change', refreshOpsAndValue);
      opSel.addEventListener('change', () => {
        const colName = colSel.value;
        const cmeta = state.allCols.find(c => c.name === colName) || {type:'string'};
        valueSlot.innerHTML = '';
        if (opSel.value === 'between') {
          valueSlot.appendChild(buildBetweenInputs(cmeta.type));
        } else {
          valueSlot.appendChild(buildValueInput(cmeta.type, cmeta.examples||[]));
        }
      });

      refreshOpsAndValue();

      // remove
      row.querySelector('.remove-filter').addEventListener('click', () => {
        row.remove();
      });

      $(cfg.filterGridId).appendChild(row);
    }

    function collectFilters() {
      const rows = els('.filter-row', $(cfg.filterGridId));
      const out = [];
      rows.forEach(r => {
        const col = el('.col-select', r).value;
        const op = el('.op-select', r).value;
        const valueHost = el('.value-slot', r);

        if (op === 'between') {
          const a = valueHost.children[0]?.value;
          const b = valueHost.children[1]?.value;
          if (a !== '' && b !== '') out.push({ col, op, value: [a,b] });
        } else {
          const v = valueHost.querySelector('input')?.value ?? '';
          if (v !== '') out.push({ col, op, value: v });
        }
      });
      return out;
    }

    function setStatus(items) {
      const host = $(cfg.statusId);
      host.innerHTML = '';
      (items||[]).forEach(it => {
        const div = document.createElement('div');
        div.className = `alert ${it.type==='ok'?'ok': it.type==='warn'?'warn':'err'}`;
        div.textContent = it.text;
        host.appendChild(div);
      });
    }

    function buildSQLFromFilters(filters, forced) {
      if (!filters.length) return { sql: 'SELECT * FROM restaurants;', warn: [] };

      const parts = filters.map(f => {
        const col = f.col;
        if (f.op === 'between') {
          const [a,b] = f.value;
          return `(${col} BETWEEN ${q(a)} AND ${q(b)})`;
        }
        const opMap = { '=':'=', '!=':'<>', '>':'>', '>=':'>=', '<':'<', '<=':'<=', 'contains':'LIKE', 'starts':'LIKE' };
        if (f.op === 'contains') return `(${col} ${opMap[f.op]} ${q('%'+f.value+'%')})`;
        if (f.op === 'starts') return `(${col} ${opMap[f.op]} ${q(f.value+'%')})`;
        return `(${col} ${opMap[f.op]||f.op} ${q(f.value)})`;
      });
      const where = parts.join(' AND ');
      const using = forced ? ` USING ${forced}` : '';
      return { sql: `SELECT * FROM restaurants${using} WHERE ${where};`, warn: [] };

      function q(v) {
        return isNaN(Number(v)) ? `"${String(v).replaceAll('"','\\"')}"` : v;
      }
    }

    function render() {
      const filtered = quickFilter(state.rows, $(cfg.quickSearchId).value);
      const sorted = state.sortBy
        ? filtered.slice().sort((a,b) => {
            const va = a[state.sortBy]; const vb = b[state.sortBy];
            if (va === vb) return 0;
            return (va > vb ? 1 : -1) * (state.sortDir === 'asc' ? 1 : -1);
          })
        : filtered;
      const pageRows = paginate(sorted, state.page, state.size);

      renderTable({
        tableId: cfg.table.tableId,
        theadId: cfg.table.thead,
        tbodyId: cfg.table.tbody,
        data: pageRows,
        visibleCols: state.visibleCols,
        sortBy: state.sortBy,
        sortDir: state.sortDir
      });

      attachPager({
        pagerId: cfg.table.pagerId,
        total: sorted.length,
        page: state.page,
        size: state.size,
        onChange: (p) => { state.page = p; render(); }
      });

      $(cfg.table.tableId).addEventListener('table:sort', (ev) => {
        const col = ev.detail.col;
        if (state.sortBy === col) {
          state.sortDir = (state.sortDir === 'asc') ? 'desc' : 'asc';
        } else {
          state.sortBy = col; state.sortDir = 'asc';
        }
        render();
      });
    }

    $(cfg.quickSearchId).addEventListener('input', debounce(render, 150));
    cfg.pageSizeButtons.forEach(id => {
      $(id).addEventListener('click', (e) => {
        els('.seg').forEach(b => b.classList.remove('active'));
        e.currentTarget.classList.add('active');
        state.size = parseInt(e.currentTarget.dataset.size, 10) || 10;
        state.page = 1;
        render();
      });
    });

    // Run / Explain
    $(cfg.runBtnId).addEventListener('click', async () => {
      const filters = collectFilters();
      const forced = $(cfg.forcedIndexId).value;
      const advice = validateForcedIndex(forced, filters);
      const sqlObj = buildSQLFromFilters(filters, forced);

      const msgs = [];
      if (advice.level === 'ERROR') msgs.push({type:'err', text: advice.msg});
      else if (advice.level === 'WARN') msgs.push({type:'warn', text: advice.msg});
      else msgs.push({type:'ok', text: advice.msg || 'Índice automático.'});

      setStatus(msgs);

      try {
        const data = await fetchJSON('/api/search', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ filters, forced_index: forced, sql: sqlObj.sql })}, {
          columns: MOCK_COLUMNS.map(c => c.name),
          rows: MOCK_ROWS,
          plan_used: forced || 'AUTO',
          status: advice.level === 'ERROR' ? 'error' : 'ok',
          message: advice.level === 'ERROR' ? 'Estructura incompatible; se ejecutó plan AUTO.' : '[OK] 3 fila(s) devueltas.'
        });

        const statusItems = [];
        if (data.status === 'error') statusItems.push({type:'warn', text: data.message});
        else statusItems.push({type:'ok', text: data.message || 'Búsqueda completada.'});
        if (forced) statusItems.push({type: data.status === 'error' ? 'warn':'ok', text:`Plan: ${data.plan_used}`});
        setStatus(statusItems);

        state.rows = data.rows || [];
        const cols = data.columns?.length ? data.columns : (state.rows[0] ? Object.keys(state.rows[0]) : []);
        state.visibleCols = cols.slice();
        // refrescar selector
        const sel = $(cfg.columnVisibilityId);
        sel.innerHTML = '';
        cols.forEach(c => {
          const opt = document.createElement('option');
          opt.value = c; opt.textContent = c; opt.selected = true;
          sel.appendChild(opt);
        });

        state.page = 1;
        render();
      } catch(e) {
        setStatus([{type:'err', text:`Búsqueda falló: ${e.message}`}]);
      }
    });

    $(cfg.explainBtnId).addEventListener('click', async () => {
      const filters = collectFilters();
      const forced = $(cfg.forcedIndexId).value;
      const sqlObj = buildSQLFromFilters(filters, forced);
      try {
        const data = await fetchJSON('/api/explain', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ sql: sqlObj.sql, options: { mode:'EXPLAIN' } })}, {
          plan: forced ? `Index Scan using ${forced} on restaurants` : 'Seq Scan on restaurants',
          filter: '(...)',
          index_used: forced || 'Sequential',
          estimated_cost: 0.21, rows: 3, execution_time_ms: 0.4
        });
        setStatus([{type:'ok', text:`${data.plan} — Index: ${data.index_used}`}]);
      } catch(e) {
        setStatus([{type:'err', text:`EXPLAIN falló: ${e.message}`}]);
      }
    });
  }

  function initExplorerPage(cfg) {
    async function load() {
      try {
        const data = await fetchJSON('/api/stats', {}, {
          hash: MOCK_STATS.hash,
          rtree: { points: MOCK_STATS.rtree.points },
          avl_count: MOCK_STATS.avl_count,
          isam_pages: MOCK_STATS.isam_pages
        });
        $(cfg.isamPagesId).textContent = data.isam_pages ?? '—';
        $(cfg.hashDepthId).textContent = data.hash?.global_depth ?? '—';
        $(cfg.hashEntriesId).textContent = data.hash?.dir_size ?? '—';
        $(cfg.hashReadsId).textContent = data.hash?.reads ?? '—';
        $(cfg.hashWritesId).textContent = data.hash?.writes ?? '—';
        $(cfg.rtreePointsId).textContent = data.rtree?.points ?? '—';
        $(cfg.avlCountId).textContent = data.avl_count ?? '—';
        setStatus([{type:'ok', text:'Estadísticas actualizadas.'}]);
      } catch(e) {
        setStatus([{type:'err', text:`No se pudieron cargar estadísticas: ${e.message}`}]);
      }
    }
    function setStatus(items) {
      const host = $(cfg.statusId);
      host.innerHTML = '';
      (items||[]).forEach(it => {
        const div = document.createElement('div');
        div.className = `alert ${it.type==='ok'?'ok': it.type==='warn'?'warn':'err'}`;
        div.textContent = it.text;
        host.appendChild(div);
      });
    }
    $(cfg.refreshBtnId).addEventListener('click', load);
    load();
  }

  return { initConsolePage, initSearchPage, initExplorerPage };
})();
