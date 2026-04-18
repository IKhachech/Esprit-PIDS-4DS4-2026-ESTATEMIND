"""
BO4/rag_juridique/api.py — FastAPI v2  (remplace Gradio)
=========================================================
Corrections v2.1 :
  - Lifespan pattern (compatible Windows --reload, pas de crash multiprocessing)
  - Chargement .env automatique depuis rag_juridique/ ET BO4/
  - ROOT auto-détecté (datasets dans BO4/ ou rag_juridique/)
  - Streaming via threading.Thread + asyncio.Queue (robuste sur Windows)

Lancement :
  cd BO4/rag_juridique
  uvicorn api:app --host 127.0.0.1 --port 8000
"""

import os, json, asyncio, tempfile, threading
from pathlib import Path
from typing import Optional, AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ── .env : cherche d'abord dans le dossier courant, puis un niveau au-dessus
_THIS = os.path.dirname(os.path.abspath(__file__))
from dotenv import load_dotenv
load_dotenv(os.path.join(_THIS, ".env"))
load_dotenv(os.path.join(os.path.dirname(_THIS), ".env"))

from rag_pipeline import get_rag, extract_text_from_pdf

rag = None  # initialisé dans lifespan

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise le RAG une seule fois au démarrage (safe Windows)."""
    global rag
    print("⏳ Initialisation du RAG Juridique...")
    rag = await asyncio.to_thread(get_rag)
    print("✅ API prête — http://127.0.0.1:8000\n")
    yield

app = FastAPI(
    title="RAG Juridique Tunisien — BO4",
    description="Analyse automatique de conformité des contrats tunisiens",
    version="2.1",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

TYPES = [
    "", "بيع عقاري", "بيع أرض", "كراء سكني",
    "كراء تجاري", "كراء مختلط", "وعد بالبيع",
    "وكالة عقارية", "بيع منقول",
]

# ══════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {
        "status": "ok",
        "groq_configured": bool(os.getenv("GROQ_API_KEY")),
        "types_contrat": TYPES[1:],
    }


@app.post("/analyze")
async def analyze(
    texte: Optional[str] = Form(None),
    type_contrat: str = Form(""),
    pdf: Optional[UploadFile] = File(None),
):
    """Analyse complète synchrone — retourne JSON."""
    contract_text = await _get_text(pdf, texte)
    result = await asyncio.to_thread(rag.analyze, contract_text, type_contrat)
    return JSONResponse(result)


@app.post("/analyze/stream")
async def analyze_stream(
    texte: Optional[str] = Form(None),
    type_contrat: str = Form(""),
    pdf: Optional[UploadFile] = File(None),
):
    """
    SSE streaming — résultats progressifs :
      1. rules  (< 100ms)  → Score + Violations
      2. rag    (< 200ms)  → JORT chunks
      3. llm    (~10-15s)  → Rapport LLM
    Utilise un Thread + Queue pour compatibilité Windows asyncio.
    """
    contract_text = await _get_text(pdf, texte)

    async def event_generator() -> AsyncGenerator[str, None]:
        loop = asyncio.get_event_loop()
        queue: asyncio.Queue = asyncio.Queue()

        def producer():
            try:
                for chunk in rag.analyze_stream(contract_text, type_contrat):
                    loop.call_soon_threadsafe(queue.put_nowait, chunk)
            except Exception as e:
                loop.call_soon_threadsafe(queue.put_nowait,
                    json.dumps({"step": "error", "message": str(e)}))
            finally:
                loop.call_soon_threadsafe(queue.put_nowait, None)

        t = threading.Thread(target=producer, daemon=True)
        t.start()

        while True:
            item = await queue.get()
            if item is None:
                break
            yield f"data: {item}\n\n"

        yield 'data: {"step": "done"}\n\n'

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ══════════════════════════════════════════════════════════════
# HELPER
# ══════════════════════════════════════════════════════════════
async def _get_text(pdf: Optional[UploadFile], texte: Optional[str]) -> str:
    if pdf is not None:
        suffix = Path(pdf.filename).suffix or ".pdf"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await pdf.read())
            tmp_path = tmp.name
        text = await asyncio.to_thread(extract_text_from_pdf, tmp_path)
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        if text.startswith("[Erreur") or text.startswith("[PDF"):
            raise HTTPException(400, text)
        return text
    if texte and texte.strip():
        return texte.strip()
    raise HTTPException(400, "Fournissez un PDF ou un texte.")


# ══════════════════════════════════════════════════════════════
# UI HTML  (SPA légère, sans framework lourd)
# ══════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="fr" dir="ltr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>⚖️ RAG Juridique Tunisien — BO4</title>
<style>
  :root {
    --bg:#0f172a;--surface:#1e293b;--border:#334155;
    --accent:#3b82f6;--accent2:#6366f1;
    --text:#e2e8f0;--muted:#94a3b8;
    --green:#22c55e;--yellow:#eab308;--orange:#f97316;--red:#ef4444;
  }
  *{box-sizing:border-box;margin:0;padding:0;}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',sans-serif;min-height:100vh;}
  header{background:linear-gradient(135deg,var(--surface),#1a2744);border-bottom:1px solid var(--border);
    padding:16px 24px;display:flex;align-items:center;gap:12px;}
  header h1{font-size:1.25rem;font-weight:700;}
  .badge{background:var(--accent);color:#fff;border-radius:6px;padding:2px 8px;font-size:.75rem;font-weight:600;}
  .groq-status{margin-left:auto;font-size:.8rem;}
  .layout{display:grid;grid-template-columns:360px 1fr;height:calc(100vh - 57px);}
  .sidebar{background:var(--surface);border-right:1px solid var(--border);
    padding:20px;display:flex;flex-direction:column;gap:16px;overflow-y:auto;}
  .sidebar h2{font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);}
  .tab-btns{display:flex;border:1px solid var(--border);border-radius:8px;overflow:hidden;}
  .tab-btn{flex:1;padding:8px;background:transparent;color:var(--muted);border:none;
    cursor:pointer;font-size:.85rem;transition:all .15s;}
  .tab-btn.active{background:var(--accent);color:#fff;}
  .input-area{display:none;flex-direction:column;gap:10px;}
  .input-area.active{display:flex;}
  .drop-zone{border:2px dashed var(--border);border-radius:10px;padding:24px;
    text-align:center;cursor:pointer;transition:border-color .2s;}
  .drop-zone:hover,.drop-zone.drag{border-color:var(--accent);background:#1e3a5f22;}
  .drop-zone .icon{font-size:2rem;margin-bottom:8px;}
  #file-input{display:none;}
  .file-info{background:#0f2027;border:1px solid var(--border);border-radius:6px;
    padding:8px 12px;font-size:.82rem;color:var(--green);display:none;}
  textarea{width:100%;background:#0f172a;border:1px solid var(--border);border-radius:8px;
    color:var(--text);padding:10px;font-size:.88rem;resize:vertical;min-height:160px;font-family:inherit;}
  textarea:focus{outline:none;border-color:var(--accent);}
  select{width:100%;background:#0f172a;border:1px solid var(--border);border-radius:8px;
    color:var(--text);padding:9px 12px;font-size:.88rem;}
  .btn-analyze{width:100%;padding:12px;background:linear-gradient(135deg,var(--accent),var(--accent2));
    border:none;border-radius:10px;color:#fff;font-size:1rem;font-weight:700;cursor:pointer;
    display:flex;align-items:center;justify-content:center;gap:8px;}
  .btn-analyze:disabled{opacity:.5;cursor:not-allowed;}
  .examples{display:flex;flex-direction:column;gap:6px;}
  .ex-btn{background:#0f172a;border:1px solid var(--border);border-radius:6px;color:var(--muted);
    padding:7px 10px;font-size:.78rem;cursor:pointer;text-align:right;direction:rtl;}
  .ex-btn:hover{border-color:var(--accent);color:var(--text);}
  .main{display:flex;flex-direction:column;overflow:hidden;}
  .progress-bar{padding:10px 20px;background:var(--surface);border-bottom:1px solid var(--border);
    display:flex;align-items:center;gap:10px;}
  .step{padding:5px 12px;border-radius:20px;border:1px solid var(--border);
    font-size:.8rem;color:var(--muted);transition:all .3s;}
  .step.running{border-color:var(--yellow);color:var(--yellow);animation:pulse 1s infinite;}
  .step.done{border-color:var(--green);color:var(--green);}
  .step.error{border-color:var(--red);color:var(--red);}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
  .result-tabs{display:flex;border-bottom:1px solid var(--border);background:var(--surface);padding:0 16px;}
  .rtab{padding:12px 16px;border:none;background:transparent;color:var(--muted);cursor:pointer;
    font-size:.88rem;border-bottom:2px solid transparent;white-space:nowrap;}
  .rtab.active{color:var(--accent);border-bottom-color:var(--accent);}
  .result-panels{flex:1;overflow-y:auto;padding:20px;}
  .panel{display:none;}.panel.active{display:block;}
  .score-card{background:var(--surface);border-radius:12px;padding:24px;border:1px solid var(--border);margin-bottom:20px;}
  .score-big{font-size:3rem;font-weight:800;line-height:1;}
  .score-bar{height:10px;background:var(--border);border-radius:5px;overflow:hidden;margin:12px 0;}
  .score-fill{height:100%;border-radius:5px;transition:width .6s;}
  .score-meta{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:16px;}
  .meta-item{background:#0f172a;border-radius:8px;padding:10px 14px;}
  .meta-item .label{font-size:.75rem;color:var(--muted);margin-bottom:4px;}
  .meta-item .value{font-size:.9rem;font-weight:600;}
  .viols-table{width:100%;border-collapse:collapse;font-size:.84rem;}
  .viols-table th{background:#0f172a;padding:10px 12px;text-align:left;color:var(--muted);
    font-weight:600;border-bottom:1px solid var(--border);}
  .viols-table td{padding:10px 12px;border-bottom:1px solid #1e293b;vertical-align:top;}
  .viols-table tr:hover td{background:#1e293b44;}
  .badge-alerte{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.75rem;font-weight:700;}
  .badge-CRITIQUE{background:#ef444422;color:var(--red);}
  .badge-ÉLEVÉ{background:#f9731622;color:var(--orange);}
  .badge-NORMALE{background:#eab30822;color:var(--yellow);}
  .law-ref{font-family:monospace;font-size:.78rem;background:#0f172a;padding:2px 6px;border-radius:4px;color:var(--accent);}
  .gravity-bar{display:flex;gap:2px;margin-bottom:2px;}
  .gb{width:8px;height:8px;border-radius:2px;}
  .jort-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:14px;}
  .jort-card h4{color:var(--accent);margin-bottom:8px;font-size:.9rem;}
  .jort-meta{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px;}
  .jort-tag{padding:2px 8px;border-radius:10px;font-size:.73rem;background:#1e3a5f;color:#93c5fd;}
  .jort-text{font-size:.82rem;color:var(--muted);line-height:1.6;border-left:2px solid var(--border);padding-left:10px;margin-top:8px;}
  .rapport-content{background:var(--surface);border:1px solid var(--border);border-radius:10px;
    padding:20px;white-space:pre-wrap;font-size:.87rem;line-height:1.7;}
  .waiting{text-align:center;padding:60px 20px;color:var(--muted);}
  .waiting .big-icon{font-size:3rem;margin-bottom:12px;}
  label{font-size:.82rem;color:var(--muted);display:block;margin-bottom:4px;}
  .spinner{display:inline-block;width:16px;height:16px;border:2px solid #fff4;
    border-top-color:#fff;border-radius:50%;animation:spin .6s linear infinite;}
  @keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>

<header>
  <span style="font-size:1.5rem">⚖️</span>
  <div>
    <h1>RAG Juridique Tunisien</h1>
    <div style="font-size:.75rem;color:var(--muted)">مجلة الالتزامات والعقود · JORT · مجلة الحقوق العينية</div>
  </div>
  <span class="badge">BO4 v2.1</span>
  <span class="groq-status" id="groq-badge">⏳</span>
</header>

<div class="layout">
  <aside class="sidebar">
    <div>
      <h2>📥 Source du contrat</h2>
      <div class="tab-btns" style="margin-top:8px">
        <button class="tab-btn active" onclick="switchTab('pdf')">📄 PDF</button>
        <button class="tab-btn" onclick="switchTab('text')">✍️ Texte</button>
      </div>
    </div>

    <div class="input-area active" id="tab-pdf">
      <div class="drop-zone" id="drop-zone"
           onclick="document.getElementById('file-input').click()"
           ondragover="dz(event,'drag')" ondragleave="dz(event,'')" ondrop="onDrop(event)">
        <div class="icon">📄</div>
        <div>Cliquez ou glissez votre PDF</div>
        <small>PDF avec couche texte (arabe / français)</small>
      </div>
      <input type="file" id="file-input" accept=".pdf" onchange="onFileSelect(this)">
      <div class="file-info" id="file-info"></div>
    </div>

    <div class="input-area" id="tab-text">
      <label>Texte du contrat</label>
      <textarea id="texte-input" placeholder="عقد بيع: &#10;&#10;Contrat de vente :"></textarea>
    </div>

    <div>
      <label>📂 Type de contrat <span style="color:var(--muted)">(optionnel)</span></label>
      <select id="type-select">
        <option value="">— Auto-détection —</option>
        <option value="بيع عقاري">بيع عقاري — Vente immobilière</option>
        <option value="بيع أرض">بيع أرض — Vente terrain</option>
        <option value="كراء سكني">كراء سكني — Bail résidentiel</option>
        <option value="كراء تجاري">كراء تجاري — Bail commercial</option>
        <option value="كراء مختلط">كراء مختلط — Bail mixte</option>
        <option value="وعد بالبيع">وعد بالبيع — Promesse de vente</option>
        <option value="وكالة عقارية">وكالة عقارية — Mandat immobilier</option>
        <option value="بيع منقول">بيع منقول — Vente de meuble</option>
      </select>
    </div>

    <button class="btn-analyze" id="btn-analyze" onclick="runAnalysis()">
      🔍 Analyser le contrat
    </button>

    <div>
      <h2>📋 Exemples rapides</h2>
      <div class="examples">
        <button class="ex-btn" onclick="loadEx(0)">عقد بيع أرض (600م²)</button>
        <button class="ex-btn" onclick="loadEx(1)">عقد كراء سكني — تونس</button>
        <button class="ex-btn" onclick="loadEx(2)">وعد بالبيع — عربون 5000د</button>
      </div>
    </div>
  </aside>

  <main class="main">
    <div class="progress-bar">
      <div class="step" id="step-rules">📋 Règles</div>
      <span style="color:var(--border)">→</span>
      <div class="step" id="step-rag">📚 JORT RAG</div>
      <span style="color:var(--border)">→</span>
      <div class="step" id="step-llm">🤖 LLM Groq</div>
    </div>

    <div class="result-tabs">
      <button class="rtab active" onclick="showP('score',this)">📊 Score</button>
      <button class="rtab" onclick="showP('violations',this)">⚠️ Violations</button>
      <button class="rtab" onclick="showP('jort',this)">📜 JORT</button>
      <button class="rtab" onclick="showP('rapport',this)">🤖 Rapport LLM</button>
      <button class="rtab" onclick="showP('texte',this)">📄 Texte</button>
    </div>

    <div class="result-panels">
      <div class="panel active" id="panel-score">
        <div class="waiting"><div class="big-icon">⚖️</div><p>Soumettez un contrat pour démarrer</p></div>
      </div>
      <div class="panel" id="panel-violations">
        <div class="waiting"><div class="big-icon">⚠️</div><p>En attente d'analyse...</p></div>
      </div>
      <div class="panel" id="panel-jort">
        <div class="waiting"><div class="big-icon">📜</div><p>En attente d'analyse...</p></div>
      </div>
      <div class="panel" id="panel-rapport">
        <div class="waiting"><div class="big-icon">🤖</div><p>En attente du rapport LLM...</p></div>
      </div>
      <div class="panel" id="panel-texte">
        <div class="waiting"><div class="big-icon">📄</div><p>En attente d'un contrat...</p></div>
      </div>
    </div>
  </main>
</div>

<script>
const EX=[
  {t:"عقد بيع: نا الموقع أسفله الجنسية الحامل لبطاقة التعريف الوطنية رقم وانساكن بدوار. شهد واعترف بأنني أبيع 600م2 من الأرض. وبهذا شهد والتزم. إمضاء",c:"بيع أرض"},
  {t:"عقد كراء: يكري المكري للمكتري الشقة الكائنة بنهج الحرية عدد 12 تونس. مدة الكراء سنة واحدة. وبهذا أمضى الطرفان.",c:"كراء سكني"},
  {t:"عقد وعد بالبيع: وعد البائع ببيع العقار الكائن بشارع الحبيب بورقيبة. وقع المشتري عربونا بمبلغ 5000 دينار.",c:"وعد بالبيع"},
];
let currentTab='pdf',currentFile=null;

fetch('/health').then(r=>r.json()).then(d=>{
  document.getElementById('groq-badge').innerHTML=d.groq_configured
    ?'🟢 Groq — LLaMA 3.3 70B':'🔴 GROQ_API_KEY absente';
}).catch(()=>{document.getElementById('groq-badge').textContent='⚠️ API non joignable';});

function switchTab(tab){
  currentTab=tab;
  document.querySelectorAll('.tab-btn').forEach((b,i)=>b.classList.toggle('active',['pdf','text'][i]===tab));
  document.getElementById('tab-pdf').classList.toggle('active',tab==='pdf');
  document.getElementById('tab-text').classList.toggle('active',tab==='text');
}
function showP(name,btn){
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.rtab').forEach(b=>b.classList.remove('active'));
  document.getElementById('panel-'+name).classList.add('active');
  if(btn)btn.classList.add('active');
}
function loadEx(i){
  switchTab('text');
  document.getElementById('texte-input').value=EX[i].t;
  document.getElementById('type-select').value=EX[i].c;
}
function onFileSelect(inp){if(inp.files[0])setFile(inp.files[0]);}
function dz(e,cls){e.preventDefault();document.getElementById('drop-zone').className='drop-zone'+(cls?' '+cls:'');}
function onDrop(e){e.preventDefault();dz(e,'');const f=e.dataTransfer.files[0];if(f&&f.type==='application/pdf')setFile(f);}
function setFile(f){
  currentFile=f;
  const info=document.getElementById('file-info');
  info.style.display='block';
  info.textContent=`📄 ${f.name} (${(f.size/1024).toFixed(0)} KB)`;
}
function setStep(id,state){document.getElementById('step-'+id).className='step'+(state?' '+state:'');}
function resetSteps(){['rules','rag','llm'].forEach(s=>setStep(s,''));}

function renderScore({score,niveau,violations}){
  const color=score>=80?'var(--green)':score>=60?'var(--yellow)':score>=35?'var(--orange)':'var(--red)';
  const nb_crit=violations.filter(v=>v.alerte==='CRITIQUE').length;
  const nb_elev=violations.filter(v=>v.alerte==='ÉLEVÉ').length;
  document.getElementById('panel-score').innerHTML=`
    <div class="score-card">
      <div style="display:flex;align-items:flex-end;gap:12px">
        <div class="score-big" style="color:${color}">${score}</div>
        <div style="font-size:1.2rem;color:var(--muted);margin-bottom:8px">/100</div>
      </div>
      <div class="score-bar"><div class="score-fill" style="width:${score}%;background:${color}"></div></div>
      <div style="font-size:1.1rem;font-weight:700;margin-bottom:16px">${niveau}</div>
      <div class="score-meta">
        <div class="meta-item"><div class="label">Total violations</div><div class="value">${violations.length} règles</div></div>
        <div class="meta-item"><div class="label">🔴 Critique</div><div class="value">${nb_crit}</div></div>
        <div class="meta-item"><div class="label">🟠 Élevé</div><div class="value">${nb_elev}</div></div>
        <div class="meta-item"><div class="label">Modèle LLM</div><div class="value" style="font-size:.78rem">LLaMA 3.3 70B</div></div>
      </div>
    </div>`;
}
function renderViolations(viols){
  if(!viols.length){document.getElementById('panel-violations').innerHTML='<div style="text-align:center;padding:60px;color:var(--green);font-size:1.1rem">✅ Aucune violation détectée</div>';return;}
  const rows=viols.map((v,i)=>{
    const gc=v.score_gravite>=8?'var(--red)':v.score_gravite>=6?'var(--orange)':'var(--yellow)';
    const bars=Array.from({length:10},(_,j)=>`<div class="gb" style="background:${j<v.score_gravite?gc:'var(--border)'}"></div>`).join('');
    return `<tr><td>${i+1}</td><td><span class="badge-alerte badge-${v.alerte}">${v.alerte}</span></td>
      <td>${v.obligation}</td><td><span class="law-ref">${v.loi}</span></td>
      <td><div class="gravity-bar">${bars}</div><small style="color:var(--muted)">${v.score_gravite}/10</small></td>
      <td style="font-size:.78rem;color:var(--muted)">${v.penalite}</td></tr>`;
  }).join('');
  document.getElementById('panel-violations').innerHTML=`
    <p style="margin-bottom:12px;color:var(--muted);font-size:.85rem">
      🔴 ${viols.filter(v=>v.alerte==='CRITIQUE').length} CRITIQUE · 
      🟠 ${viols.filter(v=>v.alerte==='ÉLEVÉ').length} ÉLEVÉ · ${viols.length} total
    </p>
    <table class="viols-table">
      <thead><tr><th>#</th><th>Alerte</th><th>Obligation manquante</th><th>Loi</th><th>Gravité</th><th>Pénalité</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}
function renderJORT(chunks){
  if(!chunks.length){document.getElementById('panel-jort').innerHTML='<div class="waiting"><p>Aucun article JORT trouvé.</p></div>';return;}
  document.getElementById('panel-jort').innerHTML=chunks.map(c=>{
    const simPct=Math.round((c.similarity||0)*100);
    return `<div class="jort-card">
      <h4>📌 ${c.source_doc||'—'}</h4>
      <div class="jort-meta">
        <span class="jort-tag">${c.domaine_juridique||''}</span>
        <span class="jort-tag" style="background:#2d1b4e;color:#c4b5fd">${c.risque_conformite||''}</span>
        <span class="jort-tag" style="background:#1b3a1b;color:#86efac">${c.niveau_alerte||''}</span>
        <span style="font-size:.75rem;color:var(--accent)"> ${simPct}% pertinent</span>
      </div>
      <div style="font-size:.8rem;color:var(--muted);margin-bottom:4px"><strong>Obligations :</strong> ${c.obligations||''}</div>
      <div class="jort-text">${String(c.texte_chunk||'').slice(0,300)}...</div>
    </div>`;
  }).join('');
}
function renderRapport(rapport){
  if(!rapport)return;
  const html=rapport
    .replace(/## (.*)/g,'<h2 style="color:var(--accent);margin:16px 0 8px;font-size:1rem">$1</h2>')
    .replace(/### (.*)/g,'<h3 style="color:var(--muted);margin:10px 0 5px;font-size:.9rem">$1</h3>')
    .replace(/\*\*(.*?)\*\*/g,'<strong>$1</strong>')
    .replace(/🔴/g,'<span style="color:var(--red)">🔴</span>')
    .replace(/🟠/g,'<span style="color:var(--orange)">🟠</span>')
    .replace(/✅/g,'<span style="color:var(--green)">✅</span>')
    .replace(/❌/g,'<span style="color:var(--red)">❌</span>');
  document.getElementById('panel-rapport').innerHTML=`<div class="rapport-content">${html}</div>`;
}

async function runAnalysis(){
  const btn=document.getElementById('btn-analyze');
  btn.disabled=true;btn.innerHTML='<div class="spinner"></div> Analyse en cours...';
  resetSteps();

  const fd=new FormData();
  fd.append('type_contrat',document.getElementById('type-select').value);
  if(currentTab==='pdf'&&currentFile){
    fd.append('pdf',currentFile);
  }else{
    const t=document.getElementById('texte-input').value.trim();
    if(!t){alert('Veuillez saisir un texte ou uploader un PDF.');btn.disabled=false;btn.innerHTML='🔍 Analyser le contrat';return;}
    fd.append('texte',t);
  }

  try{
    const resp=await fetch('/analyze/stream',{method:'POST',body:fd});
    if(!resp.ok){const e=await resp.json();throw new Error(e.detail||'Erreur serveur');}
    const reader=resp.body.getReader();
    const dec=new TextDecoder();
    let buf='';
    while(true){
      const {done,value}=await reader.read();
      if(done)break;
      buf+=dec.decode(value,{stream:true});
      const lines=buf.split('\n');buf=lines.pop();
      for(const line of lines){
        if(!line.startsWith('data: '))continue;
        try{handleSSE(JSON.parse(line.slice(6)));}catch{}
      }
    }
  }catch(e){alert('Erreur : '+e.message);}
  finally{btn.disabled=false;btn.innerHTML='🔍 Analyser le contrat';}
}

function handleSSE(d){
  switch(d.step){
    case 'rules':
      if(d.status==='running')setStep('rules','running');
      if(d.status==='done'){
        setStep('rules','done');renderScore(d);renderViolations(d.violations);
        showP('score',document.querySelector('.rtab'));
      }
      break;
    case 'rag':
      if(d.status==='running')setStep('rag','running');
      if(d.status==='done'){setStep('rag','done');renderJORT(d.jort_chunks);}
      break;
    case 'llm':
      if(d.status==='running'){
        setStep('llm','running');
        document.getElementById('panel-rapport').innerHTML=
          '<div class="waiting"><div class="spinner" style="width:30px;height:30px;border-width:3px;margin:0 auto"></div><p style="margin-top:12px">Génération du rapport LLM... (~10-15s)</p></div>';
      }
      if(d.status==='done'||d.status==='skipped'){
        setStep('llm',d.status==='done'?'done':'error');
        renderRapport(d.rapport);
      }
      break;
  }
}
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
async def ui():
    return HTML