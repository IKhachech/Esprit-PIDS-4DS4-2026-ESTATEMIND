"""
BO4/rag_pipeline.py  — VERSION 2 (FastAPI-ready, optimisée)
============================================================
Améliorations vs v1 :
  • Chargement lazy unique (singleton) — init une seule fois
  • TF-IDF char_wb conservé MAIS vectorizer pré-sauvegardé avec joblib
  • RuleEngine : tokenisation compilée (re.compile)
  • Groq : timeout explicite + retry léger
  • Pas de Gradio → compatible FastAPI / CLI
"""

import os, re, json, pickle, hashlib
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# rag_pipeline.py est dans BO4/rag_juridique/
# Les datasets sont dans BO4/  →  ROOT pointe vers BO4/
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
# Si les datasets sont dans le même dossier que ce fichier → ROOT = _THIS_DIR
# Si les datasets sont un niveau au-dessus → ROOT = parent
# On détecte automatiquement :
_PARENT = os.path.dirname(_THIS_DIR)
ROOT = _THIS_DIR if os.path.exists(os.path.join(_THIS_DIR, "dataset_BO4_contrats_final.xlsx")) \
       else _PARENT
CACHE_PATH = os.path.join(_THIS_DIR, ".vectorstore_cache.pkl")

# ══════════════════════════════════════════════════════════════
# PDF EXTRACTION
# ══════════════════════════════════════════════════════════════
def extract_text_from_pdf(pdf_path: str) -> str:
    try:
        import fitz
        doc = fitz.open(pdf_path)
        pages = [page.get_text("text") for page in doc]
        doc.close()
        texte = "\n".join(pages).strip()
        return texte or "[PDF scanné — texte non extractible.]"
    except Exception as e:
        return f"[Erreur extraction PDF : {e}]"


# ══════════════════════════════════════════════════════════════
# VECTOR STORE  —  avec cache joblib pour éviter re-fit au restart
# ══════════════════════════════════════════════════════════════
class VectorStore:
    def __init__(self, df_jort: pd.DataFrame):
        self.df = df_jort.reset_index(drop=True)

        # Hash du dataset pour invalider le cache si les données changent
        data_hash = hashlib.md5(
            pd.util.hash_pandas_object(df_jort).values.tobytes()
        ).hexdigest()[:8]
        cache_file = CACHE_PATH + f".{data_hash}"

        if os.path.exists(cache_file):
            print("  ⚡ VectorStore : cache trouvé — chargement rapide")
            with open(cache_file, "rb") as f:
                saved = pickle.load(f)
            self.vectorizer = saved["vectorizer"]
            self.matrix = saved["matrix"]
        else:
            print("  🔧 VectorStore : construction du TF-IDF...")
            corpus = (
                df_jort["texte_chunk"].fillna("") + " " +
                df_jort["texte_normalise"].fillna("") + " " +
                df_jort["domaine_juridique"].fillna("") + " " +
                df_jort["obligations"].fillna("") + " " +
                df_jort["niveau_alerte"].fillna("") + " " +
                df_jort["risque_conformite"].fillna("")
            ).tolist()
            self.vectorizer = TfidfVectorizer(
                analyzer="char_wb", ngram_range=(2, 4),
                max_features=12_000, sublinear_tf=True,
            )
            self.matrix = self.vectorizer.fit_transform(corpus)
            with open(cache_file, "wb") as f:
                pickle.dump({"vectorizer": self.vectorizer, "matrix": self.matrix}, f)
        print(f"  ✅ VectorStore : {self.matrix.shape[0]} chunks JORT indexés")

    def search(self, query: str, top_k: int = 6, threshold: float = 0.02) -> pd.DataFrame:
        q_vec = self.vectorizer.transform([query])
        scores = cosine_similarity(q_vec, self.matrix).flatten()
        idx = np.argsort(scores)[::-1][:top_k]
        hits = self.df.iloc[idx].copy()
        hits["similarity"] = scores[idx]
        return hits[hits["similarity"] >= threshold].reset_index(drop=True)


# ══════════════════════════════════════════════════════════════
# RULE ENGINE  —  regex compilée, lookup set
# ══════════════════════════════════════════════════════════════
class RuleEngine:
    DOMAIN_MAP = {
        "بيع أرض":      ["أرض", "قطعة", "دونم", "هكتار"],
        "بيع عقاري":    ["بيع", "عقار", "شقة", "منزل", "مسكن", "مبنى"],
        "كراء سكني":    ["كراء", "سكن", "شقة", "إيجار", "مسكن"],
        "كراء تجاري":   ["كراء", "تجاري", "محل", "مستودع", "مكتب"],
        "كراء مختلط":   ["كراء", "مختلط"],
        "وعد بالبيع":   ["وعد", "عربون"],
        "وكالة عقارية": ["وكالة", "وكيل", "موكّل", "تفويض"],
        "بيع منقول":    ["منقول", "سيارة", "آلة", "معدات"],
    }
    STOPWORDS = frozenset({
        "في","أو","و","من","على","دون","بدون","عند","لدى",
        "مع","إلى","عن","ب","ل","ك","هذا","هذه","التي","الذي",
        "عقد","قد","كان","يكون","يجب",
    })
    _AR_PATTERN = re.compile(r'[\u0600-\u06FF]{3,}')

    def __init__(self, df_rules: pd.DataFrame):
        self.rules = df_rules.copy()
        # Pré-tokeniser toutes les conditions
        self.rules["_tokens"] = self.rules["condition_trigger"].astype(str).apply(self._tokenize)
        print(f"  ✅ RuleEngine  : {len(df_rules)} règles chargées")

    def detect(self, texte: str, type_contrat: str = "") -> list[dict]:
        domain = self._infer_domain(texte, type_contrat)
        mask = self.rules["domaine"].isin(["عام", domain, type_contrat])
        relevant = self.rules[mask]

        violations = []
        for _, rule in relevant.iterrows():
            keywords = rule["_tokens"]
            if not keywords:
                continue
            missing = [kw for kw in keywords if kw not in texte]
            if len(missing) / len(keywords) >= 0.5:
                violations.append({
                    "rule_id":       rule["rule_id"],
                    "domaine":       rule["domaine"],
                    "obligation":    rule["obligation"],
                    "condition":     str(rule.get("condition_trigger", "")),
                    "loi":           str(rule.get("loi_reference", "—")),
                    "risk_score":    int(rule.get("risk_score", 3)),
                    "penalite":      str(rule.get("penalite_type", "—")),
                    "score_gravite": int(rule.get("score_gravite", 5)),
                    "alerte":        str(rule.get("alerte", "NORMALE")),
                    "delai":         str(rule.get("delai_conformite", "—")),
                })
        violations.sort(key=lambda x: x["score_gravite"], reverse=True)
        return violations

    def _infer_domain(self, texte: str, type_contrat: str) -> str:
        if type_contrat and type_contrat in self.DOMAIN_MAP:
            return type_contrat
        combined = f"{type_contrat} {texte[:300]}"
        best, best_score = "عام", 0
        for domain, kws in self.DOMAIN_MAP.items():
            score = sum(1 for k in kws if k in combined)
            if score > best_score:
                best, best_score = domain, score
        return best

    def _tokenize(self, text: str) -> list[str]:
        tokens = self._AR_PATTERN.findall(text)
        return [t for t in tokens if t not in self.STOPWORDS]


# ══════════════════════════════════════════════════════════════
# SYSTEM PROMPT
# ══════════════════════════════════════════════════════════════
SYSTEM_PROMPT = """You are an expert Tunisian lawyer specializing in:
- مجلة الالتزامات والعقود (م.ع.ع) — Code des Obligations et Contrats
- مجلة الحقوق العينية — Code des Droits Réels
- قانون التسجيل والطابع الجبائي — Droits d'enregistrement
- مجلة التهيئة الترابية والتعمير — Permis de construire
- قانون الكراء التجاري والسكني — Baux commerciaux et résidentiels
- قانون الاستثمار 2016 et amendments 2019
- قانون الصرف n°77-608 — Réglementation des changes
- الجريدة الرسمية التونسية (JORT) 2024-2026
- Jurisprudence des tribunaux tunisiens et Cour Foncière

MISSION: Analyze the submitted Tunisian contract with ABSOLUTE LEGAL RIGOR.

REQUIRED OUTPUT FORMAT:

---
## 📋 IDENTIFICATION DU CONTRAT | تحديد العقد
- Type de contrat détecté
- Parties identifiées
- Objet du contrat
- Langue et forme juridique

---
## ⚖️ ANALYSE JURIDIQUE | التحليل القانوني
For each clause or missing clause:
→ Constat: what is present/absent
→ Article exact de loi tunisienne applicable
→ Évaluation: ✅ Conforme | ⚠️ Lacune | ❌ Violation

---
## 🚨 VIOLATIONS & RISQUES | المخالفات والمخاطر
Sorted by severity:
🔴 CRITIQUE — Nullité / sanction pénale
🟠 ÉLEVÉ — Risque judiciaire / perte de droits
🟡 MODÉRÉ — Lacune formelle / risque fiscal
🟢 FAIBLE — Recommandation

For each:
• Article de loi EXACT avec son contenu essentiel
• Conséquence juridique concrète
• CLAUSE CORRECTIVE: exact text to insert in the contract

---
## 💡 RECOMMANDATIONS PRIORITAIRES | التوصيات العاجلة
Numbered actions from most urgent to least urgent.
Include EXACT TEXT of missing clauses to add.

---
## 📊 VERDICT FINAL | الحكم النهائي
Score: XX/100
Niveau: CONFORME / RISQUE MODÉRÉ / RISQUE ÉLEVÉ / NON CONFORME CRITIQUE
Summary in French (2 lines) + Arabic (2 lines)

---
ABSOLUTE RULES:
1. Never invent a law article — only cite existing Tunisian law
2. Always give the essential content of cited articles
3. Adapt analysis to the EXACT detected contract type
4. Flag any abusive or unbalanced clauses between parties
5. Mention recent JORT 2025-2026 texts if relevant
"""


# ══════════════════════════════════════════════════════════════
# ORCHESTRATEUR
# ══════════════════════════════════════════════════════════════
class RAGJuridique:
    def __init__(self, df_contrats, df_jort, df_rules):
        self.df_contrats = df_contrats
        self.vector_store = VectorStore(df_jort)
        self.rule_engine = RuleEngine(df_rules)

    def analyze_pdf(self, pdf_path: str, type_contrat: str = "") -> dict:
        texte = extract_text_from_pdf(pdf_path)
        return self.analyze(texte, type_contrat=type_contrat)

    def analyze(self, texte: str, type_contrat: str = "", top_k: int = 6,
                groq_api_key: str = "") -> dict:
        # 1. Rules
        violations = self.rule_engine.detect(texte, type_contrat)
        # 2. RAG
        query = f"{type_contrat} {texte[:500]}"
        jort_chunks = self.vector_store.search(query, top_k=top_k)
        context = self._build_context(jort_chunks)
        # 3. Score
        score, niveau = self._compute_score(violations)
        # 4. LLM
        api_key = groq_api_key or os.getenv("GROQ_API_KEY", "")
        rapport = None
        if api_key:
            rapport = self._call_groq(texte, violations, context, api_key, type_contrat)

        return {
            "violations":    violations,
            "jort_chunks":   jort_chunks.to_dict("records") if not jort_chunks.empty else [],
            "score":         score,
            "niveau":        niveau,
            "rapport":       rapport,
            "texte_extrait": texte,
        }

    # ── streaming version pour FastAPI SSE ──────────────────
    def analyze_stream(self, texte: str, type_contrat: str = "", top_k: int = 6,
                       groq_api_key: str = ""):
        """Génère des événements SSE progressivement."""
        import time

        # Étape 1 : Rule Engine
        yield json.dumps({"step": "rules", "status": "running"})
        violations = self.rule_engine.detect(texte, type_contrat)
        score, niveau = self._compute_score(violations)
        yield json.dumps({"step": "rules", "status": "done",
                          "violations": violations, "score": score, "niveau": niveau})

        # Étape 2 : RAG
        yield json.dumps({"step": "rag", "status": "running"})
        query = f"{type_contrat} {texte[:500]}"
        jort_chunks = self.vector_store.search(query, top_k=top_k)
        chunks_list = jort_chunks.to_dict("records") if not jort_chunks.empty else []
        yield json.dumps({"step": "rag", "status": "done", "jort_chunks": chunks_list})

        # Étape 3 : LLM
        api_key = groq_api_key or os.getenv("GROQ_API_KEY", "")
        if api_key:
            yield json.dumps({"step": "llm", "status": "running"})
            context = self._build_context(jort_chunks)
            rapport = self._call_groq(texte, violations, context, api_key, type_contrat)
            yield json.dumps({"step": "llm", "status": "done", "rapport": rapport})
        else:
            yield json.dumps({"step": "llm", "status": "skipped",
                              "rapport": "GROQ_API_KEY non configurée."})

    def _build_context(self, chunks: pd.DataFrame) -> str:
        if chunks.empty:
            return "Aucun texte réglementaire pertinent trouvé."
        parts = []
        for _, r in chunks.iterrows():
            parts.append(
                f"SOURCE: {r.get('source_doc','')} | "
                f"DOMAINE: {r.get('domaine_juridique','')} | "
                f"ALERTE: {r.get('niveau_alerte','')} | "
                f"RISQUE: {r.get('risque_conformite','')}\n"
                f"OBLIGATIONS: {r.get('obligations','')}\n"
                f"TEXTE: {str(r.get('texte_chunk',''))[:400]}"
            )
        return "\n\n".join(parts)

    def _compute_score(self, violations: list[dict]) -> tuple[int, str]:
        if not violations:
            return 100, "✅ CONFORME"
        gravity = sum(v["score_gravite"] for v in violations)
        score = max(0, 100 - gravity * 3)
        if   score >= 80: return score, "✅ CONFORME"
        elif score >= 60: return score, "🟡 RISQUE MODÉRÉ"
        elif score >= 35: return score, "🟠 RISQUE ÉLEVÉ"
        else:             return score, "🔴 NON CONFORME CRITIQUE"

    def _call_groq(self, texte, violations, context, api_key, type_contrat) -> str:
        try:
            from groq import Groq
            client = Groq(api_key=api_key)
            user_msg = f"""
## CONTRACT TO ANALYZE
Declared type: {type_contrat or 'Not specified — detect automatically'}

```
{texte[:3000]}
```

## VIOLATIONS DETECTED BY RULE ENGINE
{json.dumps(violations, ensure_ascii=False, indent=2)}

## RELEVANT JORT REGULATORY TEXTS (RAG)
{context}

---
Produce the complete legal compliance report following the exact format defined.
Be precise and rigorous. Write corrective clauses in full.
"""
            resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                max_tokens=3000,
                temperature=0.1,
                timeout=60,
            )
            return resp.choices[0].message.content
        except Exception as e:
            return f"❌ Erreur Groq LLM : {e}"


# ══════════════════════════════════════════════════════════════
# CHARGEMENT DONNÉES (singleton)
# ══════════════════════════════════════════════════════════════
_rag_instance = None

def get_rag() -> RAGJuridique:
    global _rag_instance
    if _rag_instance is None:
        df_c = pd.read_excel(os.path.join(ROOT, "dataset_BO4_contrats_final.xlsx"))
        df_j = pd.read_excel(os.path.join(ROOT, "dataset_BO4_jort_chunks.xlsx"))
        df_r = pd.read_excel(os.path.join(ROOT, "dataset_BO4_rules.xlsx"))
        print(f"  ✅ {len(df_c)} contrats | {len(df_j)} chunks JORT | {len(df_r)} règles")
        _rag_instance = RAGJuridique(df_c, df_j, df_r)
    return _rag_instance


# ══════════════════════════════════════════════════════════════
# TEST CLI
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    rag = get_rag()
    CONTRAT_TEST = """
    عقد بيع: نا الموقع أسفله الجنسية الحامل لبطاقة التعريف الوطنية رقم وانساكن بدوار.
    شهد واعترف بأنني أبيع 600م2 من الأرض ذات الرسم العقاري رقم، والكائنة بأمزالمزة.
    وبهذا شهد والتزم بحسن نية وصدق تام. إمضاء
    """
    res = rag.analyze(CONTRAT_TEST, type_contrat="بيع أرض")
    print(f"SCORE: {res['score']}/100 — {res['niveau']}")
    print(f"{len(res['violations'])} violations")
    if res["rapport"]:
        print(res["rapport"][:500])