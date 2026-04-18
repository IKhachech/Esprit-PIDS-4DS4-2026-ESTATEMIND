"""
BO4/rag_juridique/app.py
=========================
Interface Gradio — RAG Juridique Tunisien
Upload PDF + Rule Engine + Rapport LLM via Groq (LLaMA 3.3 70B)
"""

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import gradio as gr
from dotenv import load_dotenv
from rag_pipeline import RAGJuridique, load_data, extract_text_from_pdf

load_dotenv(os.path.join(ROOT, ".env"))

ICONS = {"CRITIQUE": "🔴", "ÉLEVÉ": "🟠", "NORMALE": "🟡"}
TYPES = [
    "", "بيع عقاري", "بيع أرض", "كراء سكني",
    "كراء تجاري", "كراء مختلط", "وعد بالبيع",
    "وكالة عقارية", "بيع منقول",
]

# ── Init globale ───────────────────────────────────────────────
print("⏳ Chargement des données et initialisation du RAG...")
df_contrats, df_jort, df_rules = load_data()
rag = RAGJuridique(df_contrats, df_jort, df_rules)
print("✅ RAG Juridique prêt.\n")


# ══════════════════════════════════════════════════════════════
# FONCTION D'ANALYSE
# ══════════════════════════════════════════════════════════════
def analyser(pdf_file, texte_manuel, type_contrat):

    # ── Source du texte ────────────────────────────────────────
    if pdf_file is not None:
        texte = extract_text_from_pdf(pdf_file)
        nb_mots = len(texte.split())
        source_info = f"📄 **PDF chargé** — {nb_mots} mots extraits"
    elif texte_manuel and texte_manuel.strip():
        texte = texte_manuel.strip()
        source_info = f"✍️ **Texte saisi** — {len(texte.split())} mots"
    else:
        msg = "⚠️ Veuillez uploader un PDF ou coller le texte du contrat."
        return msg, "", "", "", ""

    if texte.startswith("[Erreur") or texte.startswith("[PDF"):
        return texte, "", "", "", ""

    # ── Analyse ────────────────────────────────────────────────
    result = rag.analyze(texte, type_contrat=type_contrat)

    # ── ONGLET 1 : Aperçu texte extrait ───────────────────────
    apercu = (
        f"{source_info}\n\n---\n\n"
        f"**Aperçu du texte :**\n\n"
        f"```\n{texte[:1000]}{'...' if len(texte) > 1000 else ''}\n```"
    )

    # ── ONGLET 2 : Score ───────────────────────────────────────
    score  = result["score"]
    niveau = result["niveau"]
    nb_v   = len(result["violations"])
    nb_c   = len(result["jort_chunks"])

    # Barre de progression textuelle
    filled = int(score / 10)
    bar    = "█" * filled + "░" * (10 - filled)
    color  = "🟢" if score >= 80 else "🟡" if score >= 60 else "🟠" if score >= 35 else "🔴"

    score_md = f"""## {color} Score de conformité : {score}/100

`{bar}` **{score}/100**

### {niveau}

| Indicateur | Valeur |
|---|---|
| Score de conformité | **{score} / 100** |
| Niveau de risque | **{niveau}** |
| Violations détectées | **{nb_v}** règles non respectées |
| Articles JORT récupérés | **{nb_c}** textes réglementaires |
| Type analysé | **{type_contrat or 'Auto-détecté'}** |
| LLM utilisé | **Groq — LLaMA 3.3 70B** |
"""

    # ── ONGLET 3 : Violations ──────────────────────────────────
    viols = result["violations"]
    if viols:
        rows = [
            "| # | Alerte | Obligation manquante | Loi applicable | Gravité | Pénalité |",
            "|---|--------|---------------------|---------------|---------|----------|",
        ]
        for i, v in enumerate(viols, 1):
            icon = ICONS.get(v["alerte"], "⚪")
            rows.append(
                f"| {i} | {icon} **{v['alerte']}** | {v['obligation']} "
                f"| `{v['loi']}` | **{v['score_gravite']}/10** | {v['penalite']} |"
            )
        nb_crit = sum(1 for v in viols if v["alerte"] == "CRITIQUE")
        nb_elev = sum(1 for v in viols if v["alerte"] == "ÉLEVÉ")
        viols_md = "\n".join(rows)
        viols_md += f"\n\n> 🔴 **{nb_crit} CRITIQUE** · 🟠 **{nb_elev} ÉLEVÉ** · {nb_v} total"
    else:
        viols_md = "## ✅ Aucune violation détectée\nLe contrat respecte les 25 règles métier analysées."

    # ── ONGLET 4 : JORT ───────────────────────────────────────
    chunks = result["jort_chunks"]
    if chunks:
        parts = []
        for c in chunks:
            sim  = c.get("similarity", 0)
            bars = "█" * min(5, int(sim * 50)) + "░" * (5 - min(5, int(sim * 50)))
            parts.append(
                f"### 📌 {c.get('source_doc', '—')}\n"
                f"| Domaine | Risque | Alerte | Pertinence |\n"
                f"|---------|--------|--------|------------|\n"
                f"| {c.get('domaine_juridique','')} | **{c.get('risque_conformite','')}** "
                f"| **{c.get('niveau_alerte','')}** | {bars} {sim:.0%} |\n\n"
                f"**Obligations :** _{c.get('obligations','')}_\n\n"
                f"> {str(c.get('texte_chunk',''))[:300]}..."
            )
        context_md = "\n\n---\n\n".join(parts)
    else:
        context_md = "_Aucun article réglementaire JORT pertinent trouvé._"

    # ── ONGLET 5 : Rapport LLM ────────────────────────────────
    if result.get("rapport"):
        rapport_md = result["rapport"]
    elif not os.getenv("GROQ_API_KEY"):
        rapport_md = (
            "### ⚙️ Rapport LLM non généré\n\n"
            "Votre fichier `BO4/.env` doit contenir :\n\n"
            "```\nGROQ_API_KEY=gsk_...\n```\n\n"
            "Le moteur Rule-based et le RAG JORT fonctionnent sans clé."
        )
    else:
        rapport_md = "❌ Erreur lors de la génération du rapport LLM."

    return apercu, score_md, viols_md, context_md, rapport_md


# ══════════════════════════════════════════════════════════════
# INTERFACE GRADIO
# ══════════════════════════════════════════════════════════════
groq_status = (
    "🟢 **Groq API connectée** — LLaMA 3.3 70B activé"
    if os.getenv("GROQ_API_KEY")
    else "🔴 **GROQ_API_KEY absente** — ajoutez-la dans BO4/.env"
)

with gr.Blocks(
    title="⚖️ RAG Juridique Tunisien — BO4",
    theme=gr.themes.Soft(primary_hue="blue", neutral_hue="slate"),
) as demo:

    gr.Markdown(f"""
# ⚖️ RAG Juridique Tunisien — BO4
### Analyse automatique de conformité · *مجلة الالتزامات والعقود · JORT · مجلة الحقوق العينية*
{groq_status}
""")

    with gr.Row():

        # ── Colonne gauche : Inputs ────────────────────────────
        with gr.Column(scale=1, min_width=300):
            gr.Markdown("### 📥 Entrée du contrat")

            with gr.Tabs():
                with gr.Tab("📄 Upload PDF"):
                    pdf_input = gr.File(
                        label="Déposer le contrat PDF ici",
                        file_types=[".pdf"],
                        type="filepath",
                    )
                    gr.Markdown("*PDF avec couche texte requis (arabe / français / bilingue)*")

                with gr.Tab("✍️ Texte manuel"):
                    texte_input = gr.Textbox(
                        label="Coller le texte du contrat",
                        placeholder="عقد بيع: ...\n\nContrat de vente : ...",
                        lines=12,
                    )

            type_select = gr.Dropdown(
                label="📂 Type de contrat (optionnel — auto-détecté)",
                choices=TYPES,
                value="",
            )

            analyze_btn = gr.Button(
                "🔍  Analyser le contrat",
                variant="primary",
                size="lg",
            )

            gr.Examples(
                examples=[
                    [
                        None,
                        "عقد بيع: نا الموقع أسفله الجنسية الحامل لبطاقة التعريف الوطنية رقم وانساكن بدوار. "
                        "شهد واعترف بأنني أبيع 600م2 من الأرض. وبهذا شهد والتزم. إمضاء",
                        "بيع أرض",
                    ],
                    [
                        None,
                        "عقد كراء: يكري المكري للمكتري الشقة الكائنة بنهج الحرية عدد 12 تونس. "
                        "مدة الكراء سنة واحدة. وبهذا أمضى الطرفان.",
                        "كراء سكني",
                    ],
                    [
                        None,
                        "عقد وعد بالبيع: وعد البائع ببيع العقار الكائن بشارع الحبيب بورقيبة. "
                        "وقع المشتري عربونا بمبلغ 5000 دينار.",
                        "وعد بالبيع",
                    ],
                ],
                inputs=[pdf_input, texte_input, type_select],
                label="📋 Exemples",
            )

        # ── Colonne droite : Outputs ───────────────────────────
        with gr.Column(scale=2):
            with gr.Tabs():
                with gr.Tab("📄 Texte extrait"):
                    apercu_out  = gr.Markdown("_En attente d'un contrat..._")
                with gr.Tab("📊 Score"):
                    score_out   = gr.Markdown("_En attente d'analyse..._")
                with gr.Tab("⚠️ Violations"):
                    viols_out   = gr.Markdown("_En attente d'analyse..._")
                with gr.Tab("📜 JORT (RAG)"):
                    context_out = gr.Markdown("_En attente d'analyse..._")
                with gr.Tab("🤖 Rapport LLM"):
                    rapport_out = gr.Markdown("_En attente d'analyse..._")

    analyze_btn.click(
        fn=analyser,
        inputs=[pdf_input, texte_input, type_select],
        outputs=[apercu_out, score_out, viols_out, context_out, rapport_out],
    )


# ══════════════════════════════════════════════════════════════
# LANCEMENT  →  ouvrir http://localhost:7860
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 Lancement de l'interface...")
    print("👉 Ouvrez votre navigateur sur : http://localhost:7860\n")
    demo.launch(
        server_name="127.0.0.1",   # localhost uniquement
        server_port=7860,
        show_error=True,
        inbrowser=True,            # ouvre automatiquement le navigateur
    )