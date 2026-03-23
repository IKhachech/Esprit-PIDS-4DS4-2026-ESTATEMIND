"""
cleaning_BO4.py — ETL, nettoyage et augmentation des données pour l'Objectif 4.

Sources traitées :
  1. dataset_contrats_BO4_final.csv    — 550 contrats (30 originaux + jumeaux)
  2. dataset_conformite_immobilier_tunisien.csv — 5,784 textes JORT
  3. PDFs lois tunisiennes (si disponibles)

Étapes :
  [1] Chargement contrats + JORT
  [2] Nettoyage texte arabe (normalisation Unicode, ponctuation)
  [3] Détection automatique type/clauses/risques depuis texte
  [4] Validation cohérence Score → Label
  [5] Déduplication
  [6] Encodage colonnes catégorielles
"""

import re, os, warnings
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

from mappings_BO4 import (
    TYPE_CONTRAT_ENC, SOUS_TYPE_ENC, LABEL_ENC,
    ALERTE_ENC, STATUT_CONFORMITE_ENC,
    LOI_PAR_DOMAINE, CLAUSES_OBLIGATOIRES,
)

# ================================================================
# HELPERS TEXTE ARABE
# ================================================================

def normalize_arabic(text: str) -> str:
    """Normalise le texte arabe : Unicode, ponctuation, espaces."""
    if not isinstance(text, str): return ''
    # Normalisation Unicode arabe
    text = re.sub(r'[أإآ]', 'ا', text)
    text = re.sub(r'ة', 'ه', text)
    text = re.sub(r'ى', 'ي', text)
    # Supprimer tashkil (voyelles courtes)
    text = re.sub(r'[\u064B-\u065F]', '', text)
    # Espaces multiples
    text = re.sub(r'\s+', ' ', text)
    # Ponctuation inutile
    text = re.sub(r'[\.]{2,}', '.', text)
    text = re.sub(r'[\r\n]+', ' ', text)
    return text.strip()


def detect_type_contrat(texte: str, description: str = '') -> tuple:
    """Détecte le type de contrat depuis le texte arabe."""
    t = str(texte) + str(description)
    t_norm = normalize_arabic(t).lower()

    if 'وعد بالبيع' in t or 'وعد بالبيع' in t_norm:
        return 'وعد بالبيع', 'وعد'
    if 'وكاله' in t_norm or 'وكالة' in t:
        return 'وكالة عقارية', 'وكالة'
    if 'بيع عقار مبني' in t or ('بيع' in t and 'مبني' in t):
        return 'بيع عقار مبني', 'بيع'
    if 'بيع' in t and ('ارض' in t_norm or 'قطعه' in t_norm or 'فلاح' in t_norm or 'أرض' in t):
        return 'بيع أرض', 'بيع'
    if 'بيع' in t and ('ابقار' in t_norm or 'ماشيه' in t_norm or 'بومنيار' in t_norm):
        return 'بيع منقول', 'بيع'
    if 'بيع' in t:
        return 'بيع عقاري', 'بيع'
    if 'كراء' in t or 'تسويغ' in t or 'ايجار' in t_norm:
        if 'سكني' in t or 'سكنى' in t or 'مسكن' in t:
            return 'كراء سكني', 'كراء'
        if 'تجاري' in t or 'تجارة' in t or 'محل' in t:
            return 'كراء تجاري', 'كراء'
        if 'مهني' in t or 'مكتب' in t or 'شركة' in t:
            return 'كراء مهني', 'كراء'
        return 'كراء مختلط', 'كراء'
    return 'عقد غير مصنف', 'أخرى'


def detect_clauses(texte: str, sous_type: str) -> tuple:
    """
    Détecte clauses présentes et manquantes depuis le texte arabe.
    Retourne (clauses_presentes: str, clauses_manquantes: str)
    """
    t = str(texte)

    CLAUSES_KEYWORDS = {
        'تعريف الأطراف':      ['المسوغ', 'المتسوغ', 'البائع', 'المشتري', 'الواعد', 'الوكيل', 'الممضين'],
        'موضوع العقد':        ['المحل الكائن', 'قطعة الأرض', 'العقار', 'المنزل', 'المحل'],
        'المدة':               ['لمدة', 'سنة', 'أشهر', 'مدة التسويغ', 'تبتدئ', 'تنتهي'],
        'الثمن/المقدار':       ['مقدار', 'دينار', 'ثمن', 'مبلغ', 'درهم'],
        'الدفع':               ['دفع', 'مسبقا', 'دفعة', 'تسبيق', 'عربون', 'مقابل وصل'],
        'التسليم':             ['تسلم', 'تسليم', 'حالة حسنة', 'لوازمه', 'مفاتيح'],
        'الضمان/الكفالة':      ['ضمان', 'كفالة', 'مبلغ ضمان'],
        'الفسخ':               ['فسخ', 'وجوبا', 'فسخ هذا العقد', 'ينفسخ'],
        'الإشعار المسبق':      ['إشعار', 'إعلام', 'شهرين', 'شهر على الأقل', 'تنبيه'],
        'التسويغ للغير':       ['للغير', 'إعارته', 'التسويغ الفرعي'],
        'المعاليم/الضرائب':    ['معاليم الشؤون', 'الاداءات البلدية', 'ضريبة', 'أداء'],
        'الإصلاحات':           ['إصلاحات', 'تحسينات', 'تغيير بالمحل'],
        'ماء/كهرباء':          ['الماء والكهرباء', 'عدادين', 'فواتير', 'استهلاك'],
        'الكرامة':             ['غير لائق بالكرامة', 'السيرة السيئة'],
        'التفقد':              ['تفقد المحل', 'من ينوبه'],
        'الترفيع':             ['الترفيع', 'ترفيع في الايجار'],
        'الشهود':              ['شاهد', 'شاهدين', 'شهود'],
        'التسجيل الرسمي':      ['تسجيل', 'كاتب عدل', 'قباضة التسجيل', 'الشهر العقاري'],
        'خلو من الرهن':        ['خلو', 'رهن', 'خالص', 'قيد'],
        'العربون':             ['عربون', 'تسبيق'],
        'أجل البيع النهائي':   ['البيع النهائي', 'أجل', 'لا يتجاوز موعد'],
        'الصلاحيات':          ['صلاحيات', 'التصرف الكامل', 'التفويض'],
        'شروط الإلغاء':        ['إلغاء الوكالة', 'شروط الإلغاء', 'إلغاء هذا العقد'],
        'أجل الوكالة':         ['أجل الوكالة', 'مدة الوكالة', 'لمدة سنة'],
    }

    presentes  = [c for c, kws in CLAUSES_KEYWORDS.items() if any(kw in t for kw in kws)]
    obligatoires = CLAUSES_OBLIGATOIRES.get(sous_type, CLAUSES_OBLIGATOIRES.get('كراء', []))
    manquantes = [c for c in obligatoires if c not in presentes]

    cp = '،'.join(presentes) if presentes else 'لم يتم الكشف عن بنود'
    cm = '،'.join(manquantes) if manquantes else 'لا توجد بنود مفقودة — عقد مكتمل'
    return cp, cm


def detect_risque_principal(clauses_manquantes: str, label: str) -> str:
    """Retourne le risque principal depuis les clauses manquantes."""
    cm = str(clauses_manquantes)
    if 'التسجيل الرسمي' in cm: return 'عقد غير مسجل — لا قيمة قانونية في مواجهة الغير'
    if 'الثمن' in cm:           return 'غياب الثمن — بطلان العقد'
    if 'الفسخ' in cm:           return 'غياب بند الفسخ — تعقيد إجراءات الإخلاء'
    if 'الضمان' in cm or 'الكفالة' in cm: return 'غياب الكفالة — خطر مالي عند الإخلال'
    if 'الشهود' in cm:          return 'غياب الشهود — قابل للطعن قانونياً'
    if 'الإشعار' in cm:         return 'غياب الإشعار المسبق — خطر إخلاء فوري'
    if 'خلو من الرهن' in cm:    return 'لا إثبات خلو الرهن — خطر الاستحقاق'
    if 'أجل البيع النهائي' in cm: return 'لا أجل محدد للبيع النهائي — خطر التراجع'
    if label == 'Critique':      return 'عقد ناقص جداً — خطر بطلان'
    if label == 'Élevé':         return 'بنود جوهرية مفقودة — مراجعة قانونية ضرورية'
    if label == 'Moyen':         return 'بنود ثانوية مفقودة — يُنصح بالمراجعة'
    return 'لا مخاطر جوهرية مكتشفة'


def score_to_label(score: int) -> tuple:
    """Convertit score risque en label."""
    if score <= 2:  return 'Faible',   'منخفض', '🟢'
    if score <= 5:  return 'Moyen',    'متوسط', '🟡'
    if score <= 8:  return 'Élevé',    'مرتفع', '🔴'
    return 'Critique', 'حرج', '⛔'


def score_to_alerte(score: int, label: str) -> str:
    if score >= 9 or label == 'Critique': return 'CRITIQUE'
    if score >= 5 or label == 'Élevé':   return 'ÉLEVÉ'
    return 'NORMALE'


def score_to_statut(label: str) -> str:
    mapping = {
        'Faible':   'مطابق — عقد مكتمل',
        'Moyen':    'مطابق جزئياً — يحتاج مراجعة',
        'Élevé':    'غير مطابق — مخاطر مرتفعة',
        'Critique': 'غير مطابق — خطر قانوني حرج',
    }
    return mapping.get(label, 'غير محدد')


# ================================================================
# SECTION — chargement données
# ================================================================

def _log(msg: str) -> None:
    print(f"  {msg}")


def load_contrats(path: str) -> pd.DataFrame:
    """Charge et normalise le dataset des contrats."""
    print("\n" + "=" * 65)
    print("   ETAPE 1 — CHARGEMENT CONTRATS")
    print("=" * 65)

    if not os.path.exists(path):
        _log(f"[WARN] Fichier absent : {path}")
        return pd.DataFrame()

    df = pd.read_csv(path, encoding='utf-8-sig', on_bad_lines='skip')
    df = df.fillna('')
    _log(f"✔ Contrats chargés : {len(df):,} lignes × {len(df.columns)} colonnes")

    # Normaliser texte
    df['Texte_Normalise'] = df['Texte_Contrat'].apply(normalize_arabic)
    _log(f"  Texte normalisé (Unicode arabe)")

    return df


def load_jort(path: str, max_rows: int = 6000) -> pd.DataFrame:
    """
    Charge les textes JORT pour la base RAG.

    CORRECTION APPLIQUÉE :
    - Problème avant : texte_chunk = titre seul → chunks de 5 mots → inutilisables
    - Solution : construire texte_chunk depuis 6 colonnes combinées
    - Filtre chunks < 8 mots (trop courts pour créer un embedding utile)
    - Résultat : 716 → 1,778 chunks valides
    """
    print("\n" + "=" * 65)
    print("   ETAPE 1b — CHARGEMENT TEXTES JORT")
    print("=" * 65)

    if not os.path.exists(path):
        _log(f"[WARN] JORT absent : {path}")
        return pd.DataFrame()

    df = pd.read_csv(path, sep=';', encoding='utf-8-sig', on_bad_lines='skip')
    df = df.fillna('')

    # Garder toutes les colonnes utiles pour construire le texte_chunk
    cols_utiles = ['id', 'source', 'type_texte', 'ministere',
                   'domaine_juridique', 'risque_conformite',
                   'obligations_legales', 'penalites_potentielles',
                   'contenu_resume', 'titre', 'type_operation',
                   'textes_references']
    cols_dispo = [c for c in cols_utiles if c in df.columns]
    df = df[cols_dispo].head(max_rows)

    # ── CORRECTION 1 : construire texte_chunk depuis plusieurs colonnes ──
    # Avant : f"{titre} — {obligations} — {contenu_resume}"  → souvent < 5 mots
    # Après : combinaison de 6 champs → chunk riche et suffisamment long
    def build_chunk(row):
        parties = []
        if str(row.get('obligations_legales', '')).strip():
            parties.append(f"الالتزامات: {row['obligations_legales'].strip()}")
        if str(row.get('domaine_juridique', '')).strip():
            parties.append(f"المجال: {row['domaine_juridique'].strip()}")
        if str(row.get('type_operation', '')).strip():
            parties.append(f"نوع العملية: {row['type_operation'].strip()}")
        if str(row.get('titre', '')).strip():
            parties.append(f"العنوان: {row['titre'].strip()}")
        if str(row.get('penalites_potentielles', '')).strip():
            parties.append(f"العقوبات: {row['penalites_potentielles'].strip()}")
        if str(row.get('contenu_resume', '')).strip():
            parties.append(row['contenu_resume'].strip())
        return ' — '.join(parties)

    df['texte_chunk'] = df.apply(build_chunk, axis=1)
    df['texte_chunk'] = df['texte_chunk'].apply(normalize_arabic)
    df['nb_mots']     = df['texte_chunk'].apply(lambda x: len(str(x).split()))

    # ── CORRECTION 1b : filtrer chunks trop courts (< 8 mots inutilisables pour RAG) ──
    avant_filtre = len(df)
    df = df[df['nb_mots'] >= 8].reset_index(drop=True)
    _log(f"Chunks < 8 mots filtrés : {avant_filtre - len(df)} supprimés")
    _log(f"✔ JORT chargé : {len(df):,} chunks valides (≥ 8 mots)")
    _log(f"  Nb mots : min={df['nb_mots'].min()} max={df['nb_mots'].max()} mean={df['nb_mots'].mean():.0f}")
    return df


# ================================================================
# SECTION — nettoyage et validation
# ================================================================

def deduplicate(df: pd.DataFrame) -> tuple:
    """Supprime les doublons exacts sur Texte_Normalise."""
    print("\n" + "=" * 65)
    print("   ETAPE 2 — DÉDUPLICATION")
    print("=" * 65)

    avant = len(df)
    df = df.drop_duplicates(subset=['Texte_Normalise']).reset_index(drop=True)
    apres = len(df)
    _log(f"Passe 1 — doublons exacts : -{avant - apres} supprimés")
    _log(f"Total après dédup : {apres:,}")
    return df, avant


def validate_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valide et enrichit toutes les colonnes :
    - détecte type_contrat depuis texte si absent
    - recalcule clauses_presentes/manquantes
    - recalcule Label depuis Score
    - recalcule statut_conformite, niveau_alerte, alerte_generee
    """
    print("\n" + "=" * 65)
    print("   ETAPE 3 — VALIDATION ET ENRICHISSEMENT")
    print("=" * 65)

    # Convertir colonnes bool en object pour éviter TypeError
    for col_bool in ['alerte_generee']:
        if col_bool in df.columns:
            df[col_bool] = df[col_bool].astype(object)

    for idx, row in df.iterrows():
        texte = str(row.get('Texte_Contrat', ''))
        desc  = str(row.get('Description', ''))
        score = int(row['Score']) if str(row.get('Score', '0')).replace('.', '').lstrip('-').isdigit() else 0

        # type_contrat
        if str(row.get('type_contrat', '')).strip() in ['', 'nan', 'عقد غير مصنف']:
            tc, st = detect_type_contrat(texte, desc)
            df.at[idx, 'type_contrat'] = tc
            df.at[idx, 'sous_type']    = st

        sous_type = str(df.at[idx, 'sous_type'])

        # clauses
        if str(row.get('clauses_presentes', '')).strip() in ['', 'nan', 'لم يتم الكشف عن بنود']:
            cp, cm = detect_clauses(texte, sous_type)
            df.at[idx, 'clauses_presentes']  = cp
            df.at[idx, 'clauses_manquantes'] = cm

        # nb clauses
        cp_val = str(df.at[idx, 'clauses_presentes'])
        cm_val = str(df.at[idx, 'clauses_manquantes'])
        df.at[idx, 'nb_clauses_presentes']  = len(cp_val.split('،')) if cp_val and cp_val != 'nan' else 0
        df.at[idx, 'nb_clauses_manquantes'] = (
            0 if 'مكتمل' in cm_val
            else len(cm_val.split('،')) if cm_val and cm_val != 'nan' else 0
        )

        # label depuis score
        label_fr, label_ar, emoji = score_to_label(score)
        df.at[idx, 'Label_FR']  = label_fr
        df.at[idx, 'Label_AR']  = label_ar
        df.at[idx, 'Emoji']     = emoji

        # alerte
        alerte_val = score_to_alerte(score, label_fr)
        df.at[idx, 'niveau_alerte']  = alerte_val
        df.at[idx, 'alerte_generee'] = 'True' if alerte_val in ['CRITIQUE', 'ÉLEVÉ'] else 'False'

        # statut_conformite
        df.at[idx, 'statut_conformite'] = score_to_statut(label_fr)

        # risque_principal
        df.at[idx, 'risque_principal'] = detect_risque_principal(cm_val, label_fr)

        # loi_reference
        if str(row.get('loi_reference', '')).strip() in ['', 'nan']:
            tc = str(df.at[idx, 'type_contrat'])
            df.at[idx, 'loi_reference'] = LOI_PAR_DOMAINE.get(tc, 'م.ع.ع — مجلة الالتزامات والعقود')

        # domaine_juridique
        if str(row.get('domaine_juridique', '')).strip() in ['', 'nan']:
            tc = str(df.at[idx, 'type_contrat'])
            df.at[idx, 'domaine_juridique'] = tc.replace('كراء سكني', 'كراء سكني').replace('بيع أرض', 'بيع عقاري')

        # Nb_Mots / Nb_Caracteres
        df.at[idx, 'Nb_Mots']       = len(texte.split())
        df.at[idx, 'Nb_Caracteres'] = len(texte)

    # Statistiques
    _log(f"✔ Validation terminée : {len(df):,} contrats")
    if 'Label_FR' in df.columns and len(df) > 0:
        _log(f"  Distribution Label_FR :")
        for label, cnt in df['Label_FR'].value_counts().items():
            _log(f"    {label:<12}: {cnt:>4} ({cnt/len(df)*100:.1f}%)")
        _log(f"  Types contrats :")
        for tc, cnt in df['type_contrat'].value_counts().head(5).items():
            _log(f"    {tc:<25}: {cnt:>4}")

    return df


def encode_categorical(df: pd.DataFrame) -> pd.DataFrame:
    """Encode les colonnes catégorielles en entiers pour ML."""
    print("\n" + "=" * 65)
    print("   ETAPE 4 — ENCODAGE CATÉGORIEL")
    print("=" * 65)

    df['type_contrat_enc']    = df['type_contrat'].map(TYPE_CONTRAT_ENC).fillna(99).astype(int)
    df['sous_type_enc']       = df['sous_type'].map(SOUS_TYPE_ENC).fillna(9).astype(int)
    df['label_enc']           = df['Label_FR'].map(LABEL_ENC).fillna(0).astype(int)
    df['alerte_enc']          = df['niveau_alerte'].map(ALERTE_ENC).fillna(0).astype(int)
    df['statut_enc']          = df['statut_conformite'].map(STATUT_CONFORMITE_ENC).fillna(0).astype(int)
    df['alerte_generee_bool'] = df['alerte_generee'].apply(lambda x: 1 if str(x) == 'True' else 0)

    _log(f"✔ Encodages appliqués :")
    _log(f"  type_contrat_enc : {df['type_contrat_enc'].nunique()} valeurs")
    _log(f"  label_enc        : {df['label_enc'].value_counts().to_dict()}")
    _log(f"  alerte_enc       : {df['alerte_enc'].value_counts().to_dict()}")

    return df


def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Comble les valeurs manquantes restantes."""
    df['jumeau_de']    = df['jumeau_de'].fillna(df['ID'])
    df['technique']    = df['technique'].fillna('original')
    df['langue']       = df['langue'].fillna('AR')
    df['Nb_Risques']   = pd.to_numeric(df['Nb_Risques'], errors='coerce').fillna(0).astype(int)
    df = df.fillna('')
    return df
