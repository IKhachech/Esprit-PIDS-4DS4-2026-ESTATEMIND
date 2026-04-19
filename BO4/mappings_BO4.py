"""
mappings_BO4.py — Constantes, encodages et règles pour l'Objectif 4 : Conformité Juridique.

Sources :
  - م.ع.ع (Mجلة الالتزامات والعقود) — Code des Obligations et Contrats Tunisien
  - مجلة الحقوق العينية 2011 — Code des Droits Réels
  - JORT — Journal Officiel de la République Tunisienne
  - Loi n°77-38 sur la location commerciale

Colonnes TARGET :
  - label_nlp       → NLP Classification (Faible/Moyen/Élevé/Critique)
  - niveau_alerte   → Rule-based + ML alertes (NORMALE/ÉLEVÉ/CRITIQUE)
  - statut_conformite → RAG label (مطابق/مطابق جزئياً/غير مطابق)
"""

import warnings
warnings.filterwarnings('ignore')

# ================================================================
# ENCODAGES TYPES DE CONTRATS
# ================================================================

TYPE_CONTRAT_ENC = {
    'كراء سكني':      1,
    'كراء تجاري':     2,
    'كراء مهني':      3,
    'كراء مختلط':     4,
    'بيع عقاري':      5,
    'بيع أرض':        6,
    'بيع عقار مبني':  7,
    'بيع منقول':      8,
    'وعد بالبيع':     9,
    'وكالة عقارية':   10,
    'عقد غير مصنف':   99,
}

SOUS_TYPE_ENC = {
    'كراء': 1,
    'بيع':  2,
    'وعد':  3,
    'وكالة': 4,
    'أخرى': 9,
}

# ================================================================
# ENCODAGES LABELS (TARGET NLP)
# ================================================================

LABEL_ENC = {
    'Faible':   0,   # منخفض — عقد مكتمل لا مخاطر
    'Moyen':    1,   # متوسط — بنود ثانوية مفقودة
    'Élevé':    2,   # مرتفع — بنود جوهرية مفقودة
    'Critique': 3,   # حرج   — عقد ناقص أو غير قانوني
}

LABEL_AR_ENC = {
    'منخفض':  0,
    'متوسط':  1,
    'مرتفع':  2,
    'حرج':    3,
}

ALERTE_ENC = {
    'NORMALE':  0,
    'ÉLEVÉ':    1,
    'CRITIQUE': 2,
}

STATUT_CONFORMITE_ENC = {
    'مطابق — عقد مكتمل':              0,
    'مطابق جزئياً — يحتاج مراجعة':   1,
    'غير مطابق — مخاطر مرتفعة':       2,
    'غير مطابق — خطر قانوني حرج':     3,
}

# ================================================================
# DOMAINES JURIDIQUES
# ================================================================

DOMAINE_JURIDIQUE_ENC = {
    'كراء سكني':   1,
    'كراء تجاري':  2,
    'كراء':        3,
    'بيع عقاري':   4,
    'بيع منقول':   5,
    'وعد بالبيع':  6,
    'وكالة':       7,
    'عقد عقاري':   9,
}

# ================================================================
# LOIS DE RÉFÉRENCE — correspondance domaine → articles
# ================================================================

LOI_PAR_DOMAINE = {
    'كراء سكني':    'م.ع.ع الفصل 742-780 — الكراء / قانون 1977 المتعلق بالكراء',
    'كراء تجاري':   'م.ع.ع الفصل 742-780 — الكراء التجاري / قانون 1977',
    'كراء مهني':    'م.ع.ع الفصل 742-780 — الكراء المهني',
    'كراء مختلط':   'م.ع.ع الفصل 742-780 — الكراء',
    'بيع عقاري':    'م.ع.ع الفصل 580-641 — البيع / مجلة الحقوق العينية 2011',
    'بيع أرض':      'م.ع.ع الفصل 580-641 — البيع / قانون التسجيل',
    'بيع عقار مبني':'م.ع.ع الفصل 580-641 — البيع / قانون التسجيل',
    'بيع منقول':    'م.ع.ع الفصل 580-620 — البيع المنقول',
    'وعد بالبيع':   'م.ع.ع الفصل 580-641 — البيع / قانون التسجيل',
    'وكالة عقارية': 'م.ع.ع الفصل 1138-1174 — الوكالة',
    'عقد غير مصنف': 'م.ع.ع — مجلة الالتزامات والعقود',
}

# ================================================================
# CLAUSES OBLIGATOIRES PAR TYPE DE CONTRAT
# ================================================================

CLAUSES_OBLIGATOIRES = {
    'كراء': [
        'تعريف الأطراف',
        'موضوع العقد',
        'المدة',
        'الثمن/المقدار',
        'الدفع',
        'التسليم',
        'الفسخ',
        'المعاليم/الضرائب',
    ],
    'بيع': [
        'تعريف الأطراف',
        'موضوع العقد',
        'الثمن/المقدار',
        'الدفع',
        'التسجيل الرسمي',
        'خلو من الرهن',
        'الشهود',
    ],
    'وعد': [
        'تعريف الأطراف',
        'موضوع العقد',
        'الثمن/المقدار',
        'العربون',
        'أجل البيع النهائي',
        'التسجيل الرسمي',
    ],
    'وكالة': [
        'تعريف الأطراف',
        'موضوع العقد',
        'الصلاحيات',
        'أجل الوكالة',
        'شروط الإلغاء',
    ],
}

# ================================================================
# RÈGLES JURIDIQUES — Rule-based alertes
# ================================================================

RULES_JURIDIQUES = [
    # ── RÈGLES KIRAء ──────────────────────────────────────────
    {
        'rule_id':       'R_KR_001',
        'domaine':       'كراء',
        'obligation':    'تحديد مدة الكراء',
        'condition':     'غياب المدة في عقد الكراء',
        'loi':           'م.ع.ع الفصل 754',
        'risk_score':    3,
        'penalite_type': 'بطلان نسبي — تحويل إلى كراء غير محدد المدة',
        'penalite_score':2,
        'alerte':        'ÉLEVÉ',
    },
    {
        'rule_id':       'R_KR_002',
        'domaine':       'كراء',
        'obligation':    'تحديد مقدار الكراء',
        'condition':     'غياب المبلغ في عقد الكراء',
        'loi':           'م.ع.ع الفصل 756',
        'risk_score':    4,
        'penalite_type': 'بطلان العقد',
        'penalite_score':4,
        'alerte':        'CRITIQUE',
    },
    {
        'rule_id':       'R_KR_003',
        'domaine':       'كراء',
        'obligation':    'الإشعار المسبق بالخروج',
        'condition':     'غياب بند الإشعار المسبق',
        'loi':           'م.ع.ع الفصل 766 / قانون 1977',
        'risk_score':    2,
        'penalite_type': 'تعويض شهر كراء كامل',
        'penalite_score':2,
        'alerte':        'NORMALE',
    },
    {
        'rule_id':       'R_KR_004',
        'domaine':       'كراء',
        'obligation':    'بند الكفالة/الضمان',
        'condition':     'غياب بند الضمان في عقد الكراء',
        'loi':           'م.ع.ع الفصل 1478',
        'risk_score':    2,
        'penalite_type': 'خطر مالي — لا ضمان عند الإخلال',
        'penalite_score':2,
        'alerte':        'NORMALE',
    },
    {
        'rule_id':       'R_KR_005',
        'domaine':       'كراء',
        'obligation':    'بند الفسخ والجزاء',
        'condition':     'غياب بند الفسخ الوجوبي',
        'loi':           'م.ع.ع الفصل 273',
        'risk_score':    2,
        'penalite_type': 'تعقيد الإجراءات القضائية للإخلاء',
        'penalite_score':2,
        'alerte':        'NORMALE',
    },
    {
        'rule_id':       'R_KR_006',
        'domaine':       'كراء',
        'obligation':    'تحديد نسبة الترفيع في الكراء',
        'condition':     'غياب بند الترفيع مع تجديد العقد',
        'loi':           'م.ع.ع الفصل 756 / قانون 1977',
        'risk_score':    1,
        'penalite_type': 'نزاع مستقبلي حول الزيادة',
        'penalite_score':1,
        'alerte':        'NORMALE',
    },
    {
        'rule_id':       'R_KR_007',
        'domaine':       'كراء',
        'obligation':    'منع التسويغ الفرعي',
        'condition':     'غياب بند حظر التسويغ للغير',
        'loi':           'م.ع.ع الفصل 770',
        'risk_score':    2,
        'penalite_type': 'خطر تسويغ فرعي غير مشروع',
        'penalite_score':2,
        'alerte':        'NORMALE',
    },
    # ── RÈGLES VENTE ──────────────────────────────────────────
    {
        'rule_id':       'R_VT_001',
        'domaine':       'بيع عقاري',
        'obligation':    'التسجيل الرسمي لعقد البيع',
        'condition':     'بيع عقار بدون تسجيل لدى قباضة التسجيل',
        'loi':           'قانون التسجيل / مجلة الحقوق العينية الفصل 305',
        'risk_score':    5,
        'penalite_type': 'لا قيمة قانونية — غير نافذ في مواجهة الغير',
        'penalite_score':5,
        'alerte':        'CRITIQUE',
    },
    {
        'rule_id':       'R_VT_002',
        'domaine':       'بيع عقاري',
        'obligation':    'إثبات خلو العقار من الرهن',
        'condition':     'غياب شرط خلو العقار من الرهون والاختصاصات',
        'loi':           'م.ع.ع الفصل 612 / مجلة الحقوق العينية الفصل 161',
        'risk_score':    4,
        'penalite_type': 'خطر الاستحقاق — إلغاء البيع',
        'penalite_score':4,
        'alerte':        'ÉLEVÉ',
    },
    {
        'rule_id':       'R_VT_003',
        'domaine':       'بيع عقاري',
        'obligation':    'حضور الشهود عند إمضاء عقد البيع',
        'condition':     'غياب الشهود في عقد البيع العقاري',
        'loi':           'م.ع.ع الفصل 472',
        'risk_score':    3,
        'penalite_type': 'قابلية الطعن في صحة العقد',
        'penalite_score':3,
        'alerte':        'ÉLEVÉ',
    },
    {
        'rule_id':       'R_VT_004',
        'domaine':       'بيع عقاري',
        'obligation':    'ذكر الثمن الحقيقي للبيع',
        'condition':     'غياب أو إخفاء ثمن البيع الفعلي',
        'loi':           'م.ع.ع الفصل 586',
        'risk_score':    5,
        'penalite_type': 'بطلان العقد + تتبع جبائي',
        'penalite_score':5,
        'alerte':        'CRITIQUE',
    },
    {
        'rule_id':       'R_VT_005',
        'domaine':       'بيع عقاري',
        'obligation':    'التحقق من الرسم العقاري',
        'condition':     'بيع عقار بدون ذكر رقم الرسم العقاري',
        'loi':           'مجلة الحقوق العينية الفصل 305',
        'risk_score':    4,
        'penalite_type': 'خطر ازدواجية البيع',
        'penalite_score':4,
        'alerte':        'ÉLEVÉ',
    },
    # ── RÈGLES PROMESSE DE VENTE ──────────────────────────────
    {
        'rule_id':       'R_WD_001',
        'domaine':       'وعد بالبيع',
        'obligation':    'تحديد أجل إنجاز البيع النهائي',
        'condition':     'غياب أجل محدد لإبرام البيع النهائي',
        'loi':           'م.ع.ع الفصل 580',
        'risk_score':    4,
        'penalite_type': 'وعد غير ملزم — خطر التراجع',
        'penalite_score':4,
        'alerte':        'ÉLEVÉ',
    },
    {
        'rule_id':       'R_WD_002',
        'domaine':       'وعد بالبيع',
        'obligation':    'تسجيل وعد البيع',
        'condition':     'وعد بالبيع غير مسجل رسمياً',
        'loi':           'مجلة الحقوق العينية الفصل 305 / قانون التسجيل',
        'risk_score':    5,
        'penalite_type': 'غير نافذ في مواجهة الغير — خطر ازدواجية البيع',
        'penalite_score':5,
        'alerte':        'CRITIQUE',
    },
    {
        'rule_id':       'R_WD_003',
        'domaine':       'وعد بالبيع',
        'obligation':    'تحديد قيمة العربون وشروط استرجاعه',
        'condition':     'غياب شروط استرجاع العربون',
        'loi':           'م.ع.ع الفصل 439-441',
        'risk_score':    3,
        'penalite_type': 'نزاع حول العربون عند التراجع',
        'penalite_score':3,
        'alerte':        'ÉLEVÉ',
    },
    # ── RÈGLES MANDAT ─────────────────────────────────────────
    {
        'rule_id':       'R_WK_001',
        'domaine':       'وكالة',
        'obligation':    'تحديد أجل الوكالة',
        'condition':     'وكالة عقارية مفتوحة بدون أجل محدد',
        'loi':           'م.ع.ع الفصل 1148',
        'risk_score':    4,
        'penalite_type': 'وكالة دائمة — خطر سوء الاستخدام',
        'penalite_score':3,
        'alerte':        'ÉLEVÉ',
    },
    {
        'rule_id':       'R_WK_002',
        'domaine':       'وكالة',
        'obligation':    'تحديد شروط إلغاء الوكالة',
        'condition':     'غياب شروط إلغاء الوكالة',
        'loi':           'م.ع.ع الفصل 1163',
        'risk_score':    3,
        'penalite_type': 'صعوبة إلغاء الوكالة قانونياً',
        'penalite_score':3,
        'alerte':        'ÉLEVÉ',
    },
    # ── RÈGLES GÉNÉRALES ──────────────────────────────────────
    {
        'rule_id':       'R_GEN_001',
        'domaine':       'عام',
        'obligation':    'تحديد هوية الأطراف بالكامل',
        'condition':     'غياب أو نقص في تعريف أطراف العقد',
        'loi':           'م.ع.ع الفصل 2',
        'risk_score':    3,
        'penalite_type': 'قابلية الطعن في صحة العقد',
        'penalite_score':3,
        'alerte':        'ÉLEVÉ',
    },
    {
        'rule_id':       'R_GEN_002',
        'domaine':       'عام',
        'obligation':    'توقيع جميع الأطراف',
        'condition':     'غياب إمضاء أحد الأطراف',
        'loi':           'م.ع.ع الفصل 472',
        'risk_score':    5,
        'penalite_type': 'بطلان العقد',
        'penalite_score':5,
        'alerte':        'CRITIQUE',
    },
    {
        'rule_id':       'R_GEN_003',
        'domaine':       'عام',
        'obligation':    'تحديد موضوع العقد بوضوح',
        'condition':     'موضوع العقد غامض أو ناقص',
        'loi':           'م.ع.ع الفصل 5',
        'risk_score':    3,
        'penalite_type': 'بطلان — موضوع مجهول',
        'penalite_score':3,
        'alerte':        'ÉLEVÉ',
    },
    {
        'rule_id':       'R_GEN_004',
        'domaine':       'عام',
        'obligation':    'كتابة العقد بلغة واضحة',
        'condition':     'عقد غامض أو متناقض في بنوده',
        'loi':           'م.ع.ع الفصل 543',
        'risk_score':    2,
        'penalite_type': 'تأويل ضد الطرف الذي حرر العقد',
        'penalite_score':2,
        'alerte':        'NORMALE',
    },
    {
        'rule_id':       'R_GEN_005',
        'domaine':       'عام',
        'obligation':    'تعدد نسخ العقد',
        'condition':     'عقد بنسخة واحدة فقط',
        'loi':           'م.ع.ع الفصل 474',
        'risk_score':    1,
        'penalite_type': 'صعوبة إثبات العقد',
        'penalite_score':1,
        'alerte':        'NORMALE',
    },
    # ── RÈGLES BCT (investissement étranger) ──────────────────
    {
        'rule_id':       'R_BCT_001',
        'domaine':       'بيع عقاري',
        'obligation':    'ترخيص BCT للاستثمار الأجنبي',
        'condition':     'معاملة عقارية بالعملة الأجنبية دون ترخيص BCT',
        'loi':           'قانون الصرف n°77-608 / قانون الاستثمار 2019-47',
        'risk_score':    5,
        'penalite_type': 'عقوبات جزائية BCT / تجميد الأموال',
        'penalite_score':5,
        'alerte':        'CRITIQUE',
    },
    # ── RÈGLES JORT 2026 ──────────────────────────────────────
    {
        'rule_id':       'R_JORT_001',
        'domaine':       'بيع عقاري',
        'obligation':    'موافقة رئاسية على عمليات الدومين',
        'condition':     'نقل أو تفويت في أملاك الدولة دون موافقة رئاسية',
        'loi':           'مرسوم رئاسي 2026-6/7/8 — JORT N°3 يناير 2026',
        'risk_score':    5,
        'penalite_type': 'بطلان العقد + عقوبات جزائية',
        'penalite_score':5,
        'alerte':        'CRITIQUE',
    },
    {
        'rule_id':       'R_JORT_002',
        'domaine':       'بيع عقاري',
        'obligation':    'الحصول على رخصة البناء قبل البدء في الأشغال',
        'condition':     'بناء أو ترميم جوهري دون رخصة بناء',
        'loi':           'مجلة التهيئة الترابية والتعمير الفصل 68 / قانون 94-122',
        'risk_score':    3,
        'penalite_type': 'هدم / غرامات / متابعة جزائية',
        'penalite_score':4,
        'alerte':        'ÉLEVÉ',
    },
]

# ================================================================
# COLONNES TARGET FINALES
# ================================================================

TARGET_COLS = [
    # ── Identifiant ──────────────────────────────────────────
    'ID',                       # identifiant unique du document
    # ── Classification du document ──────────────────────────
    'type_contrat',             # encodé via TYPE_CONTRAT_ENC
    'sous_type',                # encodé via SOUS_TYPE_ENC
    'langue',                   # AR / FR / AR-FR
    'domaine_juridique',        # encodé via DOMAINE_JURIDIQUE_ENC
    # ── Texte ────────────────────────────────────────────────
    'Texte_Contrat',            # texte complet original
    'Texte_Normalise',          # texte nettoyé pour NLP/RAG
    # ── Features NLP ─────────────────────────────────────────
    'nb_clauses_presentes',     # nb clauses détectées
    'nb_clauses_manquantes',    # nb clauses manquantes
    'clauses_presentes',        # liste clauses présentes
    'clauses_manquantes',       # liste clauses manquantes
    'Nb_Mots',                  # longueur texte (mots)
    'Nb_Caracteres',            # longueur texte (chars)
    # ── Juridique ────────────────────────────────────────────
    'loi_reference',            # article de loi applicable
    'Score',                    # score risque 0-11
    'Nb_Risques',               # nb risques détectés
    'risque_principal',         # risque le plus grave (texte)
    # ── TARGET 1 — NLP Classification ────────────────────────
    'Label_FR',                 # Faible/Moyen/Élevé/Critique
    'Label_AR',                 # منخفض/متوسط/مرتفع/حرج
    # ── TARGET 2 — Rule-based + ML Alertes ───────────────────
    'niveau_alerte',            # NORMALE/ÉLEVÉ/CRITIQUE
    'alerte_generee',           # True/False
    # ── TARGET 3 — RAG label ─────────────────────────────────
    'statut_conformite',        # مطابق/جزئياً/غير مطابق
    # ── Traçabilité ──────────────────────────────────────────
    'technique',                # original/jumeau/reequilibrage
    'jumeau_de',                # ID du contrat source
]

FICHIERS_ML = {
    'contrats_BO4': ('dataset_BO4_contrats_final.xlsx', '1565C0'),
    'jort_BO4':     ('dataset_BO4_jort_chunks.xlsx',    '2E7D32'),
    'rules_BO4':    ('dataset_BO4_rules.xlsx',          'E65100'),
}


def print_encoding_summary():
    print("\n" + "=" * 65)
    print("   ENCODAGES — OBJECTIF 4 : CONFORMITÉ JURIDIQUE")
    print("=" * 65)
    print(f"  types contrats    : {len(TYPE_CONTRAT_ENC)} types encodés 1-99")
    print(f"  sous_types        : {len(SOUS_TYPE_ENC)} (كراء/بيع/وعد/وكالة)")
    print(f"  labels NLP        : {len(LABEL_ENC)} (Faible/Moyen/Élevé/Critique)")
    print(f"  alertes           : {len(ALERTE_ENC)} (NORMALE/ÉLEVÉ/CRITIQUE)")
    print(f"  règles juridiques : {len(RULES_JURIDIQUES)} règles actives")
    print(f"  TARGET_COLS       : {len(TARGET_COLS)} colonnes")
    print()
    print("  Modèles cibles :")
    print("    NLP Classification  → Label_FR (4 classes)")
    print("    Rule-based + ML     → niveau_alerte + alerte_generee")
    print("    RAG (LLM)           → statut_conformite + Texte_Normalise")
    print("=" * 65)
