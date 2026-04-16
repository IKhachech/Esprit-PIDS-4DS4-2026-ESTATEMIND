"""
tft_model.py — Temporal Fusion Transformer · Version Production BO3
====================================================================

Branché exactement sur le pipeline existant :
  - tft_dataset_builder.py → build_tft_dataset() → DataFrame structuré
  - features_config.py     → FEATURE_SETS["tft"]  → groupes de colonnes
  - cluster_map.pkl        → cluster, cluster_size (statiques)
  - pelt_A_rapport.csv     → rupture_flag, score_rupture, post_rupture

ARCHITECTURE TFT (adaptée marché immobilier tunisien multi-gouvernorats)
─────────────────────────────────────────────────────────────────────────
Basée sur "Temporal Fusion Transformers for Interpretable Multi-horizon
Time Series Forecasting" (Lim et al., 2021), adaptée aux contraintes :
  - Données panel hétérogènes (24 gouvernorats, séries de longueurs inégales)
  - Effets spatiaux forts (Grand Tunis vs Intérieur)
  - Signaux causaux PELT (ruptures de régime)
  - Horizons multiples : T+1, T+3, T+12

COMPOSANTS CLÉS :
  1. Variable Selection Network (VSN) — pondère chaque feature dynamiquement
  2. Gated Residual Network (GRN) — backbone non-linéaire avec gating
  3. LSTM Encoder-Decoder — capture la dépendance temporelle
  4. Multi-Head Attention — pondère l'importance temporelle passée
  5. Quantile Output — incertitude calibrée (P10/P50/P90)

HORIZONS :
  - T+1  : décision court terme (achat/location imminent)
  - T+3  : tendance trimestrielle (investissement)
  - T+12 : tendance annuelle (stratégie patrimoniale)

LOSS :
  QuantileLoss (P10/P50/P90) — robuste aux outliers de prix, calibrée
  pour l'incertitude du marché immobilier tunisien.

Usage :
  from tft_model import TFTModel, TFTConfig, train_tft, predict_tft
  config = TFTConfig()
  model  = TFTModel(config)
  trainer = train_tft(model, tft_df, col_groups, config)
"""

import os
import math
import warnings
import numpy as np
import pandas as pd
import pickle
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import LabelEncoder, RobustScaler
from features_registry import validate_features, get_features

warnings.filterwarnings('ignore')

# ─── Reproductibilité ─────────────────────────────────────────────────────────
SEED = 42
torch.manual_seed(SEED)
np.random.seed(SEED)

# ─── Device ───────────────────────────────────────────────────────────────────
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')


# ================================================================
# 1. CONFIGURATION
# ================================================================

@dataclass
class TFTConfig:
    """
    Configuration complète du TFT.
    Valeurs calibrées pour le marché immobilier tunisien (~24 gouvernorats,
    séries de 24-48 mois, données mensuelles panel).
    """
    # ── Fenêtres temporelles ──────────────────────────────────────
    encoder_length: int = 12        # 12 mois d'historique (1 an)
    decoder_length: int = 12        # max horizon = T+12

    # ── Horizons de prédiction (indices dans la séquence decoder) ─
    # T+1 = index 0, T+3 = index 2, T+12 = index 11
    predict_horizons: List[int] = field(default_factory=lambda: [0, 2, 11])

    # ── Quantiles (incertitude calibrée) ─────────────────────────
    quantiles: List[float] = field(default_factory=lambda: [0.1, 0.5, 0.9])

    # ── Dimensions modèle ─────────────────────────────────────────
    d_model: int = 64               # dimension cachée principale
    num_heads: int = 4              # têtes d'attention
    num_lstm_layers: int = 2        # couches LSTM encoder + decoder
    dropout: float = 0.15          # dropout (faible car peu de données)
    ffn_dim: int = 128              # dimension FFN interne GRN

    # ── Features (synchronisées avec features_config.py) ─────────
    # Catégorielles
    static_cat_features: List[str] = field(default_factory=lambda: [
        'gouvernorat_enc',          # encodé LabelEncoder
        'cluster_enc',              # encodé LabelEncoder
        'zone_geo',                 # 0-4
        'groupe_enc',               # Residentiel/Foncier/Commercial
    ])
    # Réelles statiques
    static_real_features: List[str] = field(default_factory=lambda: [
        'cluster_size_norm',        # normalisé
        'score_attractivite',
        'potentiel_emergent',
        'zscore_ref',
        'ratio_lv_ref',
    ])
    # Réelles connues à l'avance
    known_real_features: List[str] = field(default_factory=lambda: [
        'mois_sin',                 # encodage cyclique
        'mois_cos',
        'annee_norm',
        'high_season',
        'saison',
        'inflation_glissement_annuel',
        'croissance_pib_trim',
        'glissement_immo_trim',
    ])
    # Réelles observées (dynamiques — incluent les signaux PELT)
    unknown_real_features: List[str] = field(default_factory=lambda: [
        'prix_m2_norm',             # TARGET (normalisé)
        'prix_m2_loc_norm',         # sous-signal location
        'prix_m2_ven_norm',         # sous-signal vente
        'ratio_loc_ven',
        'volatilite_rolling_norm',
        'nb_annonces_proxy_norm',
        'rupture_flag',             # ← signal PELT causal
        'score_rupture',            # ← intensité rupture PELT
        'post_rupture',             # ← régime post-rupture PELT
        'indice_liquidite_norm',
        'volatilite_prix_trim_norm',
    ])

    # ── Entraînement ─────────────────────────────────────────────
    learning_rate: float = 3e-4
    weight_decay: float = 1e-4
    batch_size: int = 32
    max_epochs: int = 100
    patience: int = 15
    min_delta: float = 1e-4
    gradient_clip: float = 1.0
    val_ratio: float = 0.2         # 80/20 split temporel strict

    # ── Divers ───────────────────────────────────────────────────
    min_series_length: int = 20    # gouvernorats trop courts → exclus
    target_col: str = 'prix_m2_norm'
    target_raw_col: str = 'prix_m2'  # colonne avant normalisation (pour dénorm.)

    @property
    def n_static_cat(self) -> int:
        return len(self.static_cat_features)

    @property
    def n_static_real(self) -> int:
        return len(self.static_real_features)

    @property
    def n_known_real(self) -> int:
        return len(self.known_real_features)

    @property
    def n_unknown_real(self) -> int:
        return len(self.unknown_real_features)

    @property
    def n_quantiles(self) -> int:
        return len(self.quantiles)

    @property
    def n_horizons(self) -> int:
        return len(self.predict_horizons)


# ================================================================
# 2. COMPOSANTS TFT
# ================================================================

class GatedLinearUnit(nn.Module):
    """GLU : gate sigmoid × linear."""
    def __init__(self, input_dim: int, output_dim: int, dropout: float = 0.0):
        super().__init__()
        self.linear = nn.Linear(input_dim, output_dim * 2)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.dropout(x)
        x = self.linear(x)
        x1, x2 = x.chunk(2, dim=-1)
        return x1 * torch.sigmoid(x2)


class GatedResidualNetwork(nn.Module):
    """
    GRN : cœur du TFT.
    GRN(x, c) = LayerNorm(x + GLU(ELU(W1·[x,c]) + W2·x))
    c = contexte optionnel (features statiques dans l'encoder).
    """
    def __init__(self, input_dim: int, hidden_dim: int,
                 output_dim: int, context_dim: int = 0, dropout: float = 0.0):
        super().__init__()
        self.fc1 = nn.Linear(input_dim + context_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, output_dim * 2)  # pour GLU
        self.norm = nn.LayerNorm(output_dim)
        self.dropout = nn.Dropout(dropout)

        # Skip connection (projette si dim différente)
        self.skip = (nn.Linear(input_dim, output_dim, bias=False)
                     if input_dim != output_dim else nn.Identity())

    def forward(self, x: torch.Tensor,
                context: Optional[torch.Tensor] = None) -> torch.Tensor:
        residual = self.skip(x)
        if context is not None:
            h = torch.cat([x, context], dim=-1)
        else:
            h = x
        h = self.fc1(h)
        h = F.elu(h)
        h = self.dropout(h)
        h = self.fc2(h)
        h1, h2 = h.chunk(2, dim=-1)
        h = h1 * torch.sigmoid(h2)           # GLU
        return self.norm(h + residual)


class VariableSelectionNetwork(nn.Module):
    """
    VSN : sélectionne dynamiquement les features importantes.
    Produit des poids softmax par feature → interprétabilité native.
    """
    def __init__(self, n_features: int, d_model: int,
                 context_dim: int = 0, dropout: float = 0.0):
        super().__init__()
        self.n_features = n_features
        self.d_model    = d_model

        # Une projection par feature
        self.feature_grns = nn.ModuleList([
            GatedResidualNetwork(d_model, d_model, d_model,
                                 context_dim=context_dim, dropout=dropout)
            for _ in range(n_features)
        ])

        # GRN qui produit les poids de sélection (une entrée par feature)
        self.selection_grn = GatedResidualNetwork(
            n_features * d_model, d_model, n_features,
            context_dim=context_dim, dropout=dropout)

    def forward(self, x: torch.Tensor,
                context: Optional[torch.Tensor] = None) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        x      : (B, T, n_features, d_model) ou (B, n_features, d_model) pour statiques
        context: (B, context_dim) ou (B, T, context_dim)
        Returns: (B, T, d_model) weighted sum, (B, T, n_features) weights
        """
        # Traitement par feature
        processed = []
        for i, grn in enumerate(self.feature_grns):
            xi = x[..., i, :]                # (B, T, d_model) ou (B, d_model)
            ctx = context if context is not None else None
            processed.append(grn(xi, ctx))   # (B, T, d_model)

        processed = torch.stack(processed, dim=-2)  # (B, T, n_feat, d_model)

        # Poids de sélection
        flat = processed.view(*processed.shape[:-2], -1)  # (B, T, n_feat*d_model)
        weights = self.selection_grn(flat, context)        # (B, T, n_feat)
        weights = torch.softmax(weights, dim=-1)           # normaliser

        # Somme pondérée
        output = (processed * weights.unsqueeze(-1)).sum(dim=-2)  # (B, T, d_model)
        return output, weights


class ScaledDotProductAttention(nn.Module):
    """Multi-Head Self-Attention avec masquage temporel."""
    def __init__(self, d_model: int, num_heads: int, dropout: float = 0.0):
        super().__init__()
        assert d_model % num_heads == 0
        self.d_k     = d_model // num_heads
        self.h       = num_heads
        self.W_q     = nn.Linear(d_model, d_model, bias=False)
        self.W_k     = nn.Linear(d_model, d_model, bias=False)
        self.W_v     = nn.Linear(d_model, d_model, bias=False)
        self.W_o     = nn.Linear(d_model, d_model, bias=False)
        self.dropout = nn.Dropout(dropout)
        self.norm    = nn.LayerNorm(d_model)

    def forward(self, x: torch.Tensor,
                mask: Optional[torch.Tensor] = None) -> Tuple[torch.Tensor, torch.Tensor]:
        B, T, _ = x.shape
        q = self.W_q(x).view(B, T, self.h, self.d_k).transpose(1, 2)
        k = self.W_k(x).view(B, T, self.h, self.d_k).transpose(1, 2)
        v = self.W_v(x).view(B, T, self.h, self.d_k).transpose(1, 2)

        scores = torch.matmul(q, k.transpose(-2, -1)) / math.sqrt(self.d_k)
        if mask is not None:
            # mask shape : (T, T) ou (1, 1, T, T)
            if mask.dim() == 2:
                mask = mask.unsqueeze(0).unsqueeze(0)  # (1, 1, T, T)
            # True=visible, False=masked → set -inf where False
            scores = scores.masked_fill(~mask, -1e9)
        attn = self.dropout(torch.softmax(scores, dim=-1))

        out = torch.matmul(attn, v)
        out = out.transpose(1, 2).contiguous().view(B, T, -1)
        out = self.W_o(out)
        return self.norm(x + out), attn.mean(dim=1)


# ================================================================
# 3. ARCHITECTURE TFT COMPLÈTE
# ================================================================

class TFTModel(nn.Module):
    """
    Temporal Fusion Transformer adapté au marché immobilier tunisien.

    Pipeline interne :
      1. Embedding catégoriels → d_model
      2. Projection réelles (statiques / known / unknown) → d_model
      3. VSN encoder : sélection features + contexte statique
      4. LSTM Encoder : contexte temporel passé
      5. VSN decoder : sélection features futures
      6. LSTM Decoder : génération séquence future
      7. Multi-Head Attention : sur la séquence complète enc+dec
      8. Position-wise FFN (GRN)
      9. Quantile output : P10 / P50 / P90
    """

    def __init__(self, config: TFTConfig,
                 cat_cardinalities: Dict[str, int] = None):
        """
        cat_cardinalities : {feature_name: n_categories}
        Ex: {'gouvernorat_enc': 24, 'cluster_enc': 4, 'zone_geo': 5, 'groupe_enc': 3}
        """
        super().__init__()
        self.config = config
        d = config.d_model
        n_q = config.n_quantiles

        # ── Embeddings catégoriels ────────────────────────────────
        if cat_cardinalities is None:
            cat_cardinalities = {
                'gouvernorat_enc': 25,
                'cluster_enc'    : 8,
                'zone_geo'       : 5,
                'groupe_enc'     : 3,
            }
        self.cat_embeddings = nn.ModuleDict({
            k: nn.Embedding(v + 1, d)
            for k, v in cat_cardinalities.items()
        })

        # ── VSN statique paper-grade ───────────────────────────────
        # Traite TOUTES les features statiques (cat + real) via un VSN
        # unique qui produit 4 vecteurs contexte distincts (Lim 2021, §3.3) :
        #   cs : état initial encoder LSTM (cell state)
        #   ch : état initial encoder LSTM (hidden)
        #   ce : enrichissement encoder (injecté dans VSN enc à chaque pas)
        #   cd : enrichissement decoder (injecté dans VSN dec à chaque pas)
        n_static_total = len(cat_cardinalities) + config.n_static_real
        self.vsn_static = VariableSelectionNetwork(
            n_features  = n_static_total,
            d_model     = d,
            context_dim = 0,    # pas de contexte externe pour le statique
            dropout     = config.dropout,
        )
        self.static_real_proj = nn.Linear(1, d)  # projette chaque real → d
        # 4 GRN produisant les 4 vecteurs contexte depuis la sortie VSN statique
        self.grn_cs = GatedResidualNetwork(d, config.ffn_dim, d, dropout=config.dropout)
        self.grn_ch = GatedResidualNetwork(d, config.ffn_dim, d, dropout=config.dropout)
        self.grn_ce = GatedResidualNetwork(d, config.ffn_dim, d, dropout=config.dropout)
        self.grn_cd = GatedResidualNetwork(d, config.ffn_dim, d, dropout=config.dropout)

        # ── Projection features temporelles → d_model chacune ────
        self.known_proj   = nn.Linear(1, d)   # appliqué feature par feature
        self.unknown_proj = nn.Linear(1, d)

        # ── Variable Selection Networks ───────────────────────────
        self.vsn_encoder = VariableSelectionNetwork(
            n_features  = config.n_known_real + config.n_unknown_real,
            d_model     = d,
            context_dim = d,
            dropout     = config.dropout,
        )
        self.vsn_decoder = VariableSelectionNetwork(
            n_features  = config.n_known_real,  # decoder : known only
            d_model     = d,
            context_dim = d,
            dropout     = config.dropout,
        )

        # ── LSTM Encoder / Decoder ────────────────────────────────
        self.lstm_encoder = nn.LSTM(
            input_size  = d,
            hidden_size = d,
            num_layers  = config.num_lstm_layers,
            dropout     = config.dropout if config.num_lstm_layers > 1 else 0,
            batch_first = True,
        )
        self.lstm_decoder = nn.LSTM(
            input_size  = d,
            hidden_size = d,
            num_layers  = config.num_lstm_layers,
            dropout     = config.dropout if config.num_lstm_layers > 1 else 0,
            batch_first = True,
        )

        # Initialisation des états cachés LSTM depuis contexte statique
        self.lstm_h_init = nn.Linear(d, d * config.num_lstm_layers)
        self.lstm_c_init = nn.Linear(d, d * config.num_lstm_layers)

        # ── Gates post-LSTM ───────────────────────────────────────
        self.post_lstm_gate   = GatedLinearUnit(d, d, config.dropout)
        self.post_lstm_norm   = nn.LayerNorm(d)

        # ── Attention temporelle ──────────────────────────────────
        self.attention = ScaledDotProductAttention(
            d, config.num_heads, config.dropout)

        # ── Post-attention GRN ────────────────────────────────────
        self.post_attn_grn  = GatedResidualNetwork(d, config.ffn_dim, d,
                                                    dropout=config.dropout)
        self.post_attn_norm = nn.LayerNorm(d)

        # ── Quantile output step-wise ─────────────────────────────
        # Chaque tête prédit 1 valeur par timestep decoder.
        # On extrait ensuite les horizons voulus par indexation directe.
        # CORRECTION vs v1 : Linear(d, 1) au lieu de Linear(d, decoder_length)
        # → la représentation dec_repr[t] prédit l'horizon t (step-wise correct).
        self.quantile_heads = nn.ModuleList([
            nn.Linear(d, 1)
            for _ in range(n_q)
        ])

        self._init_weights()

    def _init_weights(self):
        for name, p in self.named_parameters():
            if 'weight' in name and p.dim() >= 2:
                nn.init.xavier_uniform_(p)
            elif 'bias' in name:
                nn.init.zeros_(p)

    def _static_context(self,
                         cat_inputs: Dict[str, torch.Tensor],
                         real_inputs: torch.Tensor,
                         ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        """
        VSN statique paper-grade (Lim 2021 §3.3).

        Projette TOUTES les features statiques dans d_model, passe dans le VSN
        statique, puis produit 4 vecteurs contexte distincts :
          cs — cell state initial LSTM
          ch — hidden state initial LSTM
          ce — enrichissement encoder (injecté dans VSN enc)
          cd — enrichissement decoder (injecté dans VSN dec)

        cat_inputs : {name: (B,) int}
        real_inputs: (B, n_static_real)
        Returns    : (cs, ch, ce, cd) chacun (B, d_model)
        """
        B = real_inputs.size(0)
        d = self.config.d_model

        # Projeter les catégoriels (chacun → d_model)
        cat_embs = [emb(cat_inputs[k])           # (B, d)
                    for k, emb in self.cat_embeddings.items()]

        # Projeter les réels statiques (chacun → d_model)
        # real_inputs : (B, n_static_real) → (B, n_static_real, 1) → project → (B, n_static_real, d)
        real_embs = []
        for i in range(real_inputs.size(1)):
            feat_i = real_inputs[:, i:i+1]       # (B, 1)
            real_embs.append(self.static_real_proj(feat_i))  # (B, d)

        # Stack : (B, n_static_total, d)
        all_static = torch.stack(cat_embs + real_embs, dim=1)

        # VSN statique → (B, d) + poids (B, n_static_total)
        # Le VSN attend (B, n_feat, d) — dimension T absente pour le statique
        # On ajoute une dim T=1 factice puis on la retire
        all_static_4d = all_static.unsqueeze(1)          # (B, 1, n_feat, d)
        vsn_out, vsn_w = self.vsn_static(all_static_4d)  # (B, 1, d), (B, 1, n_feat)
        vsn_out = vsn_out.squeeze(1)                      # (B, d)
        self._last_static_vsn_weights = vsn_w.squeeze(1) # (B, n_feat) — pour interprétabilité

        # 4 vecteurs contexte depuis GRN distincts
        cs = self.grn_cs(vsn_out)   # cell state LSTM
        ch = self.grn_ch(vsn_out)   # hidden state LSTM
        ce = self.grn_ce(vsn_out)   # enrichissement encoder
        cd = self.grn_cd(vsn_out)   # enrichissement decoder
        return cs, ch, ce, cd

    def _project_temporal(self, x: torch.Tensor,
                           proj: nn.Linear) -> torch.Tensor:
        """
        Projette feature-par-feature.
        x    : (B, T, n_feat)
        proj : Linear(1, d_model)
        Returns: (B, T, n_feat, d_model)
        """
        B, T, F = x.shape
        out = proj(x.reshape(B * T * F, 1)).view(B, T, F, -1)
        return out

    def _lstm_init_state(self, cs: torch.Tensor, ch: torch.Tensor):
        """
        Initialise (h0, c0) depuis les 2 vecteurs contexte statiques cs/ch.
        cs — cell state initial  (B, d_model)
        ch — hidden state initial (B, d_model)
        """
        B = cs.size(0)
        L = self.config.num_lstm_layers
        d = self.config.d_model
        h0 = self.lstm_h_init(ch).view(B, L, d).permute(1, 0, 2).contiguous()
        c0 = self.lstm_c_init(cs).view(B, L, d).permute(1, 0, 2).contiguous()
        return h0, c0

    def forward(self,
                cat_static   : Dict[str, torch.Tensor],
                real_static  : torch.Tensor,
                known_enc    : torch.Tensor,
                unknown_enc  : torch.Tensor,
                known_dec    : torch.Tensor,
                ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        """
        Forward pass TFT paper-grade (Lim 2021).

        Paramètres
        ----------
        cat_static  : dict {str: (B,)}          — catégoriels statiques
        real_static : (B, n_static_real)        — réels statiques
        known_enc   : (B, encoder_length, n_known_real)
        unknown_enc : (B, encoder_length, n_unknown_real)
        known_dec   : (B, decoder_length, n_known_real)

        Retourne
        --------
        quantiles    : (B, n_horizons, n_quantiles)  — P10/P50/P90 aux horizons
        attn_weights : (B, T_enc+T_dec, T_enc+T_dec) — poids attention interprétables
        vsn_enc_weights : (B, n_features_encoder)    — importance features encoder
        """
        B    = real_static.size(0)
        d    = self.config.d_model
        Tenc = self.config.encoder_length
        Tdec = self.config.decoder_length

        # ── 1. VSN statique → 4 vecteurs contexte ─────────────────
        cs, ch, ce, cd = self._static_context(cat_static, real_static)
        # cs, ch : (B, d) — init LSTM
        # ce, cd : (B, d) — enrichissement enc/dec

        h0, c0 = self._lstm_init_state(cs, ch)

        # ── 2. Projection temporelle encoder ──────────────────────
        ke_proj = self._project_temporal(known_enc,   self.known_proj)    # (B, Tenc, Nk, d)
        ue_proj = self._project_temporal(unknown_enc, self.unknown_proj)  # (B, Tenc, Nu, d)
        enc_all = torch.cat([ke_proj, ue_proj], dim=2)                    # (B, Tenc, Nk+Nu, d)

        # Contexte ce étendu pour VSN encoder (injecté à chaque pas)
        c_enc = ce.unsqueeze(1).expand(-1, Tenc, -1)               # (B, Tenc, d)

        # ── 3. VSN encoder avec contexte statique ce ──────────────
        enc_vsn, vsn_w_enc = self.vsn_encoder(enc_all, c_enc)      # (B, Tenc, d)

        # ── 4. LSTM encoder ────────────────────────────────────────
        enc_out, (h_n, c_n) = self.lstm_encoder(enc_vsn, (h0, c0))

        # Gate post-encoder + skip (papier eq. 4)
        enc_gate = self.post_lstm_gate(enc_vsn)  # GLU(enc_vsn)
        enc_out  = self.post_lstm_norm(enc_out + enc_gate)

        # ── 5. Projection temporelle decoder ──────────────────────
        kd_proj = self._project_temporal(known_dec, self.known_proj)      # (B, Tdec, Nk, d)
        c_dec   = cd.unsqueeze(1).expand(-1, Tdec, -1)

        # ── 6. VSN decoder avec contexte statique cd ──────────────
        dec_vsn, _ = self.vsn_decoder(kd_proj, c_dec)              # (B, Tdec, d)

        # ── 7. LSTM decoder (états initialisés depuis encoder) ─────
        dec_out, _ = self.lstm_decoder(dec_vsn, (h_n, c_n))

        # Gate post-decoder + skip
        dec_gate = self.post_lstm_gate(dec_vsn)
        dec_out  = self.post_lstm_norm(dec_out + dec_gate)

        # ── 8. Concaténation enc + dec pour attention ──────────────
        seq     = torch.cat([enc_out, dec_out], dim=1)              # (B, Tenc+Tdec, d)
        T_total = Tenc + Tdec

        # ── 9. Mask causal (paper-grade) ──────────────────────────
        # Le mask garantit :
        #   - encoder voit tout l'encoder (past → past OK)
        #   - decoder[t] voit encoder entier + decoder[0..t] (causal)
        #   - decoder[t] NE VOIT PAS decoder[t+1..Tdec] (pas de fuite future)
        mask = torch.zeros(T_total, T_total, device=seq.device)
        # Bloc enc → enc : tout visible (passé connu)
        mask[:Tenc, :Tenc] = 1.0
        # Bloc dec → enc : decoder voit tout l'encoder
        mask[Tenc:, :Tenc] = 1.0
        # Bloc dec → dec : causal (triangulaire inférieur)
        mask[Tenc:, Tenc:] = torch.tril(torch.ones(Tdec, Tdec, device=seq.device))
        # Bloc enc → dec : encoder ne voit PAS le futur decoder
        mask[:Tenc, Tenc:] = 0.0

        attn_out, attn_w = self.attention(seq, mask.bool())         # (B, T, d)

        # Skip connection + norm
        seq = self.post_attn_norm(seq + attn_out)

        # ── 10. GRN post-attention (position-wise) ─────────────────
        seq = self.post_attn_grn(seq)                               # (B, T, d)

        # ── 11. Multi-step quantile output step-wise ───────────────
        # CORRECTION : au lieu du pooling mean (qui écrase T+1 vs T+12),
        # on utilise la représentation dec_repr[t] pour prédire l'horizon t.
        # Chaque quantile head prend la repr au timestep horizon voulu.
        dec_repr = seq[:, Tenc:, :]                                 # (B, Tdec, d)

        # VSN encoder weights → (B, n_enc_features) pour interprétabilité
        # vsn_w_enc : (B, Tenc, n_feat) → moyenne temporelle → (B, n_feat)
        vsn_w_enc_mean = vsn_w_enc.mean(dim=1)                      # (B, n_feat)

        horizons   = [min(h, Tdec - 1) for h in self.config.predict_horizons]
        quantile_preds = []
        for head in self.quantile_heads:
            # head : Linear(d, 1) — prédit 1 valeur par timestep
            qp_all = head(dec_repr)                                 # (B, Tdec, 1)
            # Extraire uniquement les horizons voulus
            qp = qp_all[:, horizons, 0]                             # (B, n_horizons)
            quantile_preds.append(qp)

        # (B, n_horizons, n_quantiles)
        quantile_out = torch.stack(quantile_preds, dim=-1)

        return quantile_out, attn_w, vsn_w_enc_mean

    @torch.no_grad()
    def get_interpretability(self,
                              cat_static   : Dict[str, torch.Tensor],
                              real_static  : torch.Tensor,
                              known_enc    : torch.Tensor,
                              unknown_enc  : torch.Tensor,
                              known_dec    : torch.Tensor,
                              feature_names: Dict[str, List[str]] = None,
                              ) -> Dict:
        """
        Retourne les sorties d'interprétabilité du TFT.

        Returns
        -------
        dict avec clés :
          'vsn_encoder_weights'  : importance par feature encoder (dict {nom: poids})
          'vsn_static_weights'   : importance par feature statique
          'attention_weights'    : matrice attention (T_total × T_total)
          'static_context'       : 4 vecteurs contexte (cs, ch, ce, cd)
        """
        self.eval()
        pred, attn_w, vsn_enc_w = self.forward(
            cat_static, real_static, known_enc, unknown_enc, known_dec)

        result = {
            'prediction'          : pred.cpu().numpy(),
            'attention_weights'   : attn_w.cpu().numpy(),
            'vsn_encoder_weights' : vsn_enc_w.cpu().numpy(),
        }

        # Noms des features encoder (known + unknown)
        if feature_names is not None:
            known_names   = feature_names.get('known_real', [])
            unknown_names = feature_names.get('unknown_real', [])
            all_enc_names = known_names + unknown_names
            w = vsn_enc_w[0].numpy()  # première séquence du batch
            if len(w) == len(all_enc_names):
                result['vsn_encoder_named'] = {
                    n: round(float(w[i]), 4)
                    for i, n in enumerate(all_enc_names)
                }
                # Tri par importance décroissante
                result['vsn_encoder_ranked'] = sorted(
                    result['vsn_encoder_named'].items(),
                    key=lambda x: x[1], reverse=True)

        # Poids VSN statique
        if hasattr(self, '_last_static_vsn_weights'):
            sw = self._last_static_vsn_weights[0].cpu().numpy()
            cat_names  = list(self.cat_embeddings.keys())
            real_names = feature_names.get('static_real', []) \
                         if feature_names else []
            all_static = cat_names + real_names
            if len(sw) == len(all_static):
                result['vsn_static_named'] = {
                    n: round(float(sw[i]), 4)
                    for i, n in enumerate(all_static)
                }

        return result


# ================================================================
# 4. LOSS — QUANTILE PINBALL
# ================================================================

class QuantileLoss(nn.Module):
    """
    Pinball loss multi-quantile.
    Robuste aux outliers de prix, calibrée pour l'incertitude immobilière.

    Pour q=0.5 → équivalent MAE (médiane).
    Pour q=0.1/0.9 → intervalles de confiance 80%.
    """
    def __init__(self, quantiles: List[float]):
        super().__init__()
        self.register_buffer('q', torch.tensor(quantiles, dtype=torch.float32))

    def forward(self, pred: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        """
        pred  : (B, n_horizons, n_quantiles)
        target: (B, n_horizons)
        """
        target_exp = target.unsqueeze(-1)                          # (B, H, 1)
        errors     = target_exp - pred                             # (B, H, Q)
        losses     = torch.max(self.q * errors, (self.q - 1) * errors)
        return losses.mean()


# ================================================================
# 5. DATASET PYTORCH
# ================================================================

class TFTDataset(Dataset):
    """
    Dataset PyTorch pour le TFT.
    Construit les séquences (encoder + decoder) pour chaque gouvernorat.

    Branché sur le format produit par tft_dataset_builder.build_tft_dataset().
    """

    def __init__(self, df: pd.DataFrame, config: TFTConfig,
                 scalers: Dict, encoders: Dict, is_train: bool = True):
        self.config  = config
        self.scalers = scalers
        self.encoders = encoders
        self.samples = []

        Tenc = config.encoder_length
        Tdec = config.decoder_length
        T    = Tenc + Tdec

        for (gov, groupe), group in df.groupby(['gouvernorat', 'groupe']):
            group = group.sort_values(['annee', 'mois']).reset_index(drop=True)
            if len(group) < config.min_series_length:
                continue

            # ── Split temporel strict (time-series safe) ──────────────
            # cut = frontière train/val, calculée UNE SEULE FOIS pour les deux.
            # BUG corrigé : avant, cut=n pour is_train=False
            # → data = group.iloc[n-Tenc:] = seulement Tenc points
            # → impossible de générer T=Tenc+Tdec séquences → val=0.
            # FIX : cut symétrique, val démarre Tenc pts AVANT cut (overlap encoder).
            n   = len(group)
            cut = int(n * (1 - config.val_ratio))
            cut = max(cut, Tenc)   # garantit au moins Tenc pts d'historique en train

            if is_train:
                # Train : du début jusqu'à cut (strict — pas de fuite temporelle)
                data = group.iloc[:cut]
            else:
                # Val : overlap de Tenc pts pour fournir l'historique encoder,
                # puis toute la fin → garantit >= 1 séquence si n >= cut + Tdec
                data = group.iloc[max(0, cut - Tenc):]

            data  = data.reset_index(drop=True)
            n_eff = len(data)

            # Sliding window
            for start in range(0, n_eff - T + 1):
                enc_slice = data.iloc[start       : start + Tenc]
                dec_slice = data.iloc[start + Tenc: start + T   ]

                sample = self._build_sample(enc_slice, dec_slice, group)
                if sample is not None:
                    self.samples.append(sample)

    def _build_sample(self, enc: pd.DataFrame, dec: pd.DataFrame,
                      group: pd.DataFrame) -> Optional[Dict]:
        cfg = self.config

        # ── Statiques (pris sur la première ligne du groupe) ──────
        row0 = group.iloc[0]

        cat_static = {}
        for feat in cfg.static_cat_features:
            col = feat.replace('_enc', '')  # gouvernorat_enc → gouvernorat
            if col in self.encoders:
                raw_val = row0[col]
                val_str = 'Unknown' if pd.isna(raw_val) else str(raw_val)
                le = self.encoders[col]
                # Normalisation str identique à preparer_dataset_tft
                val_str = (str(val_str).strip()
                           .replace('nan','unknown').replace('None','unknown')
                           .replace('NaN','unknown'))
                if not val_str:
                    val_str = 'unknown'
                if val_str not in le.classes_:
                    # Plus de WARN intempestif : fallback silencieux sur classe 0
                    # (le nettoyage amont dans preparer_dataset_tft garantit
                    #  que ce cas ne devrait plus se produire)
                    fallback = 'unknown' if 'unknown' in le.classes_ else le.classes_[0]
                    val = le.transform([fallback])[0]
                else:
                    val = le.transform([val_str])[0]
            else:
                val = int(row0.get(feat, 0))
            cat_static[feat] = torch.tensor(val, dtype=torch.long)

        real_static_vals = []
        for feat in cfg.static_real_features:
            real_static_vals.append(float(row0.get(feat, 0.0)))
        real_static = torch.tensor(real_static_vals, dtype=torch.float32)

        # ── Encoder : known + unknown ──────────────────────────────
        known_enc_vals = []
        for t in range(len(enc)):
            row = enc.iloc[t]
            known_enc_vals.append([float(row.get(f, 0.0))
                                    for f in cfg.known_real_features])
        known_enc = torch.tensor(known_enc_vals, dtype=torch.float32)

        unknown_enc_vals = []
        for t in range(len(enc)):
            row = enc.iloc[t]
            unknown_enc_vals.append([float(row.get(f, 0.0))
                                      for f in cfg.unknown_real_features])
        unknown_enc = torch.tensor(unknown_enc_vals, dtype=torch.float32)

        # ── Decoder : known only (le futur est inconnu) ────────────
        known_dec_vals = []
        for t in range(len(dec)):
            row = dec.iloc[t]
            known_dec_vals.append([float(row.get(f, 0.0))
                                    for f in cfg.known_real_features])
        known_dec = torch.tensor(known_dec_vals, dtype=torch.float32)

        # ── Target : prix aux horizons voulus ─────────────────────
        target_vals = []
        for h in cfg.predict_horizons:
            if h < len(dec):
                target_vals.append(float(dec.iloc[h].get(cfg.target_col, 0.0)))
            else:
                return None  # série trop courte
        target = torch.tensor(target_vals, dtype=torch.float32)

        # ── Prix brut decoder (pour dénormalisation) ───────────────
        raw_vals = [float(dec.iloc[min(h, len(dec)-1)].get(cfg.target_raw_col, 0.0))
                    for h in cfg.predict_horizons]
        raw = torch.tensor(raw_vals, dtype=torch.float32)

        return {
            'cat_static'  : cat_static,
            'real_static' : real_static,
            'known_enc'   : known_enc,
            'unknown_enc' : unknown_enc,
            'known_dec'   : known_dec,
            'target'      : target,
            'raw_target'  : raw,
            'gouvernorat' : torch.tensor(int(group.iloc[0]['gouvernorat']),
                                          dtype=torch.long),
            'groupe'      : str(group.iloc[0]['groupe']),
        }

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        return self.samples[idx]


def collate_fn(batch: List[Dict]) -> Dict:
    """Collate pour DataLoader — gère le dict de catégoriels."""
    result = {}
    keys = batch[0].keys()
    for k in keys:
        if k == 'cat_static':
            result[k] = {
                feat: torch.stack([b[k][feat] for b in batch])
                for feat in batch[0][k].keys()
            }
        elif isinstance(batch[0][k], torch.Tensor):
            result[k] = torch.stack([b[k] for b in batch])
        else:
            result[k] = [b[k] for b in batch]
    return result


# ================================================================
# 6. PRÉPARATION DES DONNÉES
# ================================================================

def preparer_dataset_tft(tft_df: pd.DataFrame,
                          config: TFTConfig) -> Tuple[pd.DataFrame, Dict, Dict, Dict]:
    """
    Normalise et encode le DataFrame TFT.
    Branché sur le format de tft_dataset_builder.build_tft_dataset().

    Retourne
    --------
    df_prepared : DataFrame enrichi avec colonnes normalisées
    scalers     : {col: RobustScaler}  — pour dénormalisation
    encoders    : {col: LabelEncoder}  — pour catégoriels
    cat_cardinalities : {feat: n_categories}
    """
    df = tft_df.copy()

    # ── Stabilisation zone_geo AVANT tout encodage ────────────────
    # Corrige le warning [WARN] unseen static label zone_geo=0 -> ...
    # Cause : NaN ou types mixtes (int/float/str) → LabelEncoder instable.
    # Correction : normaliser en str propre sur tout le DataFrame.
    if 'zone_geo' in df.columns:
        df['zone_geo'] = (df['zone_geo']
                          .fillna('unknown')
                          .astype(str)
                          .str.strip()
                          .replace({'nan': 'unknown', 'None': 'unknown', '': 'unknown'}))
    else:
        df['zone_geo'] = 'unknown'

    # ── Encodage cyclique du mois (si pas déjà fait) ──────────────
    if 'mois_sin' not in df.columns:
        df['mois_sin'] = np.sin(2 * np.pi * df['mois'] / 12)
        df['mois_cos'] = np.cos(2 * np.pi * df['mois'] / 12)

    if 'annee_norm' not in df.columns:
        df['annee_norm'] = (df['annee'] - df['annee'].min()) / \
                            max(df['annee'].max() - df['annee'].min(), 1)

    if 'saison' not in df.columns:
        df['saison'] = df['mois'].apply(
            lambda m: 1 if m in (6,7,8) else (2 if m in (12,1,2) else 3))

    # ── Normalisation features réelles ───────────────────────────
    scalers = {}
    cols_to_scale = {
        'prix_m2'           : 'prix_m2_norm',
        'prix_m2_loc'       : 'prix_m2_loc_norm',
        'prix_m2_ven'       : 'prix_m2_ven_norm',
        'volatilite_rolling': 'volatilite_rolling_norm',
        'nb_annonces_proxy' : 'nb_annonces_proxy_norm',
        'indice_liquidite'  : 'indice_liquidite_norm',
        'volatilite_prix_trim': 'volatilite_prix_trim_norm',
        'cluster_size'      : 'cluster_size_norm',
    }

    for raw_col, norm_col in cols_to_scale.items():
        col_src = raw_col
        # Cherche aussi les variantes de noms
        if col_src not in df.columns:
            col_src = None
            for alias in [raw_col, raw_col.replace('_norm',''),
                          raw_col + '_lisse']:
                if alias in df.columns:
                    col_src = alias
                    break
        if col_src is None:
            df[norm_col] = 0.0
            continue
        sc = RobustScaler()
        vals = df[col_src].fillna(0.0).values.reshape(-1, 1)
        df[norm_col] = sc.fit_transform(vals).flatten()
        scalers[norm_col] = {'scaler': sc, 'source_col': col_src}

    # ── Encodage catégoriels ──────────────────────────────────────
    # CORRECTION zone_geo : normalisation str AVANT LabelEncoder sur TOUTES
    # les variables catégorielles statiques — évite les "unseen label" warnings
    # causés par des NaN, types mixtes (int/float/str) ou valeurs vides.
    encoders = {}
    cat_cardinalities = {}
    for feat_enc in config.static_cat_features:
        col = feat_enc.replace('_enc', '')
        # Gestion des colonnes de groupe (Residentiel, Foncier, Commercial)
        if col == 'groupe' and 'groupe' not in df.columns:
            df['groupe'] = 'Residentiel'
        if col not in df.columns:
            df[feat_enc] = 0
            cat_cardinalities[feat_enc] = 1
            continue

        # Normalisation robuste : NaN → "unknown", int/float → str, strip
        df[col] = (df[col]
                   .fillna('unknown')
                   .astype(str)
                   .str.strip()
                   .replace({'nan': 'unknown', 'None': 'unknown',
                              '': 'unknown', 'NaN': 'unknown'}))

        le = LabelEncoder()
        le.fit(sorted(df[col].unique()))          # fit sur valeurs triées → stable
        df[feat_enc] = le.transform(df[col])
        encoders[col] = le
        cat_cardinalities[feat_enc] = len(le.classes_)
        print(f"   [ENC] {col:<20} → {len(le.classes_)} catégories : {list(le.classes_)[:8]}"
              + ("..." if len(le.classes_) > 8 else ""))

    # ── Remplissage NaN features PELT ────────────────────────────
    for col in ['rupture_flag', 'score_rupture', 'post_rupture']:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    # ── Remplissage NaN features statiques ───────────────────────
    for col in config.static_real_features:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    return df, scalers, encoders, cat_cardinalities


# ================================================================
# 7. ENTRAÎNEMENT
# ================================================================

def train_tft(tft_df: pd.DataFrame,
              config: TFTConfig,
              models_dir: str = 'models') -> Tuple[TFTModel, Dict]:
    """
    Pipeline complet d'entraînement TFT.

    1. Prépare le dataset (normalisation, encodage)
    2. Crée train/val DataLoaders
    3. Entraîne avec early stopping
    4. Sauvegarde le modèle + scalers + encoders

    Retourne
    --------
    model   : TFTModel entraîné
    history : dict {train_loss, val_loss, best_epoch}
    """
    print("\n" + "=" * 72)
    print("  TFT — TEMPORAL FUSION TRANSFORMER · MARCHÉ IMMOBILIER TUNISIEN")
    print("=" * 72)
    print(f"  Device      : {DEVICE}")
    print(f"  Encoder     : {config.encoder_length} mois")
    print(f"  Decoder     : {config.decoder_length} mois")
    print(f"  Horizons    : T+{config.predict_horizons[0]+1} / "
          f"T+{config.predict_horizons[1]+1} / T+{config.predict_horizons[2]+1}")
    print(f"  Quantiles   : {config.quantiles}")
    print(f"  d_model     : {config.d_model}")

    os.makedirs(models_dir, exist_ok=True)

    # ── Préparation données ───────────────────────────────────────
    print("\n  Préparation dataset...")
    df_prep, scalers, encoders, cat_card = preparer_dataset_tft(tft_df, config)
    print(f"  Gouvernorats : {df_prep['gouvernorat'].nunique()}")
    print(f"  Lignes       : {len(df_prep):,}")
    print(f"  Catégoriels  : {cat_card}")

    # ── Datasets ──────────────────────────────────────────────────
    ds_train = TFTDataset(df_prep, config, scalers, encoders, is_train=True)
    ds_val   = TFTDataset(df_prep, config, scalers, encoders, is_train=False)

    # ── Debug split — diagnostique val=0 ──────────────────────────
    n_total_series = df_prep.groupby(['gouvernorat','groupe']).size()
    n_series_ok    = (n_total_series >= config.min_series_length).sum()
    need           = config.encoder_length + config.decoder_length
    print(f"  Split temporel (val_ratio={config.val_ratio}) :")
    print(f"    Séries >= min_series_length ({config.min_series_length}) : {n_series_ok}")
    print(f"    Fenêtre minimale nécessaire  : encoder({config.encoder_length})"
          f" + decoder({config.decoder_length}) = {need} points")
    print(f"  Séquences train : {len(ds_train)} | val : {len(ds_val)}")

    if len(ds_val) == 0:
        print(f"\n  [WARN] ⚠ Validation VIDE — diagnostique :")
        print(f"    • Séries trop courtes ? need={need}, "
              f"min_series_length={config.min_series_length}")
        print(f"    • val_ratio={config.val_ratio} → "
              f"essayer 0.25 pour réduire le cut")
        print(f"    • Forçage : entraînement sans validation "
              f"(early stopping sur train_loss)")
    if len(ds_val) == 0 and len(ds_train) == 0:
        raise ValueError(
            "Dataset train ET val vides. Vérifier min_series_length "
            f"({config.min_series_length}) vs encoder+decoder ({need})."
        )

    if len(ds_train) == 0:
        raise ValueError("Dataset d'entraînement vide. Vérifier min_series_length "
                         f"({config.min_series_length}) et encoder_length "
                         f"({config.encoder_length}).")

    # Si val vide : on monitore sur train_loss pour éviter early stop biaisé (val_loss=0)
    use_val = len(ds_val) > 0
    print(f"  Monitoring early stopping : {'val_loss' if use_val else 'train_loss (val vide)'}")

    dl_train = DataLoader(ds_train, batch_size=config.batch_size,
                          shuffle=True,  collate_fn=collate_fn,
                          num_workers=0, pin_memory=False)
    dl_val   = DataLoader(ds_val,   batch_size=config.batch_size,
                          shuffle=False, collate_fn=collate_fn,
                          num_workers=0, pin_memory=False)

    # ── Modèle ────────────────────────────────────────────────────
    model = TFTModel(config, cat_card).to(DEVICE)
    n_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"  Paramètres   : {n_params:,}")

    criterion = QuantileLoss(config.quantiles).to(DEVICE)
    optimizer = torch.optim.AdamW(model.parameters(),
                                   lr=config.learning_rate,
                                   weight_decay=config.weight_decay)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5,
        patience=config.patience // 3, min_lr=1e-6)

    # ── Boucle d'entraînement ─────────────────────────────────────
    history = {'train_loss': [], 'val_loss': [], 'best_epoch': 0}
    best_val  = float('inf')
    patience_cnt = 0
    best_state   = None

    print(f"\n  Entraînement ({config.max_epochs} epochs max, patience={config.patience})...")

    for epoch in range(1, config.max_epochs + 1):
        # ── Train ─────────────────────────────────────────────────
        model.train()
        train_loss = 0.0
        for batch in dl_train:
            cat_s = {k: v.to(DEVICE) for k, v in batch['cat_static'].items()}
            rs    = batch['real_static'].to(DEVICE)
            ke    = batch['known_enc'].to(DEVICE)
            ue    = batch['unknown_enc'].to(DEVICE)
            kd    = batch['known_dec'].to(DEVICE)
            tgt   = batch['target'].to(DEVICE)

            optimizer.zero_grad()
            pred, _, _ = model(cat_s, rs, ke, ue, kd)  # (B, n_horizons, n_q)
            loss = criterion(pred, tgt)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), config.gradient_clip)
            optimizer.step()
            train_loss += loss.item()

        train_loss /= max(len(dl_train), 1)

        # ── Validation + calibration ──────────────────────────────
        model.eval()
        val_loss     = 0.0
        all_pred     = []
        all_tgt      = []
        with torch.no_grad():
            for batch in dl_val:
                cat_s = {k: v.to(DEVICE) for k, v in batch['cat_static'].items()}
                rs    = batch['real_static'].to(DEVICE)
                ke    = batch['known_enc'].to(DEVICE)
                ue    = batch['unknown_enc'].to(DEVICE)
                kd    = batch['known_dec'].to(DEVICE)
                tgt   = batch['target'].to(DEVICE)
                pred, _, _ = model(cat_s, rs, ke, ue, kd)
                val_loss  += criterion(pred, tgt).item()
                all_pred.append(pred.cpu())
                all_tgt.append(tgt.cpu())

        val_loss /= max(len(dl_val), 1)

        # ── Calibration (coverage P10–P90) — calculée à chaque époque ─
        # Coverage idéale : 80% des vraies valeurs dans [P10, P90].
        # Sous 60% → modèle sous-estimé l'incertitude.
        # Au-dessus de 90% → modèle trop conservateur.
        coverage_str = ""
        if all_pred and epoch % 10 == 0:
            all_pred_t = torch.cat(all_pred, dim=0)   # (N, n_horizons, n_q)
            all_tgt_t  = torch.cat(all_tgt,  dim=0)   # (N, n_horizons)
            p10 = all_pred_t[:, :, 0]  # (N, H)
            p90 = all_pred_t[:, :, 2]  # (N, H)
            inside = ((all_tgt_t >= p10) & (all_tgt_t <= p90)).float().mean()
            coverage_str = f" | cov80%={inside.item()*100:.0f}%"
            history.setdefault('coverage_80', []).append(float(inside.item()))

        # ── Métrique de monitoring : val_loss si dispo, sinon train_loss ──
        # Évite le biais val_loss=0 quand le DataLoader val est vide.
        monitor_loss = val_loss if use_val else train_loss

        scheduler.step(monitor_loss)
        history['train_loss'].append(train_loss)
        history['val_loss'].append(val_loss)

        if epoch % 10 == 0 or epoch == 1:
            val_str = f"{val_loss:.4f}" if use_val else "N/A (val vide)"
            print(f"  Ep {epoch:>3d}/{config.max_epochs} | "
                  f"train={train_loss:.4f} | val={val_str} | "
                  f"lr={optimizer.param_groups[0]['lr']:.2e}{coverage_str}")

        # ── Early stopping basé sur monitor_loss ───────────────────
        if monitor_loss < best_val - config.min_delta:
            best_val     = monitor_loss
            patience_cnt = 0
            history['best_epoch'] = epoch
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            patience_cnt += 1
            if patience_cnt >= config.patience:
                metric_name = "val_loss" if use_val else "train_loss"
                print(f"  Early stop à epoch {epoch} "
                      f"(meilleur {metric_name}: epoch {history['best_epoch']}, "
                      f"loss={best_val:.4f})")
                break

    # ── Restaurer meilleurs poids ─────────────────────────────────
    if best_state is not None:
        model.load_state_dict({k: v.to(DEVICE) for k, v in best_state.items()})

    print(f"\n  Entraînement terminé | meilleur val_loss={best_val:.4f} "
          f"(epoch {history['best_epoch']})")

    # ── Sauvegarde ────────────────────────────────────────────────
    torch.save(model.state_dict(), os.path.join(models_dir, 'tft_model.pt'))
    with open(os.path.join(models_dir, 'tft_meta.pkl'), 'wb') as f:
        pickle.dump({
            'config'   : config,
            'scalers'  : scalers,
            'encoders' : encoders,
            'cat_card' : cat_card,
            'history'  : history,
        }, f)

    print(f"  → tft_model.pt + tft_meta.pkl sauvegardés dans {models_dir}/")
    return model, history


# ================================================================
# 8. INFÉRENCE
# ================================================================

def predict_tft(model: TFTModel,
                tft_df: pd.DataFrame,
                config: TFTConfig,
                scalers: Dict,
                encoders: Dict) -> pd.DataFrame:
    """
    Génère les prédictions TFT pour tous les gouvernorats.

    Retourne un DataFrame avec colonnes :
      gouvernorat | groupe | horizon | q10 | q50 | q90 | q10_tnd | q50_tnd | q90_tnd
    """
    model.eval()
    df_prep, _, _, _ = preparer_dataset_tft(tft_df, config)

    results = []
    horizon_labels = ['T+1', 'T+3', 'T+12']

    with torch.no_grad():
        for (gov, groupe), group in df_prep.groupby(['gouvernorat', 'groupe']):
            group = group.sort_values(['annee', 'mois']).reset_index(drop=True)
            if len(group) < config.encoder_length:
                continue

            # Prendre la dernière fenêtre disponible
            enc = group.iloc[-config.encoder_length:]
            row0 = group.iloc[0]

            # Statiques
            cat_s = {}
            for feat in config.static_cat_features:
                col = feat.replace('_enc', '')
                if col in encoders:
                    le = encoders[col]
                    raw_val = row0.get(col, 'Unknown')
                    val_str = 'Unknown' if pd.isna(raw_val) else str(raw_val)
                    if val_str not in le.classes_:
                        val_str = le.classes_[0]
                    val = le.transform([val_str])[0]
                else:
                    val = int(row0.get(feat, 0))
                cat_s[feat] = torch.tensor([val], dtype=torch.long, device=DEVICE)

            rs = torch.tensor(
                [[float(row0.get(f, 0.0)) for f in config.static_real_features]],
                dtype=torch.float32, device=DEVICE)

            # Encoder
            ke_vals = [[float(r.get(f, 0.0)) for f in config.known_real_features]
                       for _, r in enc.iterrows()]
            ue_vals = [[float(r.get(f, 0.0)) for f in config.unknown_real_features]
                       for _, r in enc.iterrows()]
            ke = torch.tensor([ke_vals], dtype=torch.float32, device=DEVICE)
            ue = torch.tensor([ue_vals], dtype=torch.float32, device=DEVICE)

            # Decoder (on projette les known futures — mois suivants)
            last_row = group.iloc[-1]
            last_annee = int(last_row['annee'])
            last_mois  = int(last_row['mois'])
            kd_vals = []
            for step in range(config.decoder_length):
                total_months = last_annee * 12 + last_mois + step + 1
                m  = (total_months - 1) % 12 + 1
                yr = (total_months - 1) // 12
                yr_norm = (yr - group['annee'].min()) / max(
                    group['annee'].max() - group['annee'].min(), 1)
                saison = 1 if m in (6,7,8) else (2 if m in (12,1,2) else 3)
                hs = 1.0 if m in (3,4,5,9,10,11) else 0.0
                known_row = [
                    np.sin(2*np.pi*m/12),
                    np.cos(2*np.pi*m/12),
                    yr_norm,
                    hs,
                    float(saison),
                    float(last_row.get('inflation_glissement_annuel', 5.0)),
                    float(last_row.get('croissance_pib_trim', 2.0)),
                    float(last_row.get('glissement_immo_trim', 5.0)),
                ]
                kd_vals.append(known_row)

            kd = torch.tensor([kd_vals], dtype=torch.float32, device=DEVICE)

            pred, _, _ = model(cat_s, rs, ke, ue, kd)
            pred_np = pred[0].cpu().numpy()               # (n_horizons, n_q)

            # Dénormalisation (P50 seulement — les autres en proportion)
            sc_info = scalers.get('prix_m2_norm', {})
            if sc_info:
                sc   = sc_info['scaler']
                prix_ref = float(group['prix_m2'].dropna().iloc[-1]) \
                           if 'prix_m2' in group.columns else 1.0
            else:
                prix_ref = 1.0

            for i, hlabel in enumerate(horizon_labels):
                q10_n, q50_n, q90_n = pred_np[i]
                # Dénormaliser via scaler si disponible
                if sc_info:
                    q50_tnd = float(sc_info['scaler'].inverse_transform([[q50_n]])[0][0])
                    q10_tnd = float(sc_info['scaler'].inverse_transform([[q10_n]])[0][0])
                    q90_tnd = float(sc_info['scaler'].inverse_transform([[q90_n]])[0][0])
                else:
                    q50_tnd = q50_n
                    q10_tnd = q10_n
                    q90_tnd = q90_n

                results.append({
                    'gouvernorat': int(gov),
                    'groupe'     : str(groupe),
                    'horizon'    : hlabel,
                    'q10_norm'   : round(float(q10_n), 4),
                    'q50_norm'   : round(float(q50_n), 4),
                    'q90_norm'   : round(float(q90_n), 4),
                    'q10_tnd'    : round(q10_tnd, 0),
                    'q50_tnd'    : round(q50_tnd, 0),
                    'q90_tnd'    : round(q90_tnd, 0),
                    'incertitude': round(abs(q90_tnd - q10_tnd), 0),
                })

    return pd.DataFrame(results)


# ================================================================
# 9. POINT D'ENTRÉE
# ================================================================

if __name__ == '__main__':
    import sys

    BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
    MODELS_DIR = os.path.join(BASE_DIR, 'models')

    print("Chargement du dataset TFT...")
    tft_pkl = os.path.join(MODELS_DIR, 'tft_dataset.pkl')
    if not os.path.exists(tft_pkl):
        print(f"✗ tft_dataset.pkl introuvable : {tft_pkl}")
        print("  → Exécutez d'abord : python tft_dataset_builder.py")
        sys.exit(1)

    with open(tft_pkl, 'rb') as f:
        data = pickle.load(f)
    tft_df = data['df']
    print(f"✔ Dataset : {len(tft_df):,} lignes | {tft_df['gouvernorat'].nunique()} gouvernorats")

    config = TFTConfig()
    model, history = train_tft(tft_df, config, MODELS_DIR)

    print("\nGénération des prédictions...")
    with open(os.path.join(MODELS_DIR, 'tft_meta.pkl'), 'rb') as f:
        meta = pickle.load(f)

    preds = predict_tft(model, tft_df, config, meta['scalers'], meta['encoders'])
    preds.to_csv(os.path.join(MODELS_DIR, 'tft_predictions.csv'), index=False)
    print(f"✔ Prédictions sauvegardées : models/tft_predictions.csv")
    print(preds.groupby(['groupe', 'horizon'])[['q50_tnd','incertitude']].mean().round(0))