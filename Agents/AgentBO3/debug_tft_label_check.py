import os
import pickle
import pandas as pd
from tft_model import TFTConfig, preparer_dataset_tft

print('cwd', os.getcwd())

pkl = os.path.join(os.getcwd(), 'models', 'tft_dataset.pkl')
print('tft_dataset exists', os.path.exists(pkl))
if not os.path.exists(pkl):
    raise SystemExit('missing tft_dataset.pkl')

with open(pkl, 'rb') as f:
    data = pickle.load(f)

if isinstance(data, dict) and 'df' in data:
    df = data['df']
else:
    raise SystemExit('unexpected tft_dataset.pkl format')

print('df cols', df.columns.tolist())
print('groupe unique', df['groupe'].astype(str).unique()[:20])
print('cluster unique', df['cluster'].astype(str).unique()[:20])
print('zone_geo unique', df['zone_geo'].astype(str).unique()[:20])
print('gouvernorat unique first', df['gouvernorat'].astype(str).unique()[:20])

cfg = TFTConfig()
df_prep, scalers, encoders, cat = preparer_dataset_tft(df, cfg)
print('cat card', cat)
for col, le in encoders.items():
    print('encoder', col, 'classes', list(le.classes_)[:30], 'len', len(le.classes_))

for name, group in df.groupby(['gouvernorat', 'groupe']):
    row0 = group.iloc[0]
    for feat in cfg.static_cat_features:
        col = feat.replace('_enc', '')
        if col in encoders:
            v = str(row0[col])
            if v not in list(encoders[col].classes_):
                print('missing label', col, v)
                raise SystemExit('missing label found')
    break

print('diagnostic complete')
