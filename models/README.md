# ML models

| File | Purpose |
|------|---------|
| `mymodel.json` | XGBoost **dump_model** JSON (tree array) loaded by Go |
| `feature_map.txt` | Feature names → indices `0..7` |

Root `./mymodel.json` is the native ValSparks `save_model` file (used only for `base_score`).

## Convert after retrain

```bash
python3 -m venv .venv && .venv/bin/pip install xgboost
.venv/bin/python -c "
import xgboost as xgb
b = xgb.Booster()
b.load_model('mymodel.json')
b.dump_model('models/mymodel.json', dump_format='json')
"
```

Then rewrite named splits to `f0`..`f7` if needed (see repo history / predict tests), or keep
`feature_map.txt` aligned with training names.

Override path: `SNIPR_MODEL_PATH=models/mymodel.json`

## Feature order

0. `msg_count`  
1. `unique_users`  
2. `avg_msg_len`  
3. `max_repeat_count`  
4. `unique_norm_msgs`  
5. `entropy_raw`  
6. `hype_score` (fuzzy Jaccard over hype words, includes `www`)  
7. `repeat_ratio`  

Decision: `P(hype) >= 0.7` → clip (30s cooldown).
