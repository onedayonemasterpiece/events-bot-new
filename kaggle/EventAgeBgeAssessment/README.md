# Event Age BGE Assessment (CPU)

Shadow/gated batch worker. It never writes the event DB and never changes a
declared rating.

Input dataset files:

- `event_age_bge_input.jsonl`: `event_id`, `input_hash`, whole-product `text`,
  optional `declared_age`;
- `event_age_bge_prototypes.json`: versioned prototype metadata/text;
- `event_age_bge_prototype_vectors.npz`: `vectors`, `model_revision`,
  `encoder_contract`, `prototype_bank_hash`;
- optional `event_age_bge_classifier.npz`: `classes`, `weights_a`, `bias_a`,
  `weights_b`, `bias_b`;
- optional `event_age_bge_evaluation.json`: explicit approval plus exact hashes
  and calibrated `head_a_min_probability` / `head_b_min_probability`.

The Kaggle kernel has `enable_gpu=false`. `EVENT_AGE_BGE_MODEL_REVISION` is
mandatory. Missing/mismatched classifier approval produces abstentions.

Launch through the standard status ledger/heartbeat path:

```bash
python kaggle/execute_event_age_bge_assessment.py \
  --dataset-source USER/PREPARED-DATASET --run-id event-age-bge-canary
```

Outputs: `event_age_bge_result.json` and a partial checkpoint under
`/kaggle/working`.

