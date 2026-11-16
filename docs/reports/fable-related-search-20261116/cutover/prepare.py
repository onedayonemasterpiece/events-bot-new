"""Build local cutover data from a fresh preview and a read-only v2 dump."""
import argparse
import csv
import hashlib
import json
import os
import sys
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("checkout", type=Path)
p.add_argument("preview", type=Path)
p.add_argument("live_csv", type=Path)
p.add_argument("archive_vectors", type=Path)
p.add_argument("output_directory", type=Path)
p.add_argument("--complete-missing", action="store_true",
               help="Request only missing U inputs through the shared embedding limiter")
args = p.parse_args()
sys.path.insert(0, str(args.checkout.resolve()))
from scripts import sync_event_search_vectors_to_supabase as sync

model, dim = "gemini-embedding-2", 768
out = args.output_directory
out.mkdir(mode=0o700, exist_ok=True)
preview = json.loads(args.preview.read_text())
events = preview["events"]
catalog = sync.exported_search_catalog_revision(events)
if not events or preview["build"]["catalog_revision"] != catalog:
    sys.exit("An exact, nonempty full fixture and its catalog_revision are required")
if any("canonical_search_digest" not in event for event in events):
    sys.exit("Regenerate the preview with the candidate exporter")
changes = sync.build_occurrence_projections(events)
for event in events:
    event.update(changes.get(int(event["id"]), {}))
docs = [sync.build_search_doc(event, site_origin="https://kenigevents.ru", base_path="",
                              ics_base_url="https://static.kenigevents.ru/ics") for event in events]
revision = sync.build_revision_contract(docs, catalog_revision=catalog, embedding_model=model, embedding_dim=dim)
sync.annotate_search_documents(docs, revision)
with args.live_csv.open() as stream:
    live = {int(i): (old_hash, search_hash, json.loads(vector))
            for i, old_hash, search_hash, vector in csv.reader(stream)}
if len(docs) != len(live) or set(live) != {doc.event_id for doc in docs}:
    sys.exit("Full projected membership differs from the complete v2 sidecar")
archive_sha = hashlib.sha256(args.archive_vectors.read_bytes()).hexdigest()
if archive_sha != "f03ae7cd7a38670e0d75136923427753d94cf44719a15feb553203a41fb77b70":
    sys.exit("Unexpected archived vectors.jsonl digest")
with args.archive_vectors.open() as stream:
    known = {row["input_hash"]: row["U"] for row in map(json.loads, stream)}
cache = out / "facts-cache.jsonl"
if cache.exists():
    with cache.open() as stream:
        for row in map(json.loads, stream):
            if row["model"] == model and row["dim"] == dim:
                known[row["hash"]] = row["vector"]
inputs = {}
for doc in docs:
    old_hash, search_hash, _ = live[doc.event_id]
    if old_hash != sync.sha256_text(doc.related_embedding_input) or search_hash != doc.document["text_hash"]:
        sys.exit(f"Refresh the v2 sidecar and fixture for event {doc.event_id}")
    inputs[sync.sha256_text(doc.related_facts_input)] = doc.related_facts_input
missing = sorted(inputs.keys() - known.keys())
print(json.dumps({"events":len(docs), "new_facts_inputs":len(missing),
                  "related_corpus_revision": revision["corpora"]["related_v1"]["revision"]}), flush=True)
if missing and not args.complete_missing:
    sys.exit("Re-run with --complete-missing for the measured number of provider inputs")
if missing:
    client = sync.build_embedding_client(key_env="GOOGLE_API_KEY4")
    with cache.open("a") as stream:
        for key in missing:
            vec = sync.gemini_embed(inputs[key], model=model, dim=dim, client=client)
            stream.write(json.dumps({"model":model,"dim":dim,"hash":key,"vector":vec})+"\n")
            stream.flush()
            os.fsync(stream.fileno())
            known[key] = vec

tmp = out / "forward.csv.tmp"
with tmp.open("w",newline="") as stream:
    writer = csv.writer(stream)
    for doc in docs:
        i = doc.event_id
        old_hash, search_hash, old = live[i]
        facts_key = sync.sha256_text(doc.related_facts_input)
        new = sync.equal_mix(old, known[facts_key], dim=dim)
        dm = doc.document["metadata"]
        rk = ("related_text_hash","catalog_revision","corpus_revision","search_document_revision",
              "search_doc_version","search_corpus_revision","related_corpus_revision")
        payload = {"id":i,"sh":search_hash,"bh":old_hash,"fh":facts_key,
                   "source_vec":old,"h":doc.document["related_text_hash"],
                   "v":doc.document["related_doc_version"],"vec":new,
                   "dm":{k:dm[k] for k in rk}}
        for key, kind in (("sm","search_v3"),("rm","related_v1")):
            payload[key]=sync.embedding_revision_metadata(doc,document_kind=kind,catalog_revision=catalog,revision_contract=revision)
        writer.writerow([json.dumps(payload, separators=(",",":"))])
tmp.replace(out / "forward.csv")
(out / "revision.json").write_text(json.dumps(revision,indent=2)+"\n")
