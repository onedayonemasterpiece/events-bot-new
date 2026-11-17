\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout='10s';
SET LOCAL search_path=pg_catalog,extensions;
SET LOCAL TIME ZONE 'UTC';
CREATE TEMP TABLE load_row (j jsonb NOT NULL) ON COMMIT DROP;
\copy load_row FROM 'forward.csv' WITH (FORMAT csv)
CREATE TEMP TABLE stage ON COMMIT DROP AS SELECT (j->>'id')::bigint AS id, j FROM load_row;
ALTER TABLE stage ADD PRIMARY KEY (id);
LOCK TABLE public.event_search_documents, public.event_embeddings IN SHARE ROW EXCLUSIVE MODE;
CREATE TEMP VIEW current_release AS
SELECT d.event_id AS id, d.text_hash AS sh, d.related_text_hash AS h, d.related_doc_version AS v,
       d.metadata AS dm, e.text_hash AS eh, e.embedding::text AS vec, e.metadata AS rm, e.embedded_at AS ts,
       s.metadata AS sm, s.text_hash AS search_embedding_hash
FROM public.event_search_documents d
JOIN public.event_embeddings e ON e.event_id=d.event_id AND e.embedding_model='gemini-embedding-2' AND e.embedding_dim=768 AND e.embedding_doc_kind='related_v1'
JOIN public.event_embeddings s ON s.event_id=d.event_id AND s.embedding_model=e.embedding_model AND s.embedding_dim=e.embedding_dim AND s.embedding_doc_kind='search_v3';
DO $guard$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM stage)
    OR EXISTS (SELECT event_id FROM public.event_search_documents EXCEPT SELECT id FROM stage)
    OR EXISTS (SELECT id FROM stage EXCEPT SELECT id FROM current_release)
    OR EXISTS (SELECT FROM stage t JOIN current_release c USING(id)
      WHERE c.v IS DISTINCT FROM 'event-related-doc-v2-content-only'
       OR c.rm->>'doc_version' IS DISTINCT FROM c.v
       OR c.h IS DISTINCT FROM t.j->>'bh' OR c.eh IS DISTINCT FROM c.h
       OR c.sh IS DISTINCT FROM t.j->>'sh' OR c.search_embedding_hash IS DISTINCT FROM c.sh
       OR (c.vec)::extensions.vector(768) IS DISTINCT FROM (t.j->>'source_vec')::extensions.vector(768)
       OR t.j->>'v' IS DISTINCT FROM 'event-related-doc-v3-equal-content-facts'
       OR (t.j->>'h') IS NULL OR (t.j->>'fh') IS NULL
       OR t.j#>>'{dm,related_text_hash}' IS DISTINCT FROM t.j->>'h'
       OR t.j#>>'{rm,doc_version}' IS DISTINCT FROM t.j->>'v'
       OR t.j#>>'{sm,related_doc_version}' IS DISTINCT FROM t.j->>'v'
       OR t.j#>>'{rm,embedding_corpus_revision}' IS DISTINCT FROM t.j#>>'{dm,related_corpus_revision}'
       OR abs(extensions.vector_norm((t.j->>'vec')::extensions.vector(768))-1) > 0.00001)
    THEN RAISE EXCEPTION 'Stale or incomplete catalog, document, source vector or stage'; END IF;
END $guard$;
CREATE TEMP TABLE transition ON COMMIT DROP AS
SELECT c.id, to_jsonb(c) AS before, to_jsonb(c) || jsonb_build_object(
    'h', j->'h','eh', j->'h','v', j->'v','vec',((j->>'vec')::extensions.vector(768))::text,
    'dm', c.dm || (j->'dm'), 'rm', c.rm || (j->'rm'), 'sm', c.sm || (j->'sm'),
    'ts', transaction_timestamp()) AS after
FROM current_release c JOIN stage USING(id);
\copy (SELECT jsonb_build_object('before',before,'after',after) FROM transition ORDER BY id) TO 'rollback.csv' WITH (FORMAT csv)
UPDATE public.event_search_documents d SET related_text_hash=t.after->>'h', related_doc_version=t.after->>'v', metadata=t.after->'dm'
FROM transition t WHERE t.id=d.event_id;
UPDATE public.event_embeddings e SET embedding=(t.after->>'vec')::extensions.vector(768), text_hash=t.after->>'h',
  embedded_at=(t.after->>'ts')::timestamptz, metadata=t.after->'rm'
FROM transition t WHERE t.id=e.event_id AND e.embedding_model='gemini-embedding-2' AND e.embedding_dim=768 AND e.embedding_doc_kind='related_v1';
UPDATE public.event_embeddings e SET metadata=t.after->'sm'
FROM transition t WHERE t.id=e.event_id AND e.embedding_model='gemini-embedding-2' AND e.embedding_dim=768 AND e.embedding_doc_kind='search_v3';
DO $verify$
BEGIN
  IF EXISTS (SELECT FROM transition t JOIN current_release c USING(id)
             WHERE to_jsonb(c) IS DISTINCT FROM t.after)
    THEN RAISE EXCEPTION 'Cutover readback differs'; END IF;
END $verify$;
COMMIT;
