-- Smart Update vector identity recall foundation.
-- Backend/service-role only: no public/browser API grants.

BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE OR REPLACE FUNCTION public.event_identity_candidates_by_embedding_v1(
    p_embedding vector,
    p_embedding_doc_kind text DEFAULT 'identity_candidate_v1',
    p_city text DEFAULT NULL,
    p_event_type text DEFAULT NULL,
    p_limit integer DEFAULT 8,
    p_min_similarity double precision DEFAULT 0.75
)
RETURNS TABLE (
    event_id bigint,
    document_id bigint,
    embedding_id bigint,
    embedding_doc_kind text,
    candidate_doc_kind text,
    similarity double precision,
    distance double precision,
    title text,
    event_date text,
    event_time text,
    end_date text,
    city text,
    event_type text,
    location_name text,
    location_address text,
    ticket_link text,
    source_url text,
    source_type text,
    telegraph_url text,
    tg_event_post_url text,
    source_vk_post_url text,
    document_hash text,
    document_text text,
    evidence jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, extensions
AS $$
DECLARE
    v_limit integer := greatest(1, least(coalesce(p_limit, 8), 50));
    v_embedding_col text;
    v_embedding_id_col text;
    v_embedding_document_id_col text;
    v_embedding_event_id_col text;
    v_embedding_kind_col text;
    v_document_id_col text;
    v_document_event_id_col text;
    v_document_kind_col text;
    v_join_sql text;
    v_sql text;
    v_distance_expr text;
    v_embedding_id_expr text := 'NULL::bigint';
    v_document_id_expr text := 'NULL::bigint';
    v_event_id_expr text := 'NULL::bigint';
    v_kind_expr text := 'NULL::text';
    v_title_expr text := 'NULL::text';
    v_event_date_expr text := 'NULL::text';
    v_event_time_expr text := 'NULL::text';
    v_end_date_expr text := 'NULL::text';
    v_city_expr text := 'NULL::text';
    v_event_type_expr text := 'NULL::text';
    v_location_name_expr text := 'NULL::text';
    v_location_address_expr text := 'NULL::text';
    v_ticket_link_expr text := 'NULL::text';
    v_source_url_expr text := 'NULL::text';
    v_source_type_expr text := 'NULL::text';
    v_telegraph_url_expr text := 'NULL::text';
    v_tg_event_post_url_expr text := 'NULL::text';
    v_source_vk_post_url_expr text := 'NULL::text';
    v_document_hash_expr text := 'NULL::text';
    v_document_text_expr text := 'NULL::text';
    v_city_filter text := 'TRUE';
    v_type_filter text := 'TRUE';
    v_kind_filter text := 'TRUE';
BEGIN
    IF to_regclass('public.event_embeddings') IS NULL THEN
        RAISE EXCEPTION 'event_embeddings table is required';
    END IF;
    IF to_regclass('public.event_search_documents') IS NULL THEN
        RAISE EXCEPTION 'event_search_documents table is required';
    END IF;

    SELECT column_name INTO v_embedding_col
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'event_embeddings'
      AND column_name IN ('embedding', 'embedding_vector', 'vector')
    ORDER BY array_position(ARRAY['embedding','embedding_vector','vector'], column_name)
    LIMIT 1;
    IF v_embedding_col IS NULL THEN
        RAISE EXCEPTION 'event_embeddings requires embedding/embedding_vector/vector column';
    END IF;

    SELECT column_name INTO v_embedding_id_col
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'event_embeddings' AND column_name IN ('id', 'embedding_id')
    ORDER BY array_position(ARRAY['id','embedding_id'], column_name)
    LIMIT 1;
    IF v_embedding_id_col IS NOT NULL THEN
        v_embedding_id_expr := format('ee.%I::bigint', v_embedding_id_col);
    END IF;

    SELECT column_name INTO v_embedding_document_id_col
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'event_embeddings' AND column_name IN ('document_id', 'search_document_id', 'event_search_document_id')
    ORDER BY array_position(ARRAY['document_id','search_document_id','event_search_document_id'], column_name)
    LIMIT 1;

    SELECT column_name INTO v_embedding_event_id_col
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'event_embeddings' AND column_name = 'event_id'
    LIMIT 1;

    SELECT column_name INTO v_embedding_kind_col
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'event_embeddings' AND column_name IN ('embedding_doc_kind', 'doc_kind', 'document_kind', 'kind')
    ORDER BY array_position(ARRAY['embedding_doc_kind','doc_kind','document_kind','kind'], column_name)
    LIMIT 1;

    SELECT column_name INTO v_document_id_col
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'event_search_documents' AND column_name IN ('id', 'document_id')
    ORDER BY array_position(ARRAY['id','document_id'], column_name)
    LIMIT 1;
    IF v_document_id_col IS NOT NULL THEN
        v_document_id_expr := format('d.%I::bigint', v_document_id_col);
    END IF;

    SELECT column_name INTO v_document_event_id_col
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'event_search_documents' AND column_name = 'event_id'
    LIMIT 1;

    SELECT column_name INTO v_document_kind_col
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'event_search_documents' AND column_name IN ('embedding_doc_kind', 'doc_kind', 'document_kind', 'kind')
    ORDER BY array_position(ARRAY['embedding_doc_kind','doc_kind','document_kind','kind'], column_name)
    LIMIT 1;

    IF v_embedding_document_id_col IS NOT NULL AND v_document_id_col IS NOT NULL THEN
        v_join_sql := format('d.%I = ee.%I', v_document_id_col, v_embedding_document_id_col);
    ELSIF v_embedding_event_id_col IS NOT NULL AND v_document_event_id_col IS NOT NULL THEN
        v_join_sql := format('d.%I = ee.%I', v_document_event_id_col, v_embedding_event_id_col);
    ELSE
        RAISE EXCEPTION 'event_embeddings and event_search_documents require document_id or event_id join columns';
    END IF;

    IF v_document_event_id_col IS NOT NULL AND v_embedding_event_id_col IS NOT NULL THEN
        v_event_id_expr := format('coalesce(d.%I::bigint, ee.%I::bigint)', v_document_event_id_col, v_embedding_event_id_col);
    ELSIF v_document_event_id_col IS NOT NULL THEN
        v_event_id_expr := format('d.%I::bigint', v_document_event_id_col);
    ELSIF v_embedding_event_id_col IS NOT NULL THEN
        v_event_id_expr := format('ee.%I::bigint', v_embedding_event_id_col);
    END IF;

    IF v_document_kind_col IS NOT NULL AND v_embedding_kind_col IS NOT NULL THEN
        v_kind_expr := format('coalesce(d.%I::text, ee.%I::text)', v_document_kind_col, v_embedding_kind_col);
    ELSIF v_document_kind_col IS NOT NULL THEN
        v_kind_expr := format('d.%I::text', v_document_kind_col);
    ELSIF v_embedding_kind_col IS NOT NULL THEN
        v_kind_expr := format('ee.%I::text', v_embedding_kind_col);
    END IF;

    -- Document-table evidence columns are optional; select NULL when absent.
    SELECT coalesce(max(CASE WHEN column_name = 'title' THEN 'd.title::text' END), v_title_expr) INTO v_title_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name IN ('event_date','date') THEN format('d.%I::text', column_name) END), v_event_date_expr) INTO v_event_date_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents' AND column_name IN ('event_date','date');
    SELECT coalesce(max(CASE WHEN column_name IN ('event_time','time') THEN format('d.%I::text', column_name) END), v_event_time_expr) INTO v_event_time_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents' AND column_name IN ('event_time','time');
    SELECT coalesce(max(CASE WHEN column_name = 'end_date' THEN 'd.end_date::text' END), v_end_date_expr) INTO v_end_date_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name = 'city' THEN 'd.city::text' END), v_city_expr) INTO v_city_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name = 'event_type' THEN 'd.event_type::text' END), v_event_type_expr) INTO v_event_type_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name = 'location_name' THEN 'd.location_name::text' END), v_location_name_expr) INTO v_location_name_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name IN ('location_address','address') THEN format('d.%I::text', column_name) END), v_location_address_expr) INTO v_location_address_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents' AND column_name IN ('location_address','address');
    SELECT coalesce(max(CASE WHEN column_name = 'ticket_link' THEN 'd.ticket_link::text' END), v_ticket_link_expr) INTO v_ticket_link_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name = 'source_url' THEN 'd.source_url::text' END), v_source_url_expr) INTO v_source_url_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name = 'source_type' THEN 'd.source_type::text' END), v_source_type_expr) INTO v_source_type_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name = 'telegraph_url' THEN 'd.telegraph_url::text' END), v_telegraph_url_expr) INTO v_telegraph_url_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name = 'tg_event_post_url' THEN 'd.tg_event_post_url::text' END), v_tg_event_post_url_expr) INTO v_tg_event_post_url_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name = 'source_vk_post_url' THEN 'd.source_vk_post_url::text' END), v_source_vk_post_url_expr) INTO v_source_vk_post_url_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents';
    SELECT coalesce(max(CASE WHEN column_name IN ('document_hash','hash','sha256') THEN format('d.%I::text', column_name) END), v_document_hash_expr) INTO v_document_hash_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents' AND column_name IN ('document_hash','hash','sha256');
    SELECT coalesce(max(CASE WHEN column_name IN ('document_text','content','text') THEN format('d.%I::text', column_name) END), v_document_text_expr) INTO v_document_text_expr
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_search_documents' AND column_name IN ('document_text','content','text');

    IF v_kind_expr <> 'NULL::text' THEN
        v_kind_filter := format('($2 IS NULL OR %s = $2)', v_kind_expr);
    END IF;
    IF v_city_expr <> 'NULL::text' THEN
        v_city_filter := format('($3 IS NULL OR lower(%s) = lower($3))', v_city_expr);
    END IF;
    IF v_event_type_expr <> 'NULL::text' THEN
        v_type_filter := format('($4 IS NULL OR lower(%s) = lower($4))', v_event_type_expr);
    END IF;

    v_distance_expr := format('ee.%I <=> $1', v_embedding_col);

    v_sql := format($fmt$
        WITH scored AS (
            SELECT
                %s AS event_id,
                %s AS document_id,
                %s AS embedding_id,
                %s AS embedding_doc_kind,
                %s AS candidate_doc_kind,
                (%s)::double precision AS distance,
                %s AS title,
                %s AS event_date,
                %s AS event_time,
                %s AS end_date,
                %s AS city,
                %s AS event_type,
                %s AS location_name,
                %s AS location_address,
                %s AS ticket_link,
                %s AS source_url,
                %s AS source_type,
                %s AS telegraph_url,
                %s AS tg_event_post_url,
                %s AS source_vk_post_url,
                %s AS document_hash,
                %s AS document_text
            FROM public.event_embeddings ee
            JOIN public.event_search_documents d ON %s
            WHERE %s
              AND %s
              AND %s
        )
        SELECT
            scored.event_id,
            scored.document_id,
            scored.embedding_id,
            scored.embedding_doc_kind,
            scored.candidate_doc_kind,
            (1.0 - scored.distance)::double precision AS similarity,
            scored.distance,
            scored.title,
            scored.event_date,
            scored.event_time,
            scored.end_date,
            scored.city,
            scored.event_type,
            scored.location_name,
            scored.location_address,
            scored.ticket_link,
            scored.source_url,
            scored.source_type,
            scored.telegraph_url,
            scored.tg_event_post_url,
            scored.source_vk_post_url,
            scored.document_hash,
            scored.document_text,
            jsonb_strip_nulls(jsonb_build_object(
                'event_id', scored.event_id,
                'document_id', scored.document_id,
                'embedding_id', scored.embedding_id,
                'doc_kind', scored.embedding_doc_kind,
                'document_hash', scored.document_hash,
                'source_url', scored.source_url,
                'telegraph_url', scored.telegraph_url,
                'tg_event_post_url', scored.tg_event_post_url,
                'source_vk_post_url', scored.source_vk_post_url
            )) AS evidence
        FROM scored
        WHERE ($6 IS NULL OR (1.0 - scored.distance) >= $6)
        ORDER BY scored.distance ASC
        LIMIT %s
    $fmt$,
        v_event_id_expr,
        v_document_id_expr,
        v_embedding_id_expr,
        v_kind_expr,
        v_kind_expr,
        v_distance_expr,
        v_title_expr,
        v_event_date_expr,
        v_event_time_expr,
        v_end_date_expr,
        v_city_expr,
        v_event_type_expr,
        v_location_name_expr,
        v_location_address_expr,
        v_ticket_link_expr,
        v_source_url_expr,
        v_source_type_expr,
        v_telegraph_url_expr,
        v_tg_event_post_url_expr,
        v_source_vk_post_url_expr,
        v_document_hash_expr,
        v_document_text_expr,
        v_join_sql,
        v_kind_filter,
        v_city_filter,
        v_type_filter,
        v_limit
    );

    RETURN QUERY EXECUTE v_sql
        USING p_embedding, p_embedding_doc_kind, p_city, p_event_type, v_limit, p_min_similarity;
END;
$$;

COMMENT ON FUNCTION public.event_identity_candidates_by_embedding_v1(vector, text, text, text, integer, double precision)
IS 'Service-role backend vector recall for Smart Update event identity candidates. Uses event_search_documents/event_embeddings only and returns candidate evidence columns when present.';

REVOKE ALL ON FUNCTION public.event_identity_candidates_by_embedding_v1(vector, text, text, text, integer, double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.event_identity_candidates_by_embedding_v1(vector, text, text, text, integer, double precision) FROM anon;
REVOKE ALL ON FUNCTION public.event_identity_candidates_by_embedding_v1(vector, text, text, text, integer, double precision) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.event_identity_candidates_by_embedding_v1(vector, text, text, text, integer, double precision) TO service_role;

COMMIT;
