\set ON_ERROR_STOP on
begin;
insert into auth.users(id,email) values('fa000000-0000-4000-8000-000000000001','voice-contract@example.invalid'),('fa000000-0000-4000-8000-000000000002','other-contract@example.invalid');
do $$
declare o uuid:='fa000000-0000-4000-8000-000000000001'; x uuid:='fa000000-0000-4000-8000-000000000002'; op_id uuid:=gen_random_uuid(); c uuid; r jsonb; rejected boolean;
begin
 if has_table_privilege('authenticated','public.event_search_assistant_operations','SELECT') or has_table_privilege('anon','public.event_search_assistant_audio_parts','SELECT') then raise exception 'private rows exposed'; end if;
 if has_function_privilege('authenticated','public.event_search_assistant_admit_v1(uuid,uuid,text,jsonb)','EXECUTE') then raise exception 'privileged RPC exposed'; end if;
 r:=public.event_search_assistant_admit_v1(o,op_id,'interpret','{"text":"не концерт"}');
 if r->>'state'<>'accepted' then raise exception 'intake not durable'; end if;
 perform public.event_search_assistant_admit_v1(o,op_id,'interpret','{"text":"не концерт"}');
 rejected:=false;
 begin perform public.event_search_assistant_admit_v1(x,op_id,'interpret','{"text":"не концерт"}'); exception when insufficient_privilege then rejected:=true; end;
 if not rejected then raise exception 'cross owner accepted'; end if;
 rejected:=false;
 begin perform public.event_search_assistant_admit_v1(o,op_id,'interpret','{"text":"концерт"}'); exception when unique_violation then rejected:=true; end;
 if not rejected then raise exception 'payload replacement accepted'; end if;
 r:=public.event_search_assistant_claim_v1(o,op_id);c:=(r->>'claim_id')::uuid;
 if not (r->>'claimed')::boolean then raise exception 'first claim failed'; end if;
 if (public.event_search_assistant_claim_v1(o,op_id)->>'claimed')::boolean then raise exception 'second claim succeeded'; end if;
 perform public.event_search_assistant_checkpoint_v1(o,op_id,c,'dispatched');
 rejected:=false;
 begin perform public.event_search_assistant_checkpoint_v1(o,op_id,c,'accepted'); exception when serialization_failure then rejected:=true; end;
 if not rejected then raise exception 'sent stage rearmed'; end if;
 perform public.event_search_assistant_checkpoint_v1(o,op_id,c,'completed','{"result":{"text":"не концерт"},"accounting":{"pending":true}}');
 if (public.event_search_assistant_claim_v1(o,op_id)->>'claimed')::boolean then raise exception 'completed stage replayed'; end if;
 if (select op.outcome->'result'->>'text' from public.event_search_assistant_operations op where op.id=op_id)<>'не концерт' then raise exception 'checkpoint lost'; end if;
end $$;
-- Audio: exact duplicate accepted, different bytes rejected, original retained.
do $$
declare o uuid:='fa000000-0000-4000-8000-000000000001'; op_id uuid:=gen_random_uuid(); rejected boolean:=false; claim uuid;
begin
 perform public.event_search_assistant_admit_v1(o,op_id,'asr','{"frames":4,"sampleRate":44100,"partCount":2}');
 perform public.event_search_assistant_audio_part_v1(o,op_id,1,2,2,44100,repeat('a',64),'AQI=');
 perform public.event_search_assistant_audio_part_v1(o,op_id,1,2,2,44100,repeat('a',64),'AQI=');
 begin perform public.event_search_assistant_audio_part_v1(o,op_id,1,2,2,44100,repeat('a',64),'AwQ='); exception when unique_violation then rejected:=true; end;
 if not rejected then raise exception 'audio replaced'; end if;
 claim:=(public.event_search_assistant_claim_v1(o,op_id)->>'claim_id')::uuid;
 perform public.event_search_assistant_checkpoint_v1(o,op_id,claim,'dispatched');
 update public.event_search_assistant_operations op set updated_at=now()-interval '10 minutes' where op.id=op_id;
 if public.event_search_assistant_claim_v1(o,op_id)->>'state'<>'outcome_unknown' then raise exception 'stale sent stage replayed'; end if;
 if (select count(*) from public.event_search_assistant_audio_parts p where p.operation_id=op_id)<>1 then raise exception 'audio lost'; end if;
end $$;
rollback;
