import csv, io, json, pathlib, subprocess, sys, time, uuid
root=pathlib.Path(__file__).resolve().parent
case="fable_cutover_test_"+uuid.uuid4().hex[:10]
image="pgvector/pgvector:0.8.2-pg17"
def cli(args, inp=None, timeout=90, ok=True):
    p = subprocess.run(args,input=inp,capture_output=True,text=True,timeout=timeout)
    if ok and p.returncode:
        raise Exception(str(args[:3])+p.stderr[-900:])
    return p
def sql(text, ok=True):
    return cli(["docker","exec","-i",case,"psql","-X","-v","ON_ERROR_STOP=1","-At","-U","postgres"],text,ok=ok)
def put(name, data):
    cli(["docker","exec","-i",case,"sh","-c","cat > /tmp/"+name],data)
def run(name,success=True):
    cmd=["docker","exec","-w","/tmp",case,"psql","-X","-v","ON_ERROR_STOP=1","-At","-U","postgres","-f","/tmp/"+name]
    r=cli(cmd,ok=False)
    if bool(r.returncode)!= (not success):
        raise Exception(name+" unexpected result: "+r.stderr[:1400]+r.stdout[-120:])
    if r.returncode: print("expected refusal:",r.stderr.splitlines()[0][:190])
def data():
    return sql("SELECT md5(jsonb_agg(to_jsonb(d) ORDER BY d.event_id)::text) FROM public.event_search_documents d; SELECT md5(jsonb_agg(to_jsonb(e) ORDER BY e.embedding_doc_kind,e.event_id)::text) FROM public.event_embeddings e").stdout
def csv_stage(records):
    out=io.StringIO(newline='')
    for v in records:csv.writer(out).writerow([json.dumps(v)])
    return out.getvalue()
old="event-related-doc-v2-content-only"
new="event-related-doc-v3-equal-content-facts"
try:
    print("starting isolated container", case, flush=True)
    cli(["docker","run","--rm","--name",case,"--network","none","--tmpfs","/var/lib/postgresql/data:rw,size=196m","-e","POSTGRES_HOST_AUTH_METHOD=trust","-d",image])
    for _ in range(100):
        if cli(["docker","exec",case,"pg_isready","-U","postgres"],ok=False).returncode==0: break
        time.sleep(.2)
    sql("""CREATE SCHEMA extensions; CREATE EXTENSION vector WITH SCHEMA extensions;
    CREATE TABLE public.event_search_documents(event_id bigint PRIMARY KEY, text_hash text,related_text_hash text,related_doc_version text,metadata jsonb);
    CREATE TABLE public.event_embeddings(event_id bigint references public.event_search_documents,embedding_model text,embedding_dim int,embedding_doc_kind text,embedding extensions.vector(768),text_hash text,metadata jsonb,embedded_at timestamptz default now(),primary key(event_id, embedding_model,embedding_dim,embedding_doc_kind));""")
    for f in ("apply","rollback","export-v2"):
        put(f+".sql",(root/(f+".sql")).read_text())
    rows=[]
    for i in [1,2]:
        vec=[0.0]*768; vec[i]=1
        rep=[0.0]*768; rep[i+5]=1
        h=("%064x"%i)
        sh=("%064x"%(i+400))
        nh=("%064x"%(i+800))
        fr=("%064x"%(i+1000))
        j=dict(id=i,sh=sh,bh=h,fh=fr,source_vec=vec,h=nh,v=new,vec=rep,dm={"related_text_hash":nh,"related_corpus_revision":"abcd"},rm={"doc_version":new,"embedding_corpus_revision":"abcd"},sm={"related_doc_version":new})
        rows.append(j)
        sql(f"""INSERT INTO public.event_search_documents VALUES ({i},'{sh}','{h}','{old}','{{"unrelated":1}}');
           INSERT INTO public.event_embeddings (event_id,embedding_model,embedding_dim,embedding_doc_kind,embedding,text_hash,metadata)
           VALUES ({i},'gemini-embedding-2',768,'related_v1','{json.dumps(vec)}','{h}','{{"doc_version":"{old}","custom":"keep"}}'),
           ({i},'gemini-embedding-2',768,'search_v3','{json.dumps(vec)}','{sh}','{{"custom":"keep"}}');""")
    before=data()
    run("export-v2.sql")
    assert "000001" in cli(["docker","exec",case,"cat","/tmp/live-v2.csv"]).stdout
    put("forward.csv",csv_stage(rows[:1]));run("apply.sql",False);assert before==data()
    damaged=dict(rows[0],bh="changed");put("forward.csv",csv_stage([damaged,*rows[1:]]));run("apply.sql",False);assert before==data()
    put("forward.csv",csv_stage(rows));run("apply.sql");mixed=data();assert mixed!=before
    run("apply.sql",False);assert mixed==data()
    sql("UPDATE public.event_embeddings SET metadata= metadata|| jsonb_build_object('changed',1) WHERE event_id=1 AND embedding_doc_kind='related_v1'")
    advanced=data();run("rollback.sql",False);assert data()==advanced
    sql("UPDATE public.event_embeddings SET metadata=metadata-'changed' WHERE event_id=1 AND embedding_doc_kind='related_v1'")
    assert data()==mixed
    run("rollback.sql");assert data()==before
    run("rollback.sql",False);assert data()==before
    print(json.dumps({"engine":sql("select version(),extversion from pg_extension where extname='vector'").stdout.strip(),"cases":["export-v2","missing membership","changed input","apply all","duplicate apply","rollback refuses changed row","exact rollback incl search metadata and timestamp","duplicate rollback"],"source_data":"fully synthetic; two vectors of dimension 768; no mounts or network"}))
finally:
    try: cli(["docker","rm","-f",case],ok=False,timeout=90)
    except subprocess.TimeoutExpired: print("Deferred cleanup of private synthetic container",case,flush=True)
