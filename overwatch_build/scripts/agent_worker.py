#!/usr/bin/env python3
"""OverWatch stage worker.

n8n is the orchestrator; this helper owns the low-level SQLite/state-machine work.
Secrets are never read from workflow JSON. LLM/API secrets come from environment variables.
"""
from __future__ import annotations
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import re
import sqlite3
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

ROOT = Path(os.getenv("OVERWATCH_ROOT", "/data"))
DB = ROOT / "overwatch.db"
MEDIA = ROOT / "media"
AUDIT = ROOT / "audit"
BACKUPS = ROOT / "backups"
CONFIG = Path(os.getenv("OVERWATCH_CONFIG", "/opt/overwatch/config"))

if not CONFIG.exists():
    CONFIG = Path(__file__).resolve().parents[1] / "config"
THEME_PATH = CONFIG / "theme.json"

for p in (ROOT, MEDIA, AUDIT, BACKUPS):
    p.mkdir(parents=True, exist_ok=True)


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(ts: dt.datetime | None = None) -> str:
    return (ts or now()).isoformat()


def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB, timeout=60)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("PRAGMA busy_timeout=10000")
    return c


def cfg(c: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = c.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def cfg_bool(c: sqlite3.Connection, key: str, default=False) -> bool:
    v = str(cfg(c, key, str(default))).strip().lower()
    return v in {"1", "true", "yes", "on"}


def j(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def audit(c: sqlite3.Connection, stage: str, entity_id: str | None, status: str,
          result: Any = None, error: str | None = None, duration_ms: int | None = None) -> None:
    c.execute(
        "INSERT INTO audit_log(workflow,stage,entity_id,status,payload,result,error,duration_ms,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        ("OverWatch", stage, entity_id, status, None,
         j(result) if result is not None else None, error, duration_ms, iso()),
    )


def safe_title(s: str, max_len=140) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len].rstrip()


def title_hash(title: str) -> str:
    normalized = re.sub(r"[^\w\s]", " ", (title or "").lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def http_get(url: str, headers=None, timeout=20, max_bytes=2_000_000):
    headers = headers or {"User-Agent": "Mozilla/5.0 OverWatch/1.1"}
    if requests:
        r = requests.get(url, headers=headers, timeout=timeout, stream=True)
        r.raise_for_status()
        total = 0
        chunks = []
        for chunk in r.iter_content(64 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise ValueError("response exceeded max_bytes")
            chunks.append(chunk)
        r._content = b"".join(chunks)
        return r
    req = urllib.request.Request(url, headers=headers)
    r = urllib.request.urlopen(req, timeout=timeout)
    body = r.read(max_bytes + 1)
    if len(body) > max_bytes:
        raise ValueError("response exceeded max_bytes")
    class R:
        content = body
        status_code = 200
        text = body.decode("utf-8", "replace")
        def raise_for_status(self): return None
        def json(self): return json.loads(self.text)
    return R()


def post_json(url: str, body: dict, headers=None, timeout=90):
    headers = headers or {"Content-Type": "application/json"}
    if requests:
        r = requests.post(url, json=body, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json() if r.content else {}
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8"))


def extract_json(text: str) -> Any:
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.I | re.S).strip()
    try:
        return json.loads(text)
    except Exception:
        # First balanced-looking JSON block, not arbitrary greedy text.
        for pattern in (r"\{.*\}", r"\[.*\]"):
            m = re.search(pattern, text, re.S)
            if m:
                return json.loads(m.group(0))
        raise


def llm_enabled_or_raise(c: sqlite3.Connection):
    if not cfg_bool(c, "llm_enabled", True):
        raise RuntimeError("LLM globally disabled")


def log_llm(c, provider, model, task, success=True, error=None):
    c.execute("INSERT INTO llm_usage(provider,model,task,request_units,success,error,created_at) VALUES(?,?,?,?,?,?,?)",
              (provider, model, task, 1, int(success), error, iso()))


def call_llm(c: sqlite3.Connection, prompt: str, task: str, temperature=0.2, max_tokens=5000):
    """Gemini -> Groq -> Ollama failover."""
    llm_enabled_or_raise(c)
    providers = [
        ("gemini", os.getenv("GEMINI_API_KEY"), os.getenv("GEMINI_MODEL", "gemini-2.5-flash")),
        ("groq", os.getenv("GROQ_API_KEY"), os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")),
        ("ollama", os.getenv("OLLAMA_URL", "http://127.0.0.1:11434"), os.getenv("OLLAMA_MODEL", "llama3.1:8b")),
    ]
    last = None
    for provider, secret, model in providers:
        if not secret:
            continue
        try:
            if provider == "gemini":
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={urllib.parse.quote(secret)}"
                body = {
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "temperature": temperature,
                        "maxOutputTokens": max_tokens,
                        "responseMimeType": "application/json" if task in {"score", "claims", "verdict", "caption", "outline"} else "text/plain",
                    },
                }
                data = post_json(url, body, {"Content-Type": "application/json"}, timeout=120)
                text = data["candidates"][0]["content"]["parts"][0]["text"]
            elif provider == "groq":
                url = "https://api.groq.com/openai/v1/chat/completions"
                body = {"model": model, "messages": [{"role": "user", "content": prompt}],
                        "temperature": temperature, "max_tokens": max_tokens}
                data = post_json(url, body, {"Authorization": f"Bearer {secret}", "Content-Type": "application/json"}, timeout=120)
                text = data["choices"][0]["message"]["content"]
            else:
                url = secret.rstrip("/") + "/api/chat"
                data = post_json(url, {"model": model, "messages": [{"role": "user", "content": prompt}],
                                       "stream": False, "options": {"temperature": temperature}}, timeout=180)
                text = data["message"]["content"]
            log_llm(c, provider, model, task, True)
            return text.strip(), provider, model
        except Exception as e:
            last = f"{provider}: {e}"
            try: log_llm(c, provider, model, task, False, str(e))
            except Exception: pass
    raise RuntimeError(last or "No LLM provider configured")


FEED_TOPICS = {"world": "world",
               "business": "business",
               "tech": "tech",
               "science": "science",
               "health": "health",
               "sports": "sports",
               "culture": "culture",
               "mixed": "world"}

def parse_feed(xml_bytes: bytes, feed_name: str, topic: str):
    root = ET.fromstring(xml_bytes)
    items = []

    for node in root.findall(".//item"):
        def text(tag):
            e = node.find(tag)
            return e.text.strip() if e is not None and e.text else ""

        title = safe_title(text("title"))
        link = text("link") or ""
        pub = text("pubDate")
        if not title or not link: continue
        items.append({"title": title, "source": feed_name, "url": link, "topic": FEED_TOPICS.get(topic, "world"),
                      "published_raw": pub})
    return items

def parse_date(value: str | None) -> dt.datetime | None:
    if not value: return None
    # Common RSS format plus ISO.
    for v in (value, value.replace("Z", "+00:00")):
        try:
            x = dt.datetime.fromisoformat(v)
            return x if x.tzinfo else x.replace(tzinfo=dt.timezone.utc)
        except Exception: pass
    try:
        import email.utils
        x = email.utils.parsedate_to_datetime(value)
        return x if x.tzinfo else x.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None

def feeds(c):
    raw = cfg(c, "feeds_json", "[]")
    return json.loads(raw)

def scrape():
    started = time.time(); c = db()
    try:
        if not cfg_bool(c, "ingest_enabled", True):
            print(j({"ok": True, "skipped": True, "reason": "ingest_disabled"})); return
        unique = 0;
        raw_count = 0;
        failures = []
        max_age_h = int(cfg(c, "headline_max_age_hours", 48))
        feed_results=[]
        feed_list=feeds(c)
        
        def fetch_feed(entry):
            feed_name,url,topic=entry
            try:
                r=http_get(url,timeout=20,max_bytes=2_000_000)
                return entry,parse_feed(r.content,feed_name,topic),None
            except Exception as e:
                return entry,[],str(e)
        with ThreadPoolExecutor(max_workers=min(16,len(feed_list) or 1)) as ex:
            futures=[ex.submit(fetch_feed,x) for x in feed_list]
            for f in as_completed(futures): feed_results.append(f.result())

        for (feed_name,url,topic),items,err in feed_results:
            if err:
                failures.append({"feed":feed_name,"error":err}); continue
            for item in items:
                raw_count += 1
                pdt=parse_date(item["published_raw"])
                if pdt and (now()-pdt.astimezone(dt.timezone.utc)).total_seconds()>max_age_h*3600: continue
                title=item["title"]
                hid=f"H-{now().strftime('%Y%m%d')}-{hashlib.md5((title+item['url']).encode()).hexdigest()[:10]}"
                th=title_hash(title)
                c.execute("INSERT INTO headlines(id,title,source,url,topic,published_at,fetched_at,raw_xml,title_hash,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(title_hash) DO NOTHING",
                          (hid,title,item["source"],item["url"],item["topic"],iso(pdt) if pdt else None,iso(),None,th,"pending",iso()))
                unique += c.execute("SELECT changes()").fetchone()[0]
        c.execute("INSERT INTO meta_kpis(metric_date,headlines_in,headlines_unique,notes,created_at) VALUES(?,?,?,?,?) "
                  "ON CONFLICT(metric_date) DO UPDATE SET headlines_in=headlines_in+excluded.headlines_in,headlines_unique=headlines_unique+excluded.headlines_unique,notes=excluded.notes",
                  (now().date().isoformat(), raw_count, unique, j({"scrape_failures": failures}), iso()))
        if unique < int(cfg(c,"ingest_min_headlines","100")):
            audit(c,"scrape","cycle","warning",{"raw":raw_count,"unique":unique,"failures":failures})
        else:
            audit(c,"scrape","cycle","done",{"raw":raw_count,"unique":unique,"failures":failures},duration_ms=int((time.time()-started)*1000))
        mark_run(c,'scrape',True); c.commit(); print(j({"ok":True,"raw":raw_count,"unique":unique,"failures":failures}));
    finally: c.close()

def tokenize_vector(text: str):
    toks=re.findall(r"[a-z0-9]{3,}", (text or '').lower())
    d={}
    for t in toks:
        d[t]=d.get(t,0)+1
    return d

def cosine(a,b):
    if isinstance(a,dict) and isinstance(b,dict):
        keys=set(a)|set(b); aa=sum(a.get(k,0)*a.get(k,0) for k in keys); bb=sum(b.get(k,0)*b.get(k,0) for k in keys)
        if not aa or not bb: return 0.0
        return sum(a.get(k,0)*b.get(k,0) for k in keys)/(aa**0.5*bb**0.5)
    return 0.0

def semantic_vector(text: str):
    # Optional local sentence-transformers; deterministic lexical fallback keeps the base image small.
    try:
        from sentence_transformers import SentenceTransformer
        if not hasattr(semantic_vector,'model'):
            semantic_vector.model=SentenceTransformer(os.getenv('EMBEDDING_MODEL','all-MiniLM-L6-v2'))
        return semantic_vector.model.encode([text],normalize_embeddings=True)[0].tolist(), 'sentence-transformers'
    except Exception:
        return tokenize_vector(text), 'lexical-fallback'

def semantic_duplicate(c, headline_id, title):
    vec, model=semantic_vector(title)
    raw=vec if isinstance(vec,dict) else vec
    h=hashlib.sha256(title_hash(title).encode()).hexdigest()
    c.execute("INSERT OR IGNORE INTO embeddings(id,entity_type,entity_id,text_hash,model,embedding_json,created_at) VALUES(?,?,?,?,?,?,?)",
              (f'E-{headline_id}',
              'headline',
              headline_id,
              h,
              model,
              j(raw),
              iso()))

    threshold=float(cfg(c,'semantic_dedup_threshold','0.85'))
    
    rows=c.execute("SELECT entity_id,embedding_json FROM embeddings WHERE entity_type='headline' AND entity_id<>? ORDER BY created_at DESC LIMIT 500",(headline_id,)).fetchall()
    
    for r in rows:
        try:
            other=json.loads(r['embedding_json']); sim=cosine(raw,other)
            if sim>=threshold:
                # Verify candidate is already published or pooled; don't suppress against arbitrary rejected headlines.
                active=c.execute("SELECT 1 FROM pool WHERE headline_id=? AND status='pooled' UNION SELECT 1 FROM posts p JOIN groups g ON g.id=p.group_id WHERE g.headline_id=? AND p.status IN ('queued','published') LIMIT 1",(r['entity_id'],r['entity_id'])).fetchone()
                if active: return True, r['entity_id'], sim, model
        except Exception: pass
    return False, None, 0.0, model


def mark_run(c, workflow, success=True, execution_id=None):
    key=f"stage:{workflow}"
    if success:
        c.execute("INSERT INTO workflow_runs(workflow_name,last_success_at,last_execution_id,updated_at) VALUES(?,?,?,?) ON CONFLICT(workflow_name) DO UPDATE SET last_success_at=excluded.last_success_at,last_execution_id=excluded.last_execution_id,updated_at=excluded.updated_at",(key,iso(),execution_id,iso()))
    else:
        c.execute("INSERT INTO workflow_runs(workflow_name,last_failure_at,last_execution_id,updated_at) VALUES(?,?,?,?) ON CONFLICT(workflow_name) DO UPDATE SET last_failure_at=excluded.last_failure_at,last_execution_id=excluded.last_execution_id,updated_at=excluded.updated_at",(key,iso(),execution_id,iso()))

def score():
    started=time.time(); c=db()
    try:
        rows=c.execute("SELECT * FROM headlines WHERE status='pending' ORDER BY COALESCE(published_at,created_at) LIMIT 200").fetchall()
        if not rows:
            print(j({"ok":True,"processed":0})); return
        rubric='''You are the SCORING AGENT of an automated news channel. Score each headline using EXACTLY this rubric. Output JSON only.\n'
curiosity 20%; emotion 15%; relevance 15%; freshness 15%; visual 15%; authority 10%; shareability 10%. Each score 0-10. Outrage bait scores DOWN. No speculation as fact. Never invent numbers.'''

        for b in range(0,len(rows),20):
            batch=rows[b:b+20]
            payload=[{"headline_id":r["id"],
                      "title":r["title"],
                      "source":r["source"],
                      "topic":r["topic"],
                      "published_at":r["published_at"]} for r in batch]

            prompt=rubric+"\nReturn {\"scores\":[{\"headline_id\":\"...\",\"curiosity\":0,\"emotion\":0,\"relevance\":0,\"freshness\":0,\"visual\":0,\"authority\":0,\"shareability\":0,\"rationale\":\"...\"}]}\nINPUT:\n"+j(payload)
            text,provider,model=call_llm(c,prompt,"score",0,2500)
            out=extract_json(text).get("scores",[]); by={x.get("headline_id"):x for x in out}
            
            for r in batch:
                s=by.get(r["id"])
                if not s: c.execute("UPDATE headlines SET status='failed',last_error=? WHERE id=?",("missing LLM score",r["id"])); continue
                try:
                    vals={k:max(0,min(10,float(s[k]))) for k in ("curiosity","emotion","relevance","freshness","visual","authority","shareability")}
                    total=10*(.20*vals["curiosity"]+.15*vals["emotion"]+.15*vals["relevance"]+.15*vals["freshness"]+.15*vals["visual"]+.10*vals["authority"]+.10*vals["shareability"])

                    if r["source"] in {"BBC World","DW All","Al Jazeera","France24"}:
                        total=min(100,total+2)

                    passed=total>=float(cfg(c,"score_threshold","72"))
                    sid=f"S-{r['id']}"
                    c.execute("INSERT INTO scored(id,headline_id,curiosity,emotion,relevance,freshness,visual,authority,shareability,total_score,passed,rationale,rubric_version,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                              "ON CONFLICT(headline_id) DO UPDATE SET total_score=excluded.total_score,passed=excluded.passed,rationale=excluded.rationale,rubric_version=excluded.rubric_version,status='done'",
                              (sid,r["id"],vals["curiosity"],vals["emotion"],vals["relevance"],vals["freshness"],vals["visual"],vals["authority"],vals["shareability"],total,int(passed),s.get("rationale",""),cfg(c,"rubric_version","v1.1"),"done",iso()))
                    c.execute("UPDATE headlines SET status=? WHERE id=?",("scored" if passed else "rejected",r["id"]))

                    if passed:
                        dup,dup_id,sim,embed_model=semantic_duplicate(c,r['id'],r['title'])
                        if dup:
                            passed=False
                            c.execute("UPDATE scored SET passed=0,dedup_reason=? WHERE headline_id=?",(f"semantic_duplicate:{dup_id}:{sim:.3f}",r['id']))
                            c.execute("UPDATE headlines SET status='duplicate' WHERE id=?",(r['id'],))
                        else:
                            ts=parse_date(r["published_at"]) or now(); expiry=ts+dt.timedelta(hours=int(cfg(c,"freshness_expiry_hours","72")))
                            c.execute("INSERT OR IGNORE INTO pool(id,headline_id,total_score,priority,created_at,freshness_ts,expiry_ts,status) VALUES(?,?,?,?,?,?,?,'pooled')",
                                      (f"P-{r['id']}",r["id"],total,total,iso(),iso(ts),expiry.isoformat()))
                            c.execute("UPDATE embeddings SET created_at=created_at WHERE entity_id=?",(r['id'],))
                    audit(c,"score",r["id"],"done",{"score":total,"passed":passed,"provider":provider})
                except Exception as e:
                    c.execute("UPDATE headlines SET status='failed',last_error=? WHERE id=?",(str(e),r["id"])); audit(c,"score",r["id"],"failed",error=str(e))
        # Rank among all selected candidates; enforce topic cap and max 40 deterministically.
        candidates=c.execute("SELECT s.id,s.headline_id,s.total_score,s.freshness,s.visual,h.topic,h.published_at FROM scored s JOIN headlines h ON h.id=s.headline_id WHERE s.passed=1 ORDER BY s.total_score DESC,s.freshness DESC,s.visual DESC").fetchall()
        cap=int(cfg(c,"topic_cap","8")); selected=[]; topic_counts={}

        for r in candidates:
            if topic_counts.get(r["topic"],0)>=cap: continue
            selected.append(r); topic_counts[r["topic"]]=topic_counts.get(r["topic"],0)+1
            if len(selected)>=int(cfg(c,"candidate_ceiling","40")): break
        # Floor relaxation only if fewer than floor exist.
        floor=int(cfg(c,"candidate_floor","30"))

        if len(selected)<floor:
            relaxed=float(cfg(c,"score_relaxed_threshold","65"))
            relaxed_rows=c.execute("SELECT s.id,s.headline_id,s.total_score,s.freshness,s.visual,h.topic,h.published_at FROM scored s JOIN headlines h ON h.id=s.headline_id WHERE s.total_score>=? ORDER BY s.total_score DESC,s.freshness DESC,s.visual DESC",(relaxed,)).fetchall()
            chosen={x["headline_id"] for x in selected}; relaxed_topic_counts=dict(topic_counts)
            for r in relaxed_rows:
                if r["headline_id"] in chosen: continue
                if relaxed_topic_counts.get(r["topic"],0)>=cap: continue
                selected.append(r); chosen.add(r["headline_id"]); relaxed_topic_counts[r["topic"]]=relaxed_topic_counts.get(r["topic"],0)+1
                if len(selected)>=floor: break
        
        if len(selected)<floor:
            # Last-resort starvation guard: fill to the floor from highest total scores regardless of threshold.
            chosen={x["headline_id"] for x in selected}
            for r in candidates:
                if r["headline_id"] in chosen: continue
                selected.append(r); chosen.add(r["headline_id"])
                if len(selected)>=floor: break
        selected_ids={r["headline_id"] for r in selected}
        
        for rank,r in enumerate(selected,1):
            c.execute("UPDATE scored SET rank=? WHERE headline_id=?",(rank,r["headline_id"]))
            audit(c,"score_select",r["headline_id"],"selected",{"rank":rank})
        c.execute("UPDATE scored SET passed=0,dedup_reason=COALESCE(dedup_reason,'topic_cap_or_ceiling') WHERE passed=1 AND headline_id NOT IN (%s)" % (",".join("?"*len(selected_ids)) if selected_ids else "'__none__'"), tuple(selected_ids) if selected_ids else ())
        mark_run(c,'score',True); c.commit(); print(j({"ok":True,"processed":len(rows),"selected":len(selected)}))
    finally: c.close()


def fetch_source(url:str):
    r=http_get(url,headers={"User-Agent":"Mozilla/5.0 OverWatch/1.1"},timeout=20,max_bytes=2_500_000)
    text=re.sub(r"<script[\s\S]*?</script>|<style[\s\S]*?</style>|<[^>]+>"," ",r.text if hasattr(r,'text') else r.content.decode('utf-8','replace'))
    return re.sub(r"\s+"," ",text).strip()[:120000]

def writer():
    started=time.time(); c=db()
    try:
        rows=c.execute("SELECT s.*,h.title,h.url,h.source,h.topic,h.published_at FROM scored s JOIN headlines h ON h.id=s.headline_id WHERE s.passed=1 AND NOT EXISTS(SELECT 1 FROM articles a WHERE a.headline_id=s.headline_id) ORDER BY COALESCE(s.rank,999) LIMIT 15").fetchall()
        # Rewrites have priority.
        rewrites=c.execute("SELECT a.*,h.title,h.url,h.source,h.topic FROM articles a JOIN headlines h ON h.id=a.headline_id WHERE a.status='pending_review' AND a.failed_claims_json IS NOT NULL AND a.draft_rev<=? ORDER BY a.updated_at LIMIT 15",(int(cfg(c,"max_rewrites","2")),)).fetchall()
        total=0
        for r in list(rewrites)+list(rows):
            aid=r["id"] if "id" in r.keys() and str(r["id"]).startswith("A-") else f"A-{r['headline_id']}"
            try:
                source_text=fetch_source(r["url"])
                failure_context = ('\nFAILED FACT-CHECK CLAIMS TO CORRECT:\n'+j(json.loads(r['failed_claims_json'])) ) if 'failed_claims_json' in r.keys() and r['failed_claims_json'] else ''
                outline_prompt=f'''You are the RESEARCH lead of a news channel. Given the headline and source article below, produce JSON: {{"angle":"...","standfirst":"...","sections":["8 section titles in narrative order"],"key_facts":["5-10 verifiable facts"],"quotes_to_verify":["short source quotes"],"open_questions":["2-3 questions"]}}. Constraints: 8 sections, each later 400-500 words; facts must appear in source or be marked UNVERIFIED; no invented quotes.\nHEADLINE: {r['title']}\nSOURCE: {source_text}{failure_context}'''
                out_txt,prov,model=call_llm(c,outline_prompt,"outline",0.3,4000); outline=extract_json(out_txt)
                if len(outline.get('sections',[])) != 8: raise ValueError('writer outline must contain exactly 8 sections')
                sections=[]; prior=""
                failed_claims=json.loads(r['failed_claims_json']) if 'failed_claims_json' in r.keys() and r['failed_claims_json'] else []
                for n,title in enumerate(outline.get("sections",[])[:8],1):
                    sec_prompt=f'''You are a senior journalist writing for a global audience. Write SECTION {n} of the article: "{title}". Outline: {j(outline)} Previously written sections: {prior[-12000:]}. Style: 400-500 words; grade-8 reading level; short sentences; numbers over adjectives; named people; active voice; one idea per paragraph; bolded takeaway every 3-5 paragraphs; no invented facts, quotes, or stats; uncertainty explicit; end with bridge sentence. Output only section text.'''
                    st,_,_=call_llm(c,sec_prompt,"section",0.7,1800); sections.append(st); prior += "\n\n"+st
                article=f"# {r['title']}\n\n**{outline.get('standfirst','')}**\n\n_OverWatch — AI-assisted & fact-checked after verification._\n\n"+"\n\n".join(f"## {outline['sections'][i]}\n\n{s}" for i,s in enumerate(sections))
                facts=outline.get("key_facts",[])
                article += "\n\n## Sources\n"+"\n".join(f"- {r['url']}")
                words=len(re.findall(r"\b\w+[’'-]?\w*\b",article))
                # One controlled expansion pass if below hard gate.
                if words<int(cfg(c,"article_min_words","3000")) and sections:
                    sec_idx=min(range(len(sections)),key=lambda i:len(sections[i]))
                    exp_prompt=f"Expand this section to 450-550 words without adding facts not supported by the source. Return only revised section. SOURCE SUMMARY: {outline.get('key_facts',[])}\nSECTION:\n{sections[sec_idx]}"
                    sections[sec_idx],_,_=call_llm(c,exp_prompt,"section",0.7,2200)
                    article=f"# {r['title']}\n\n**{outline.get('standfirst','')}**\n\n_OverWatch — AI-assisted & fact-checked after verification._\n\n"+"\n\n".join(f"## {outline['sections'][i]}\n\n{s}" for i,s in enumerate(sections))+"\n\n## Sources\n- "+r['url']
                    words=len(re.findall(r"\b\w+[’'-]?\w*\b",article))
                if words<int(cfg(c,"article_min_words","3000")):
                    raise ValueError(f"word_count {words} below hard gate")
                rev=int(r.get("draft_rev",1) or 1)
                c.execute("INSERT INTO articles(id,headline_id,article_md,word_count,draft_rev,status,outline_json,failed_claims_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) "
                          "ON CONFLICT(id) DO UPDATE SET article_md=excluded.article_md,word_count=excluded.word_count,draft_rev=excluded.draft_rev,status='pending_review',outline_json=excluded.outline_json,failed_claims_json=NULL,updated_at=excluded.updated_at",
                          (aid,r["headline_id"],article,words,rev,"pending_review",j(outline),None,iso(),iso()))
                audit(c,"write",aid,"done",{"words":words,"revision":rev,"provider":prov})
                total+=1
            except Exception as e:
                audit(c,"write",aid,"failed",error=str(e));
                if str(aid).startswith("A-"):
                    c.execute("UPDATE articles SET status='failed',updated_at=?,last_error=? WHERE id=?",(iso(),str(e),aid)) if False else None
        mark_run(c,'write',True); c.commit(); print(j({"ok":True,"processed":len(rows)+len(rewrites),"completed":total,"duration_ms":int((time.time()-started)*1000)}))
    finally: c.close()

def factcheck():
    c=db();
    try:
        rows=c.execute("SELECT a.*,h.title,h.url,h.source,h.topic FROM articles a JOIN headlines h ON h.id=a.headline_id WHERE a.status='pending_review' ORDER BY a.updated_at LIMIT 15").fetchall(); done=0
        high_topics={"health","business","world"}
        for r in rows:
            try:
                claim_prompt=f'''Extract every verifiable factual claim from this article (max 12): numbers, dates, names, attributions, causal statements. Output JSON array [{{"claim":"...","type":"statistic|event|attribution|causal|identity","importance":"high|medium|low"}}]. Skip opinions and generic statements. ARTICLE:\n{r['article_md']}'''
                ct,prov,_=call_llm(c,claim_prompt,"claims",0,3000); claims=extract_json(ct)
                evidence=[]; checked=[]
                for cl in claims[:12]:
                    q=urllib.parse.quote(cl.get("claim",""))
                    rss=f"https://news.google.com/rss/search?q={q}&hl=en-US"
                    try:
                        rr=http_get(rss,timeout=20,max_bytes=800_000); root=ET.fromstring(rr.content)
                        results=[]
                        for item in root.findall('.//item')[:5]:
                            title=(item.findtext('title') or '').strip(); link=(item.findtext('link') or '').strip(); pub=(item.findtext('pubDate') or '').strip()
                            results.append({"title":title,"link":link,"published":pub})
                        evidence.append({"claim":cl.get("claim"),"results":results}); checked += [x["link"] for x in results if x.get("link")]
                    except Exception:
                        evidence.append({"claim":cl.get("claim"),"results":[]})
                verdict_prompt=f'''You are a strict fact-checker. For each claim, given evidence search results, output JSON array with claim, verdict supported|unsupported|contradicted|unverifiable, confidence 0-1, evidence 1-line. Two independent high-trust sources = corroborated; anonymous blogs and aggregators are not evidence; conflicting evidence => contradicted. CLAIMS: {j(claims)} EVIDENCE: {j(evidence)}'''
                vt,_,_=call_llm(c,verdict_prompt,"verdict",0,4500); verdicts=extract_json(vt)
                total=max(1,len(verdicts)); supported=sum(v.get("verdict")=="supported" for v in verdicts); mean=sum(float(v.get("confidence",0)) for v in verdicts)/total; score=.7*(supported/total)+.3*mean
                high_bad=any(v.get("verdict") in {"unsupported","contradicted"} and claims[i].get("importance")=="high" for i,v in enumerate(verdicts) if i<len(claims))
                overall="PASS" if score>=float(cfg(c,"factcheck_pass_score","0.90")) and not high_bad else "REWRITE" if (score>=float(cfg(c,"factcheck_rewrite_score","0.75")) or high_bad) else "QUARANTINE"
                failed=[v for v in verdicts if v.get("verdict")!="supported"]
                fc=f"FC-{r['id']}-{r['draft_rev']}"
                c.execute("INSERT INTO fact_checks(id,article_id,claims_json,sources_checked_json,verdict,confidence,score,notes,rewrite_notes,checked_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(article_id) DO UPDATE SET claims_json=excluded.claims_json,sources_checked_json=excluded.sources_checked_json,verdict=excluded.verdict,confidence=excluded.confidence,score=excluded.score,rewrite_notes=excluded.rewrite_notes,checked_at=excluded.checked_at",
                          (fc,r['id'],j(verdicts),j(checked),overall,mean,score,"automated source comparison",j(failed),iso()))

                if overall=="PASS": st="verified"; inc=0
                elif overall=="REWRITE": st="pending_review"; inc=1
                else: st="quarantined"; inc=0

                c.execute("UPDATE articles SET status=?,failed_claims_json=?,draft_rev=draft_rev+?,updated_at=? WHERE id=?",(st,j(failed),inc,iso(),r['id']))
                audit(c,"factcheck",r['id'],overall,{"score":score,"claims":len(verdicts),"provider":prov})
                done+=1
            
            except Exception as e:
                audit(c,"factcheck",r['id'],'failed',error=str(e));
                c.execute("UPDATE articles SET status='failed',updated_at=? WHERE id=?",(iso(),r['id']))
        
        mark_run(c,'factcheck',True); c.commit(); print(j({"ok":True,"processed":len(rows),"completed":done}))
    finally: c.close()

def group():
    c=db()
    try:
        rows=c.execute("SELECT a.*,h.title,h.url,h.source,h.topic,s.total_score,s.rubric_version,fc.verdict,fc.score fc_score,fc.claims_json,fc.sources_checked_json FROM articles a JOIN headlines h ON h.id=a.headline_id JOIN scored s ON s.headline_id=h.id JOIN fact_checks fc ON fc.article_id=a.id WHERE a.status='verified' AND NOT EXISTS(SELECT 1 FROM groups g WHERE g.article_id=a.id) LIMIT 20").fetchall()
        seq=1
        for r in rows:
            audit_id=f"OW-{now().strftime('%Y%m%d')}-{seq:02d}"; seq+=1; gid=f"G-{r['id']}"; out=AUDIT/audit_id; out.mkdir(parents=True,exist_ok=True)
            bundle={"audit_id":audit_id,"headline":{"title":r['title'],"url":r['url'],"source":r['source'],"topic":r['topic']},"score":{"total":r['total_score'],"rubric_version":r['rubric_version']},"article":{"id":r['id'],"word_count":r['word_count'],"draft_rev":r['draft_rev']},"fact_check":{"verdict":r['verdict'],"score":r['fc_score'],"claims":len(json.loads(r['claims_json'] or '[]')),"sources":json.loads(r['sources_checked_json'] or '[]')},"timeline":[{"stage":"group","at":iso()}]}
            path=out/'bundle.json'; path.write_text(j(bundle),encoding='utf-8')
            html=f"<html><body><h1>{audit_id}</h1><pre>{json.dumps(bundle,indent=2,ensure_ascii=False)}</pre></body></html>"; (out/'bundle.html').write_text(html,encoding='utf-8')
            c.execute("INSERT INTO groups(id,audit_id,headline_id,article_id,audit_bundle_path,audit_bundle_json,topic,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)",(gid,audit_id,r['headline_id'],r['id'],str(path),j(bundle),r['topic'],'ready_for_media',iso()))
            c.execute("UPDATE articles SET status='grouped',updated_at=? WHERE id=?",(iso(),r['id'])); audit(c,"group",gid,"done",{"audit_id":audit_id})
        mark_run(c,'group',True); c.commit(); print(j({"ok":True,"processed":len(rows)}))
    finally: c.close()

def render_card(im, draw, text, title, card_no, theme):
    from PIL import ImageFont
    font_base='/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'; font_bold='/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf'
    fb=ImageFont.truetype(font_base,46); fh=ImageFont.truetype(font_bold,72); fsmall=ImageFont.truetype(font_bold,30)
    nav='#0F2A43' if theme=='NewsBlue' else '#FAF6EF'; ink='#1B1B1B'; accent='#F2A104' if theme=='NewsBlue' else '#C02037'
    draw.rectangle((0,0,1080,96),fill=nav)
    draw.text((55,24),'OVERWATCH · DAILY BRIEF',font=fsmall,fill='white' if theme=='NewsBlue' else ink)
    y=140

    if card_no==1:
        draw.text((70,y),title[:110],font=fh,fill=white if False else nav); y=340
    lines=[]

    for paragraph in text.split('\n'):
        lines.extend(textwrap.wrap(paragraph,width=40) or [''])
    lines=lines[:18 if card_no!=1 else 15]
    
    for line in lines:
        draw.text((70,y),line,font=fb,fill=ink); y += 61
    draw.text((930,1280),f'{card_no}/10',font=fsmall,fill=accent)

def maybe_pollinations(topic,title):
    if not requests: return None
    prompt=urllib.parse.quote(f"Editorial magazine background art, {topic} themed, {title}, muted navy blue and amber colors, no text, no letters, no words, soft depth of field, 4:5")
    url=f"https://image.pollinations.ai/prompt/{prompt}?width=1080&height=1350&nologo=true&model=flux"
    for _ in range(3):
        try:
            r=requests.get(url,timeout=60); r.raise_for_status(); return r.content
        except Exception: time.sleep(10)
    return None

def media():
    from PIL import Image,ImageDraw
    c=db()
    try:
        rows=c.execute("SELECT g.*,a.article_md,h.title FROM groups g JOIN articles a ON a.id=g.article_id JOIN headlines h ON h.id=g.headline_id WHERE g.status='ready_for_media' LIMIT 10").fetchall()
        for r in rows:
            out=MEDIA/r['audit_id'];
            out.mkdir(parents=True,exist_ok=True);
            article=r['article_md']

            # 8 dense article segments across cards 2–9; card 1 is the cover; card 10 is CTA/sources.
            words=article.split();
            usable=max(1,math.ceil(len(words)/8));
            chunks=[' '.join(words[i:i+usable]) for i in range(0,len(words),usable)]

            while len(chunks)<8:
                chunks.append('')

            chunks=chunks[:8]
            card_texts=[r['title']]+chunks+[f"Bottom line: Follow OverWatch for the next daily brief.\n\nSources: {r['url']}\n\nAI-assisted & fact-checked."]
            card_texts=card_texts[:10]
            c.execute("DELETE FROM cards WHERE group_id=?",(r['id'],))

            for no,chunk in enumerate(card_texts,1):
                base=Image.new('RGB',(1080,1350),'#F4F7FB')
                bg=maybe_pollinations(r['topic'],r['title']) if no in (1,10) else None
                if bg:
                    try:
                        bgim=Image.open(io.BytesIO(bg)).convert('RGB').resize((1080,1350)); base=Image.blend(base,bgim,0.30)
                    except Exception: pass
                d=ImageDraw.Draw(base); render_card(base,d,chunk,r['title'],no,cfg(c,'theme','NewsBlue'))
                path=out/f'card-{no:02d}.png'; base.save(path,'PNG',optimize=True)
                c.execute("INSERT INTO cards(id,group_id,card_no,img_path,text_snippet,theme,width,height,bytes,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                          (f"C-{r['id']}-{no:02d}",r['id'],no,str(path),chunk,cfg(c,'theme','NewsBlue'),1080,1350,path.stat().st_size,'rendered',iso()))
            c.execute("UPDATE groups SET status='ready_for_caption' WHERE id=?",(r['id'],)); audit(c,'media',r['id'],'done',{'cards':10})
        mark_run(c,'media',True); c.commit(); print(j({'ok':True,'processed':len(rows)}))
    finally: c.close()

def r2_upload(path: Path, key: str) -> str:
    import subprocess, shutil
    # Prefer boto3 when available; otherwise use a presigned PUT URL supplied by the caller.
    try:
        import boto3
        endpoint=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        s3=boto3.client('s3',endpoint_url=endpoint,aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],region_name='auto')
        s3.upload_file(str(path),os.environ['R2_BUCKET'],key,ExtraArgs={'ContentType':'image/png'})
        base=os.environ['R2_PUBLIC_BASE_URL'].rstrip('/')
        return base+'/'+urllib.parse.quote(key)
    except Exception:
        pres=os.getenv('R2_PRESIGNED_PUT_BASE','').rstrip('/')
        if not pres: raise RuntimeError('R2 upload not configured; set boto3 credentials or R2_PRESIGNED_PUT_BASE')
        url=pres+'/'+urllib.parse.quote(key)
        if requests: requests.put(url,data=path.read_bytes(),headers={'Content-Type':'image/png'},timeout=60).raise_for_status()
        return os.getenv('R2_PUBLIC_BASE_URL','').rstrip('/')+'/'+urllib.parse.quote(key)

def upload_media():
    c=db()
    try:
        rows=c.execute("SELECT g.audit_id,c.* FROM cards c JOIN groups g ON g.id=c.group_id WHERE c.status='rendered' AND (c.img_url IS NULL OR c.img_url='') LIMIT 100").fetchall(); done=0
        for r in rows:
            try:
                key=f"media/{r['audit_id']}/card-{r['card_no']:02d}.png"; url=r2_upload(Path(r['img_path']),key)
                c.execute("UPDATE cards SET img_url=?,status='ready' WHERE id=?",(url,r['id'])); done+=1
            except Exception as e:
                c.execute("UPDATE cards SET status='upload_failed' WHERE id=?",(r['id'],)); audit(c,'r2_upload',r['id'],'failed',error=str(e))
        c.commit(); print(j({'ok':True,'processed':len(rows),'uploaded':done}))
    finally: c.close()

def caption():
    c=db()
    try:
        rows=c.execute("SELECT g.*,a.article_md,h.title,h.source FROM groups g JOIN articles a ON a.id=g.article_id JOIN headlines h ON h.id=g.headline_id WHERE g.status='ready_for_caption' AND EXISTS(SELECT 1 FROM cards WHERE group_id=g.id AND status IN ('ready','rendered')) LIMIT 10").fetchall()
        for r in rows:
            p=f'''Write an Instagram caption. Output JSON {{"hook":"1-2 lines, surprising fact or question","summary":"3-5 lines","value_line":"swipe line","cta":"question|save|follow","hashtags":"3-8 tags","sources_line":"Sources: ... — verified.","full_caption":"..."}}. full_caption <= 2000 chars; exactly one CTA; no engagement-bait; no emoji spam. HEADLINE: {r['title']} SOURCE: {r['source']} ARTICLE: {r['article_md'][:5000]}'''
            text,prov,_=call_llm(c,p,'caption',0.7,1800); x=extract_json(text); full=str(x.get('full_caption','')).strip()

            if len(full)>2000: full=full[:1997]+'...'
            hashtags=x.get('hashtags','');

            c.execute("INSERT OR REPLACE INTO captions(id,group_id,hook,summary,value_line,cta,hashtags,char_count,variant,full_caption,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                      (f"CAP-{r['id']}-A",r['id'],x.get('hook'),x.get('summary'),x.get('value_line'),x.get('cta'),hashtags,len(full),'A',full,'ready',iso()))

            # Variant B: use a different CTA but regenerate only when requested later.
            alt_cta='Save this for later' if x.get('cta')!='save' else 'Follow for daily briefs'
            bfull=re.sub(r"What(?:'s| is) your take\?","Save this for later",full,flags=re.I)
            
            c.execute("INSERT OR REPLACE INTO captions(id,group_id,hook,summary,value_line,cta,hashtags,char_count,variant,full_caption,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                      (f"CAP-{r['id']}-B",r['id'],x.get('hook'),x.get('summary'),x.get('value_line'),alt_cta,hashtags,len(bfull),'B',bfull[:2000],'ready',iso()))
            
            c.execute("UPDATE groups SET status='ready_for_schedule' WHERE id=?",(r['id'],))
            
            audit(c,'caption',r['id'],'done',{'provider':prov,'char_count':len(full)})
        
        mark_run(c,'caption',True); c.commit(); print(j({'ok':True,'processed':len(rows)}))
    finally: c.close()

def local_time_zone(name):
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception: return dt.timezone.utc

def queue():
    c=db()
    try:
        current=now();
        expiry_h=int(cfg(c,'freshness_expiry_hours','72'));
        pool_rows=c.execute("SELECT * FROM pool WHERE status='pooled'").fetchall();
        expired=0

        # Expire queued posts whose headline is older than freshness limit; their groups become eligible for replacement.
        stale=c.execute("SELECT p.id,g.id group_id,h.published_at FROM posts p JOIN groups g ON g.id=p.group_id JOIN headlines h ON h.id=g.headline_id WHERE p.status='queued'").fetchall()
        
        for x in stale:
            pub=parse_date(x['published_at']) or current
            if (current-pub).total_seconds()/3600 >= expiry_h:
                c.execute("UPDATE posts SET status='expired',error='freshness expiry' WHERE id=?",(x['id'],)); c.execute("UPDATE groups SET status='ready_for_schedule' WHERE id=?",(x['group_id'],)); expired+=1
        
        for r in pool_rows:
            age=(current-dt.datetime.fromisoformat(r['freshness_ts'].replace('Z','+00:00'))).total_seconds()/3600
            decay=1 if age<=12 else .75 if age<=24 else .5 if age<=48 else .25 if age<=72 else 0

            if decay==0 or age>=expiry_h:
                c.execute("UPDATE pool SET status='expired',effective_priority=0,reason='freshness_expiry' WHERE id=?",(r['id'],)); expired+=1
            
            else: c.execute("UPDATE pool SET effective_priority=priority*? WHERE id=?",(decay,r['id']))
        
        tz=local_time_zone(cfg(c,'timezone','Asia/Karachi'))
        
        # Queue only groups that have complete, uploaded media + caption.
        candidates=c.execute("SELECT g.id,g.headline_id FROM groups g WHERE g.status='ready_for_schedule' AND NOT EXISTS(SELECT 1 FROM posts p WHERE p.group_id=g.id)").fetchall()
        existing=c.execute("SELECT publish_at FROM posts WHERE publish_at IS NOT NULL AND status IN ('queued','published')").fetchall()
        taken=[]
        
        for x in existing:
            try: taken.append(dt.datetime.fromisoformat(x['publish_at'].replace('Z','+00:00')))
            except: pass
        
        max_day=int(cfg(c,'max_posts_per_day','3')); min_gap=int(cfg(c,'min_gap_hours','3'))
        assigned=0
        
        for g in candidates:
            pool=c.execute("SELECT effective_priority FROM pool WHERE headline_id=? AND status='pooled'",(g['headline_id'],)).fetchone()
            if not pool: continue
            slot=None
            for day_off in range(3):
                local_date=(current.astimezone(tz).date()+dt.timedelta(days=day_off))
                for hour in [8,10,13,17,18,19,20,21,22]:
                    local=dt.datetime.combine(local_date,dt.time(hour,0),tzinfo=tz); cand=local.astimezone(dt.timezone.utc)
                    if sum(1 for e in taken if e.astimezone(tz).date()==local_date)>=max_day: continue
                    if any(abs((cand-e).total_seconds())<min_gap*3600 for e in taken): continue
                    # configured no-publish window
                    if local.time() < dt.time(6,0) or local.time() >= dt.time(23,0): continue
                    slot=cand; break
                if slot: break
            if slot:
                pid=f"POST-{g['id']}"; c.execute("INSERT OR IGNORE INTO posts(id,group_id,publish_at,status,created_at) VALUES(?,?,?,?,?)",(pid,g['id'],iso(slot),'queued',iso())); taken.append(slot); assigned+=1
                audit(c,'queue',pid,'queued',{'publish_at':iso(slot),'priority':pool['effective_priority']})
        cap=int(cfg(c,'pool_cap','80')); overflow=c.execute("SELECT id FROM pool WHERE status='pooled' ORDER BY created_at ASC").fetchall()
        for old in overflow[:-cap] if len(overflow)>cap else []:
            c.execute("UPDATE pool SET status='expired',reason='pool_cap_overflow',effective_priority=0 WHERE id=?",(old['id'],)); expired+=1
        mark_run(c,'queue',True); c.commit(); print(j({'ok':True,'assigned':assigned,'expired':expired}))
    finally: c.close()

def graph_api(path, method='GET', data=None, params=None):
    version=os.getenv('META_API_VERSION','v23.0'); token=os.environ.get('META_ACCESS_TOKEN');
    
    if not token: raise RuntimeError('META_ACCESS_TOKEN missing')
    base=f"https://graph.facebook.com/{version}/{path.lstrip('/')}"
    
    if requests:
        if method=='GET': r=requests.get(base,params={**(params or {}),'access_token':token},timeout=60)
        else: r=requests.post(base,data={**(data or {}),'access_token':token},timeout=90)
        r.raise_for_status(); return r.json()
    
    if method=='GET':
        qs=urllib.parse.urlencode({**(params or {}),'access_token':token}); return http_get(base+'?'+qs,timeout=60,max_bytes=2_000_000).json()
    return post_json(base,{**(data or {}),'access_token':token})

def publish():
    c=db()
    try:
        if not cfg_bool(c,'publish_enabled',False): print(j({'ok':True,'skipped':True,'reason':'publish_disabled'})); return
        rows=c.execute("SELECT p.*,g.audit_id FROM posts p JOIN groups g ON g.id=p.group_id WHERE p.status='queued' AND p.publish_at<=? ORDER BY p.publish_at LIMIT 1",(iso(),)).fetchall();
        if not rows: print(j({'ok':True,'processed':0})); return
        ig=os.environ.get('META_IG_USER_ID');
        if not ig: raise RuntimeError('META_IG_USER_ID missing')
        for p in rows:
            group_id=p['group_id'];
            cards=c.execute("SELECT * FROM cards WHERE group_id=? ORDER BY card_no",(group_id,)).fetchall();
            cap=c.execute("SELECT full_caption FROM captions WHERE group_id=? AND variant='A'",(group_id,)).fetchone()

            if len(cards)!=10 or any(not r['img_url'] for r in cards) or not cap: raise RuntimeError(f"post {p['id']} incomplete media/caption")

            if cfg_bool(c,'review_mode',True):
                rq=c.execute("SELECT status FROM review_queue WHERE group_id=?",(group_id,)).fetchone()
                if not rq or rq['status']!='approved':
                    # Put into review queue, do not publish.
                    c.execute("INSERT OR IGNORE INTO review_queue(id,group_id,status,created_at) VALUES(?,?,?,?)",(f"REV-{group_id}",group_id,'pending',iso()));
                    c.commit(); print(j({'ok':True,'waiting_review':True,'group_id':group_id})); return
            child=[]
            
            for i,card in enumerate(cards):
                payload={'image_url':card['img_url'],'is_carousel_item':'true'}

                if i==0: payload['caption']=cap['full_caption']
                child.append(graph_api(f"{ig}/media",'POST',payload)['id'])
            
            parent=graph_api(f"{ig}/media",'POST',{'media_type':'CAROUSEL','children':','.join(child),'caption':cap['full_caption']})['id']
            published=graph_api(f"{ig}/media_publish",'POST',{'creation_id':parent})
            media_id=published.get('id',parent); detail=graph_api(media_id,'GET',params={'fields':'id,permalink'}); permalink=detail.get('permalink')

            c.execute("UPDATE posts SET status='published',published_at=?,media_id=?,permalink=?,attempts=attempts+1 WHERE id=?",(iso(),media_id,permalink,p['id']));
            audit(c,'publish',p['id'],'published',{'media_id':media_id,'permalink':permalink});
            mark_run(c,'publish',True);
            c.commit()
        print(j({'ok':True,'processed':len(rows)}))
    except Exception as e:
        # Record retry without silently dropping.
        if rows:
            p=rows[0];
            attempts=int(p['attempts'])+1;
            st='failed' if attempts>=3 else 'queued';
            c.execute("UPDATE posts SET attempts=?,error=?,status=? WHERE id=?",(attempts,str(e),st,p['id']));
            audit(c,'publish',p['id'],'failed',error=str(e));
            c.commit()
        print(j({'ok':False,'error':str(e)})); raise
    finally: c.close()

def insights():
    c=db()
    try:
        ig=os.environ.get('META_IG_USER_ID')
        if not ig: print(j({'ok':True,'skipped':True,'reason':'META_IG_USER_ID missing'})); return

        # Metric names vary by API version; configure via META_INSIGHT_METRICS.
        metrics=os.getenv('META_INSIGHT_METRICS','reach,impressions,likes,comments,shares,saved')
        data=graph_api(f"{ig}/insights",'GET',params={'metric':metrics,'period':'day'})
        c.execute("INSERT INTO meta_kpis(metric_date,notes,created_at) VALUES(?,?,?) ON CONFLICT(metric_date) DO UPDATE SET notes=excluded.notes",(now().date().isoformat(),j(data),iso()));
        mark_run(c,'insights',True);
        c.commit();
        print(j({'ok':True,'data':data}))
    finally: c.close()

def status_message(c):
    counts={t:c.execute(f'SELECT COUNT(*) n FROM {t}').fetchone()['n'] for t in ['headlines','scored','pool','articles','fact_checks','groups','cards','captions','posts','review_queue']}
    return 'OverWatch status\n'+ '\n'.join(f'{k}: {v}' for k,v in counts.items())

def review(op, group_id=None, note=''):
    c=db()
    try:
        if op not in {'approve','reject'}: raise ValueError('review op must be approve or reject')
        if not group_id: raise ValueError('group id required')
        status='approved' if op=='approve' else 'rejected'
        c.execute("INSERT INTO review_queue(id,group_id,status,reviewer_note,created_at,reviewed_at) VALUES(?,?,?,?,?,?) ON CONFLICT(group_id) DO UPDATE SET status=excluded.status,reviewer_note=excluded.reviewer_note,reviewed_at=excluded.reviewed_at",(f'REV-{group_id}',group_id,status,note,iso(),iso()))
        if status=='rejected':
            c.execute("UPDATE groups SET status='ready_for_caption' WHERE id=?",(group_id,))
            c.execute("UPDATE posts SET status='cancelled',error='rejected by human review' WHERE group_id=? AND status='queued'",(group_id,))
        c.commit(); print(j({'ok':True,'group_id':group_id,'status':status}))
    finally: c.close()

def watchtower():
    c=db();
    try:
        alerts=[]
        counts={t:c.execute(f'SELECT COUNT(*) n FROM {t}').fetchone()['n'] for t in ['headlines','scored','pool','articles','fact_checks','groups','cards','captions','posts','review_queue']}
        cutoff=iso(now()-dt.timedelta(minutes=45)); stuck=c.execute("SELECT COUNT(*) n FROM audit_log WHERE status='processing' AND created_at<?",(cutoff,)).fetchone()['n']

        if stuck: alerts.append(f'{stuck} stuck processing rows >45m')
        if counts['headlines'] and counts['headlines']<int(cfg(c,'ingest_min_headlines','100')): alerts.append('ingest coverage below threshold')

        quarantine=c.execute("SELECT COUNT(*) n FROM fact_checks WHERE verdict='QUARANTINE' AND checked_at>=datetime('now','-1 day')").fetchone()['n'];
        total_fc=c.execute("SELECT COUNT(*) n FROM fact_checks WHERE checked_at>=datetime('now','-1 day')").fetchone()['n']

        if total_fc and quarantine/total_fc>0.15: alerts.append('quarantine rate >15%')
        if c.execute("SELECT COUNT(*) n FROM posts WHERE status='failed' AND attempts>=3").fetchone()['n']: alerts.append('post failed 3 times')

        wf=c.execute("SELECT workflow_name,last_success_at FROM workflow_runs WHERE workflow_name LIKE 'stage:%'").fetchall()

        for w in wf:
            if not w['last_success_at']: continue
            age=(now()-dt.datetime.fromisoformat(w['last_success_at'].replace('Z','+00:00'))).total_seconds()/3600
            if age>24: alerts.append(f"{w['workflow_name']} silent >24h")

        if DB.exists() and DB.stat().st_size>2*1024*1024*1024: alerts.append('database >2GB')
        usage=c.execute("SELECT provider,COUNT(*) n FROM llm_usage WHERE created_at>=datetime('now','-1 day') GROUP BY provider").fetchall();

        for u in usage:
            limit=1000 if u['provider']=='gemini' else 1000
            if u['n']>0.8*limit: alerts.append(f"{u['provider']} usage >80% of configured daily budget")

        audit(c,'watchtower','health','done',{'counts':counts,'alerts':alerts}); mark_run(c,'watchtower',True); c.commit(); print(j({'ok':True,'counts':counts,'alerts':alerts}))
    finally: c.close()

def backup():
    c=db();
    c.execute('PRAGMA wal_checkpoint(TRUNCATE)');
    c.close();
    stamp=now().strftime('%Y%m%d_%H%M%S'); dest=BACKUPS/f'overwatch_{stamp}.db';
    src=sqlite3.connect(DB);
    dst=sqlite3.connect(dest);
    src.backup(dst);
    dst.close();
    src.close();
    print(j({'ok':True,'backup':str(dest)}))

def main():
    op=sys.argv[1] if len(sys.argv)>1 else 'help'
    if op=='scrape': return scrape()
    if op=='score': return score()
    if op in {'write','writer'}: return writer()
    if op=='factcheck': return factcheck()
    if op=='group': return group()
    if op=='media': return media()
    if op=='upload_media': return upload_media()
    if op=='caption': return caption()
    if op=='queue': return queue()
    if op=='publish': return publish()
    if op=='insights': return insights()
    if op=='watchtower': return watchtower()
    if op=='backup': return backup()
    if op=='review' and len(sys.argv)>=4: return review(sys.argv[2],sys.argv[3],sys.argv[4] if len(sys.argv)>4 else '')
    raise SystemExit('unknown stage')

if __name__=='__main__': main()