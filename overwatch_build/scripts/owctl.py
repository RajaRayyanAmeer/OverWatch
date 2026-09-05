#!/usr/bin/env python3
import os
import sys
import sqlite3
import json
import datetime
import hashlib
import re

ROOT=os.getenv('OVERWATCH_ROOT','/data');
DB=os.path.join(ROOT,'overwatch.db')

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def c():
    x=sqlite3.connect(DB,timeout=30);
    x.row_factory=sqlite3.Row
    x.execute('PRAGMA foreign_keys=ON');
    x.execute('PRAGMA journal_mode=WAL');
    x.execute('PRAGMA busy_timeout=10000');
    return x

def counts(x):
    ts=['headlines',
        'scored',
        'pool',
        'articles',
        'fact_checks',
        'groups',
        'cards',
        'captions',
        'posts',
        'review_queue']
    return {t:x.execute(f'SELECT COUNT(*) n FROM {t}').fetchone()['n'] for t in ts}

def main():
    op=sys.argv[1] if len(sys.argv)>1 else 'help'; x=c()
    try:
        if op=='config': print(json.dumps({r['key']:r['value'] for r in x.execute('SELECT key,value FROM config')})); return

        if op in ('counts','health'):
            z=counts(x)
            if op=='health': z['db_bytes']=os.path.getsize(DB)
            print(json.dumps(z)); return

        if op=='claim':
            stage=sys.argv[2]; lim=int(sys.argv[3]) if len(sys.argv)>3 else 20
            q={
            'score':("SELECT * FROM headlines WHERE status='pending' ORDER BY COALESCE(published_at,created_at) LIMIT ?",'headlines'),
            'write':("SELECT s.*,h.title,h.url,h.source,h.topic,h.published_at FROM scored s JOIN headlines h ON h.id=s.headline_id WHERE s.passed=1 AND NOT EXISTS(SELECT 1 FROM articles a WHERE a.headline_id=s.headline_id) ORDER BY COALESCE(s.rank,999) LIMIT ?",'scored'),
            'factcheck':("SELECT a.*,h.title,h.url,h.source,h.topic FROM articles a JOIN headlines h ON h.id=a.headline_id WHERE a.status='pending_review' AND a.draft_rev<=3 LIMIT ?",'articles'),
            'group':("SELECT a.*,h.title,h.url,h.source,h.topic,s.total_score,s.rubric_version,fc.verdict,fc.score fc_score,fc.claims_json,fc.sources_checked_json FROM articles a JOIN headlines h ON h.id=a.headline_id JOIN scored s ON s.headline_id=h.id JOIN fact_checks fc ON fc.article_id=a.id WHERE a.status='verified' AND NOT EXISTS(SELECT 1 FROM groups g WHERE g.article_id=a.id) LIMIT ?",'articles'),
            'media':("SELECT * FROM groups WHERE status='ready_for_media' LIMIT ?",'groups'),
            'caption':("SELECT * FROM groups WHERE status='ready_for_caption' AND NOT EXISTS(SELECT 1 FROM captions c WHERE c.group_id=groups.id AND c.variant='A') LIMIT ?",'groups')}
            sql,t=q[stage]; rows=x.execute(sql,(lim,)).fetchall()

            for r in rows:
                x.execute(f"UPDATE {t} SET status='processing' WHERE id=?",(r['id'],))

            x.commit(); print(json.dumps([dict(r) for r in rows],ensure_ascii=False)); return

        if op=='queue':
            rows=x.execute("SELECT id,freshness_ts,priority FROM pool WHERE status='pooled'").fetchall();
            expired=0
            nowd=datetime.datetime.now(datetime.timezone.utc)

            for r in rows:
                age=(nowd-datetime.datetime.fromisoformat(r['freshness_ts'].replace('Z','+00:00'))).total_seconds()/3600
                decay=1
                if age<=12 else .75 if age<=24 else .5 if age<=48 else .25 if age<=72 else 0

                if decay==0:
                    x.execute("UPDATE pool SET status='expired',effective_priority=0,reason='freshness_expiry' WHERE id=?",(r['id'],));
                    expired+=1
                else:
                    x.execute('UPDATE pool SET effective_priority=priority*? WHERE id=?',(decay,r['id']))

            x.commit(); print(json.dumps({'ok':True,'expired':expired})); return

        if op=='save':
            d=json.loads(sys.stdin.read() or '{}');
            a=d['action'];
            z=d['data']

            if a=='headline':
                h=hashlib.md5(re.sub(r'[^\w\s]',' ',z['title'].lower()).encode()).hexdigest()
                x.execute("INSERT INTO headlines(id,title,source,url,topic,published_at,fetched_at,raw_xml,title_hash,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(title_hash) DO NOTHING",
                          (z['id'],
                           z['title'],
                           z.get('source','unknown'),
                           z['url'],
                           z.get('topic','world'),
                           z.get('published_at'),
                           z.get('fetched_at',now()),
                           z.get('raw_xml'),
                           h,
                           'pending',
                           now()))
            elif a=='score':
                x.execute("INSERT INTO scored(id,headline_id,curiosity,emotion,relevance,freshness,visual,authority,shareability,total_score,passed,rank,rationale,rubric_version,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(headline_id) DO UPDATE SET total_score=excluded.total_score,passed=excluded.passed,rank=excluded.rank,rationale=excluded.rationale",
                          (z['id'],
                           z['headline_id'],
                           z['curiosity'],
                           z['emotion'],
                           z['relevance'],
                           z['freshness'],
                           z['visual'],
                           z['authority'],
                           z['shareability'],
                           z['total_score'],
                           int(z['passed']),
                           z.get('rank'),
                           z.get('rationale'),
                           z.get('rubric_version','v1.1'),
                           'done',
                           now()))

                x.execute('UPDATE headlines SET status=? WHERE id=?',
                          ('scored' if z['passed'] else 'rejected',
                           z['headline_id']))

            elif a=='article':
                x.execute("INSERT INTO articles(id,headline_id,article_md,word_count,draft_rev,status,outline_json,failed_claims_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET article_md=excluded.article_md,word_count=excluded.word_count,draft_rev=excluded.draft_rev,status=excluded.status,failed_claims_json=excluded.failed_claims_json,updated_at=excluded.updated_at",
                          (z['id'],
                           z['headline_id'],
                           z['article_md'],
                           z['word_count'],
                           z.get('draft_rev',1),
                           z.get('status','pending_review'),
                           z.get('outline_json'),
                           z.get('failed_claims_json'),
                           now(),
                           now()))

            elif a=='factcheck':
                x.execute("INSERT INTO fact_checks(id,article_id,claims_json,sources_checked_json,verdict,confidence,score,notes,rewrite_notes,checked_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(article_id) DO UPDATE SET claims_json=excluded.claims_json,sources_checked_json=excluded.sources_checked_json,verdict=excluded.verdict,confidence=excluded.confidence,score=excluded.score,notes=excluded.notes,rewrite_notes=excluded.rewrite_notes,checked_at=excluded.checked_at",
                          (z['id'],
                           z['article_id'],
                           json.dumps(z.get('claims',[])),
                           json.dumps(z.get('sources_checked',[])),
                           z['verdict'],
                           z.get('confidence'),
                           z.get('score'),
                           z.get('notes'),
                           z.get('rewrite_notes'),
                           now()))

                st={'PASS':'verified',
                    'REWRITE':'pending_review',
                    'QUARANTINE':'quarantined'}[z['verdict']];
                inc=1 if z['verdict']=='REWRITE' else 0

                x.execute('UPDATE articles SET status=?,failed_claims_json=?,draft_rev=draft_rev+?,updated_at=? WHERE id=?',
                          (st,
                           json.dumps(z.get('failed_claims',[])),
                           inc,
                           now(),
                           z['article_id']))

            elif a=='group':
                x.execute("INSERT INTO groups(id,audit_id,headline_id,article_id,audit_bundle_path,audit_bundle_json,topic,status,created_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING",
                          (z['id'],
                           z['audit_id'],
                           z['headline_id'],
                           z['article_id'],
                           z.get('audit_bundle_path'),
                           json.dumps(z.get('audit_bundle',{})),
                           z['topic'],
                           'ready_for_media',now()))

                x.execute("UPDATE articles SET status='grouped',updated_at=? WHERE id=?",
                          (now(),z['article_id']))

            elif a=='caption':
                x.execute("INSERT INTO captions(id,group_id,hook,summary,value_line,cta,hashtags,char_count,variant,full_caption,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(group_id,variant) DO UPDATE SET full_caption=excluded.full_caption,char_count=excluded.char_count",
                          (z['id'],
                           z['group_id'],
                           z.get('hook'),
                           z.get('summary'),
                           z.get('value_line'),
                           z.get('cta'),
                           z.get('hashtags'),
                           z.get('char_count'),
                           z.get('variant','A'),
                           z['full_caption'],
                           'ready',
                           now()))

            elif a=='post':
                x.execute("INSERT INTO posts(id,group_id,publish_at,status,created_at) VALUES(?,?,?,?,?) ON CONFLICT(group_id) DO UPDATE SET publish_at=excluded.publish_at,status=excluded.status",
                          (z['id'],
                           z['group_id'],
                           z.get('publish_at'),
                           z.get('status','queued'),
                           now()))

            elif a=='review':
                x.execute("INSERT INTO review_queue(id,group_id,status,reviewer_note,created_at,reviewed_at) VALUES(?,?,?,?,?,?) ON CONFLICT(group_id) DO UPDATE SET status=excluded.status,reviewer_note=excluded.reviewer_note,reviewed_at=excluded.reviewed_at",
                          (z['id'],
                           z['group_id'],
                           z['status'],
                           z.get('note'),
                           now(),
                           now() if z['status']!='pending' else None))

            else:
                raise ValueError(a)

            x.commit();
            print(json.dumps({'ok':True,'action':a})); return

        raise SystemExit('unknown op')
    
    finally:
        x.close()

if __name__=='__main__': main()