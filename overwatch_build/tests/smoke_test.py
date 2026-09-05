import os
import sqlite3
import subprocess
import tempfile
import datetime as dt
from pathlib import Path

ROOT=Path(tempfile.mkdtemp(prefix='ow-smoke-'))
os.environ['OVERWATCH_ROOT']=str(ROOT)
INIT=Path(__file__).parents[1]/'scripts/init_db.py'
WORK=Path(__file__).parents[1]/'scripts/agent_worker.py'

subprocess.check_call(['python3',str(INIT)],env=os.environ.copy())
con=sqlite3.connect(ROOT/'overwatch.db');
con.row_factory=sqlite3.Row
now=dt.datetime.now(dt.timezone.utc).isoformat()

# Seed one complete verified group and one pooled candidate for queue/publish gating tests.
con.execute("INSERT INTO headlines VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ('H-test',
             'Test headline',
             'BBC',
             'https://example.com',
             'world',
             now,
             now,
             None,
             'hash-test',
             'scored',
             0,
             None,
             now))

con.execute("INSERT INTO scored(id,headline_id,total_score,passed,rank,rubric_version,status,created_at) VALUES(?,?,?,?,?,?,?,?)",
            ('S-test','H-test',90,1,1,'v1.1','done',now))

exp=(dt.datetime.now(dt.timezone.utc)+dt.timedelta(hours=72)).isoformat()

con.execute("INSERT INTO pool(id,headline_id,total_score,priority,created_at,freshness_ts,expiry_ts,status) VALUES(?,?,?,?,?,?,?,?)",
            ('P-test','H-test',90,90,now,now,exp,'pooled'))
con.execute("INSERT INTO articles(id,headline_id,article_md,word_count,draft_rev,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            ('A-test','H-test','x '*3000,3001,1,'verified',now,now))
con.execute("INSERT INTO fact_checks(id,article_id,claims_json,verdict,confidence,score,checked_at) VALUES(?,?,?,?,?,?,?)",
            ('FC-test','A-test','[]','PASS',1,.99,now))
con.execute("INSERT INTO groups(id,audit_id,headline_id,article_id,topic,status,created_at) VALUES(?,?,?,?,?,?,?)",
            ('G-test','OW-test-01','H-test','A-test','world','ready_for_schedule',now))

for i in range(1,11):
    con.execute("INSERT INTO cards(id,group_id,card_no,img_path,img_url,text_snippet,theme,width,height,bytes,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (f'C-{i}','G-test',i,'/tmp/x',f'https://example.com/{i}.png','x','NewsBlue',1080,1350,200001,'ready',now))
con.execute("INSERT INTO captions(id,group_id,variant,full_caption,char_count,status,created_at) VALUES(?,?,?,?,?,?,?)",
            ('CAP-test','G-test','A','test caption',12,'ready',now))
con.commit();
con.close()

subprocess.check_call(['python3',str(WORK),'queue'],env=os.environ.copy())
con=sqlite3.connect(ROOT/'overwatch.db');
con.row_factory=sqlite3.Row
row=con.execute("SELECT * FROM posts WHERE group_id='G-test'").fetchone()
assert row and row['status']=='queued' and row['publish_at']
print('SMOKE_OK',ROOT,row['publish_at'])