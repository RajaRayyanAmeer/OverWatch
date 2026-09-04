#!/usr/bin/env python3
import os, sqlite3, json, datetime
ROOT=os.getenv('OVERWATCH_ROOT','/data')
DB=os.path.join(ROOT,'overwatch.db')
HERE=os.path.dirname(os.path.abspath(__file__))
os.makedirs(ROOT,exist_ok=True)
for d in ('media','audit','backups'): os.makedirs(os.path.join(ROOT,d),exist_ok=True)
with open(os.path.join(HERE,'../config/schema.sql'),encoding='utf-8') as f: schema=f.read()
con=sqlite3.connect(DB); con.executescript(schema)
def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def seed(k,v): con.execute("INSERT INTO config(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",(k,str(v),now()))
feeds=[
 ['Google News Top','https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en','mixed'],['Google News World','https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en&topic=w','world'],['Google News Business','https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en&topic=b','business'],['Google News Tech','https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en&topic=t','tech'],['Google News Science','https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en&topic=snc','science'],['Google News Health','https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en&topic=m','health'],['Google News Sports','https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en&topic=s','sports'],['Google News Entertainment','https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en&topic=e','culture'],['Google News AI','https://news.google.com/rss/search?q=artificial+intelligence&hl=en-US','tech'],['BBC World','https://feeds.bbci.co.uk/news/world/rss.xml','world'],['DW All','https://rss.dw.com/rdf/rss-en-all','world'],['Al Jazeera','https://www.aljazeera.com/xml/rss/all.xml','world'],['France24','https://www.france24.com/en/rss','world'],['TechCrunch','https://techcrunch.com/feed/','tech'],['The Verge','https://www.theverge.com/rss/index.xml','tech'],['Hacker News','https://news.ycombinator.com/rss','tech']]
defaults={
 'theme':'NewsBlue','timezone':'Asia/Karachi','review_mode':'on','auto_publish':'false','ingest_enabled':'true','llm_enabled':'true','publish_enabled':'false','llm_primary':'gemini','rubric_version':'v1.1','score_threshold':'72','score_relaxed_threshold':'65','topic_cap':'8','candidate_floor':'30','candidate_ceiling':'40','pool_cap':'80','freshness_expiry_hours':'72','max_posts_per_day':'3','min_gap_hours':'3','no_post_before':'06:00','no_post_after':'23:00','headline_max_age_hours':'48','semantic_dedup_threshold':'0.85','caption_max_chars':'2000','article_min_words':'3000','article_target_min_words':'3200','article_target_max_words':'3500','max_rewrites':'2','factcheck_pass_score':'0.90','factcheck_rewrite_score':'0.75','ingest_min_headlines':'100','dayparts_json':json.dumps([{'start':'06:00','end':'10:00','weight':0.05},{'start':'10:00','end':'13:00','weight':0.15},{'start':'13:00','end':'17:00','weight':0.20},{'start':'17:00','end':'21:00','weight':0.45},{'start':'21:00','end':'23:00','weight':0.15}]),'feeds_json':json.dumps(feeds)}
for k,v in defaults.items(): seed(k,v)
con.commit(); con.close(); print(json.dumps({'ok':True,'db':DB}))
