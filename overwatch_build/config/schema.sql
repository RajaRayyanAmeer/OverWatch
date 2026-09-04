PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS headlines (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  source TEXT NOT NULL,
  url TEXT NOT NULL,
  topic TEXT NOT NULL,
  published_at TEXT,
  fetched_at TEXT NOT NULL,
  raw_xml TEXT,
  title_hash TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_headlines_status ON headlines(status);
CREATE INDEX IF NOT EXISTS idx_headlines_published_at ON headlines(published_at);

CREATE TABLE IF NOT EXISTS scored (
  id TEXT PRIMARY KEY,
  headline_id TEXT NOT NULL UNIQUE REFERENCES headlines(id) ON DELETE CASCADE,
  curiosity REAL, emotion REAL, relevance REAL, freshness REAL,
  visual REAL, authority REAL, shareability REAL,
  total_score REAL NOT NULL,
  dedup_reason TEXT,
  passed INTEGER NOT NULL DEFAULT 0,
  rank INTEGER,
  rationale TEXT,
  rubric_version TEXT NOT NULL,
  embedding_json TEXT,
  status TEXT NOT NULL DEFAULT 'done',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scored_passed_rank ON scored(passed, rank);
CREATE INDEX IF NOT EXISTS idx_scored_status ON scored(status);

CREATE TABLE IF NOT EXISTS pool (
  id TEXT PRIMARY KEY,
  headline_id TEXT NOT NULL UNIQUE REFERENCES headlines(id) ON DELETE CASCADE,
  total_score REAL NOT NULL,
  priority REAL NOT NULL,
  created_at TEXT NOT NULL,
  freshness_ts TEXT NOT NULL,
  expiry_ts TEXT NOT NULL,
  effective_priority REAL,
  status TEXT NOT NULL DEFAULT 'pooled',
  reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_pool_status_priority ON pool(status, effective_priority DESC);

CREATE TABLE IF NOT EXISTS articles (
  id TEXT PRIMARY KEY,
  headline_id TEXT NOT NULL REFERENCES headlines(id) ON DELETE CASCADE,
  article_md TEXT NOT NULL,
  word_count INTEGER NOT NULL,
  draft_rev INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'pending_review',
  outline_json TEXT,
  failed_claims_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status);

CREATE TABLE IF NOT EXISTS fact_checks (
  id TEXT PRIMARY KEY,
  article_id TEXT NOT NULL UNIQUE REFERENCES articles(id) ON DELETE CASCADE,
  claims_json TEXT NOT NULL,
  sources_checked_json TEXT,
  verdict TEXT NOT NULL,
  confidence REAL,
  score REAL,
  notes TEXT,
  rewrite_notes TEXT,
  checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS groups (
  id TEXT PRIMARY KEY,
  audit_id TEXT NOT NULL UNIQUE,
  headline_id TEXT NOT NULL REFERENCES headlines(id),
  article_id TEXT NOT NULL REFERENCES articles(id),
  audit_bundle_path TEXT,
  audit_bundle_json TEXT,
  topic TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ready_for_media',
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_groups_status ON groups(status);

CREATE TABLE IF NOT EXISTS cards (
  id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  card_no INTEGER NOT NULL,
  img_path TEXT,
  img_url TEXT,
  text_snippet TEXT,
  theme TEXT NOT NULL,
  width INTEGER,
  height INTEGER,
  bytes INTEGER,
  status TEXT NOT NULL DEFAULT 'rendered',
  created_at TEXT NOT NULL,
  UNIQUE(group_id, card_no)
);

CREATE TABLE IF NOT EXISTS captions (
  id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  hook TEXT,
  summary TEXT,
  value_line TEXT,
  cta TEXT,
  hashtags TEXT,
  char_count INTEGER,
  variant TEXT NOT NULL,
  full_caption TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ready',
  created_at TEXT NOT NULL,
  UNIQUE(group_id, variant)
);

CREATE TABLE IF NOT EXISTS posts (
  id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL UNIQUE REFERENCES groups(id) ON DELETE CASCADE,
  publish_at TEXT,
  status TEXT NOT NULL DEFAULT 'queued',
  published_at TEXT,
  media_id TEXT,
  permalink TEXT,
  error TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  UNIQUE(publish_at)
);

CREATE INDEX IF NOT EXISTS idx_posts_status_publish_at ON posts(status, publish_at);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workflow TEXT NOT NULL,
  stage TEXT NOT NULL,
  entity_id TEXT,
  status TEXT NOT NULL,
  payload TEXT,
  result TEXT,
  error TEXT,
  duration_ms INTEGER,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity_id);

CREATE TABLE IF NOT EXISTS config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta_kpis (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  metric_date TEXT NOT NULL,
  headlines_in INTEGER DEFAULT 0,
  headlines_unique INTEGER DEFAULT 0,
  scored INTEGER DEFAULT 0,
  articles_ok INTEGER DEFAULT 0,
  rejects INTEGER DEFAULT 0,
  quarantine_count INTEGER DEFAULT 0,
  rewrite_count INTEGER DEFAULT 0,
  posts_published INTEGER DEFAULT 0,
  gemini_requests INTEGER DEFAULT 0,
  groq_requests INTEGER DEFAULT 0,
  llm_failovers INTEGER DEFAULT 0,
  pipeline_ok INTEGER DEFAULT 1,
  notes TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(metric_date)
);

CREATE TABLE IF NOT EXISTS embeddings (
  id TEXT PRIMARY KEY,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  text_hash TEXT NOT NULL UNIQUE,
  model TEXT NOT NULL,
  embedding_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS llm_usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  task TEXT NOT NULL,
  request_units INTEGER NOT NULL DEFAULT 1,
  success INTEGER NOT NULL DEFAULT 1,
  error TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_usage_provider_date ON llm_usage(provider, created_at);

CREATE TABLE IF NOT EXISTS workflow_runs (
  workflow_name TEXT PRIMARY KEY,
  last_success_at TEXT,
  last_failure_at TEXT,
  last_execution_id TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_queue (
  id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL UNIQUE REFERENCES groups(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending',
  reviewer_note TEXT,
  created_at TEXT NOT NULL,
  reviewed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_review_status ON review_queue(status);

CREATE TABLE IF NOT EXISTS golden_scoring (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  headline TEXT NOT NULL,
  expected_pass INTEGER,
  expected_min_score REAL,
  expected_max_score REAL,
  expected_rank INTEGER
);

CREATE TABLE IF NOT EXISTS golden_factcheck (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  article_id TEXT,
  planted_error_count INTEGER DEFAULT 0,
  expected_flags INTEGER DEFAULT 0,
  clean_article INTEGER DEFAULT 0
);
