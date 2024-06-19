CREATE SCHEMA coherebench;

CREATE TABLE coherebench.embeddings_table (
    id TEXT PRIMARY KEY,
    language TEXT,
    title TEXT,
    url TEXT,
    passage TEXT,
    embedding HALFVEC(1024)
);

CREATE INDEX ON coherebench.embeddings_table USING hnsw (embedding halfvec_ip_ops);
CREATE INDEX ON coherebench.embeddings_table (language);
