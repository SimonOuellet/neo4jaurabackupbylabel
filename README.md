# neo4j-backup

Label-level backup & restore for Neo4j databases.

Export nodes by label (with flexible AND/OR filtering), their internal relationships, constraints, and indexes to a portable JSON file. Import into any Neo4j database — same instance or different — with optional clear-before-load that only affects the targeted labels.

## Features

- **Label-scoped export** — select nodes by one or more labels using AND and/or OR logic
- **Secondary label filters** — `--require-label` and `--exclude-label` for fine-grained control
- **Internal relationships** — automatically exports all relationships between exported nodes, including parallel edges
- **Schema preservation** — constraints (uniqueness, node key) and indexes (range, vector, text, fulltext) are saved and recreated on import
- **Safe clear** — `--clear` on import only deletes nodes matching the backup's label criteria; other data is untouched
- **Flexible connection** — target different databases for export and import via CLI flags, `NEO4J_EXPORT_*` / `NEO4J_IMPORT_*` env vars, or a shared `.env` file
- **Resilient** — automatic retry with exponential backoff on transient Neo4j errors
- **Zero config** — single Python file with [PEP 723](https://peps.python.org/pep-0723/) inline metadata; `uv run` handles dependencies automatically

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or `pip install neo4j`
- Neo4j 5.x (tested with AuraDB Pro)

## Quick Start

```bash
# Export all nodes with the JIRA label
uv run neo4j_backup.py export --label JIRA -o jira_backup.json

# Import into another database, clearing JIRA nodes first
uv run neo4j_backup.py import -i jira_backup.json --clear \
    --uri neo4j+ssc://target.databases.neo4j.io --password xxx
```

## Installation

No installation needed — just download the script and run it with `uv`:

```bash
# uv handles dependency resolution automatically
uv run neo4j_backup.py --help
```

Or install the dependency manually:

```bash
pip install neo4j
python neo4j_backup.py --help
```

## Configuration

### Environment Variables

Connection settings can be provided via environment variables or a `.env` file in the working directory.

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | — | Bolt URI (e.g. `neo4j+ssc://xxx.databases.neo4j.io`) |
| `NEO4J_USER` or `NEO4J_USERNAME` | `neo4j` | Authentication user |
| `NEO4J_PASSWORD` | — | Authentication password |
| `NEO4J_DATABASE` | `neo4j` | Database name |

### Per-Operation Overrides

Use prefixed variables to target different databases for export and import without changing the defaults:

| Prefix | Applies to |
|--------|-----------|
| `NEO4J_EXPORT_*` | `export` command |
| `NEO4J_IMPORT_*` | `import` command |

Example `.env` for cross-database migration:

```env
# Source (export)
NEO4J_EXPORT_URI=neo4j+ssc://source.databases.neo4j.io
NEO4J_EXPORT_PASSWORD=source_password

# Target (import)
NEO4J_IMPORT_URI=neo4j+ssc://target.databases.neo4j.io
NEO4J_IMPORT_PASSWORD=target_password
```

### Resolution Order

Connection parameters are resolved in this order (first match wins):

1. CLI flags (`--uri`, `--password`, etc.)
2. Prefixed env vars (`NEO4J_EXPORT_*` or `NEO4J_IMPORT_*`)
3. Default env vars (`NEO4J_URI`, `NEO4J_PASSWORD`, etc.)

## Usage

### Export

```
neo4j_backup.py export [options] -o FILE
```

#### Label Selection

Labels control **which nodes** are exported. Two modes are available and can be combined:

| Flag | Mode | Behavior |
|------|------|----------|
| `--label LABEL` | AND | Nodes must have **all** listed labels (repeatable) |
| `--label-or LABEL` | OR | Nodes must have **at least one** listed label (repeatable) |

At least one `--label` or `--label-or` is required.

#### Secondary Filters

| Flag | Behavior |
|------|----------|
| `--require-label LABEL` | Only include nodes that also carry this label (repeatable) |
| `--exclude-label LABEL` | Exclude nodes that carry this label (repeatable) |

#### Examples

```bash
# All nodes with the JIRA label
uv run neo4j_backup.py export --label JIRA -o jira_backup.json

# AND: nodes that are both JIRA and Issue
uv run neo4j_backup.py export --label JIRA --label Issue -o jira_issues.json

# OR: nodes that are Episodic or Entity
uv run neo4j_backup.py export --label-or Episodic --label-or Entity -o memory.json

# AND + OR: must be JIRA AND (Issue or Epic)
uv run neo4j_backup.py export --label JIRA --label-or Issue --label-or Epic -o issues_epics.json

# Exclude: JIRA nodes but not Comments
uv run neo4j_backup.py export --label JIRA --exclude-label Comment -o jira_no_comments.json

# Require: JIRA nodes that also have the Issue label
uv run neo4j_backup.py export --label JIRA --require-label Issue -o jira_issues.json

# Override connection for this export
uv run neo4j_backup.py export --label JIRA -o backup.json \
    --uri neo4j+ssc://prod.databases.neo4j.io --password xxx
```

### Import

```
neo4j_backup.py import [options] -i FILE
```

| Flag | Description |
|------|-------------|
| `-i FILE` | Path to the backup JSON file (required) |
| `--clear` | Delete nodes matching the backup's labels before importing |
| `--batch-size N` | Number of nodes per MERGE batch (default: 500) |

#### Examples

```bash
# Import into the default database
uv run neo4j_backup.py import -i jira_backup.json

# Clear matching nodes first, then import
uv run neo4j_backup.py import -i jira_backup.json --clear

# Import into a different database
uv run neo4j_backup.py import -i backup.json --clear \
    --uri neo4j+ssc://target.databases.neo4j.io --password xxx

# Smaller batches for constrained environments
uv run neo4j_backup.py import -i large_backup.json --clear --batch-size 100
```

## How It Works

### Export Process

1. **Node selection** — Matches nodes using the label AND/OR criteria plus require/exclude filters
2. **Schema discovery** — Reads constraints and indexes relevant to the exported labels via `SHOW CONSTRAINTS` / `SHOW INDEXES`
3. **Merge key resolution** — For each node, determines the MERGE key from uniqueness constraints (falls back to all properties if none found)
4. **Relationship export** — Finds all relationships where **both** endpoints are in the exported node set (internal relationships only)
5. **Serialization** — Writes everything to a single JSON file with full type preservation

### Import Process

1. **Clear (optional)** — Deletes nodes matching the backup's label criteria in batches of 10,000 using `DETACH DELETE`. Only the matched nodes are removed; other labels are unaffected. `DETACH DELETE` also removes any relationships connected to deleted nodes.
2. **Schema recreation** — Runs the saved `CREATE CONSTRAINT` / `CREATE INDEX` statements with `IF NOT EXISTS`, then waits for indexes to come online
3. **Node import** — Groups nodes by label+merge-key combination and uses `UNWIND` + `MERGE` for efficient batched upserts
4. **Relationship import** — Matches start/end nodes by their merge keys and uses `CREATE` to recreate each relationship, preserving parallel edges between the same node pair

### Backup File Format

The JSON backup file contains:

```json
{
  "metadata": {
    "exported_at": "2026-03-09T17:00:07Z",
    "labels": ["JIRA"],
    "labels_or": [],
    "require_labels": [],
    "exclude_labels": [],
    "source_uri": "neo4j+ssc://xxx.databases.neo4j.io",
    "source_database": "neo4j",
    "node_count": 252060,
    "relationship_count": 591058,
    "label_summary": {"Comment": 186764, "Epic": 562, "Issue": 63308, "...": "..."}
  },
  "constraints": [
    {"name": "...", "type": "UNIQUENESS", "labels": ["Issue"], "properties": ["key"],
     "create_statement": "CREATE CONSTRAINT ..."}
  ],
  "indexes": [
    {"name": "...", "type": "VECTOR", "labels": ["Issue"], "properties": ["embedding"],
     "options": {"indexConfig": {"vector.dimensions": 1536, "...": "..."}},
     "create_statement": "CREATE VECTOR INDEX ..."}
  ],
  "nodes": [
    {"labels": ["Issue", "JIRA"], "properties": {"key": "OWL-123", "...": "..."},
     "merge_keys": ["key"]}
  ],
  "relationships": [
    {"type": "BELONGS_TO_EPIC", "start_node": 0, "end_node": 42,
     "properties": {"...": "..."}}
  ]
}
```

### Property Type Preservation

All Neo4j property types are preserved through serialization:

| Neo4j Type | JSON Representation |
|-----------|-------------------|
| String, Integer, Float, Boolean | Native JSON |
| List | JSON array |
| Bytes | `{"__neo4j__": "bytes", "v": "<base64>"}` |
| Temporal (Date, DateTime, etc.) | `{"__neo4j__": "<TypeName>", "v": "<iso_format>"}` |

## Limitations

- **Internal relationships only** — Only relationships where both endpoints are in the exported node set are included. Cross-label relationships (e.g., `Issue → Person` when only exporting `Issue`) are not exported.
- **No relationship constraints** — Neo4j doesn't support uniqueness constraints on relationships. The import uses `CREATE` for relationships, so re-importing without `--clear` will create duplicates.
- **Large exports** — Exports with vector embeddings can produce large files (e.g., 2+ GB for 250K nodes with 1536-dimensional embeddings). Ensure sufficient disk space.
- **Single database** — Targets a single Neo4j database per operation. Use `--database` or env vars to switch.
- **Neo4j 5.x** — Uses `SHOW CONSTRAINTS/INDEXES` syntax from Neo4j 5. Not compatible with Neo4j 4.x.

## License

MIT
