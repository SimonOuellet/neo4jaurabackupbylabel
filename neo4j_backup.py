#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "neo4j",
# ]
# ///
"""Neo4j label-level backup & restore.

Export nodes of a specific label (with optional secondary label filters)
and their internal relationships to a JSON file.
Import back into any Neo4j database with optional clear-before-load.

Environment variables (.env supported):
  NEO4J_URI / NEO4J_PASSWORD / NEO4J_USERNAME / NEO4J_DATABASE  — default connection
  NEO4J_EXPORT_URI / NEO4J_EXPORT_PASSWORD / ...                — export source override
  NEO4J_IMPORT_URI / NEO4J_IMPORT_PASSWORD / ...                — import target override
"""

import argparse
import base64
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Any


# ── .env loader ──────────────────────────────────────────────────────────

def load_env_file(filepath: str = ".env", override: bool = False) -> None:
    """Load environment variables from a .env file.
    
    Args:
        filepath: Path to the .env file. Defaults to ".env".
        override: If True, overwrite existing environment variables.
                  If False, only set variables that are not already present.
    """
    if not os.path.exists(filepath):
        return
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if override or key not in os.environ:
                    os.environ[key] = value


load_env_file()


# ── Configuration ────────────────────────────────────────────────────────

def _env(name: str, default: str | None = None) -> str | None:
    """Fetch an environment variable with an optional default.
    
    Args:
        name: The target environment variable name.
        default: The default value if the environment variable is not defined.
        
    Returns:
        The environment variable value as a string, or the default.
    """
    return os.getenv(name, default)


NEO4J_URI = _env("NEO4J_URI")
NEO4J_USER = _env("NEO4J_USER") or _env("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = _env("NEO4J_PASSWORD")
NEO4J_DATABASE = _env("NEO4J_DATABASE", "neo4j")


# ── Retry session ────────────────────────────────────────────────────────

MAX_RETRIES: int = 5
RETRY_DELAY: int = 5  # seconds


def _is_transient(e: Exception) -> bool:
    """Determine if a Neo4j exception is transient and should be retried.
    
    Args:
        e: The exception raised by the Neo4j driver.
        
    Returns:
        True if the error indicates a temporary failure (like network issues), False otherwise.
    """
    s = str(e).lower()
    return any(k in s for k in [
        "transient", "replication", "shutting down", "serviceunavailable",
        "session expired", "connection", "no data", "defunct", "raft",
        "502", "503", "504", "bad gateway", "service unavailable",
        "gateway timeout", "rate limit", "too many requests",
    ])


class RetrySession:
    """Neo4j session wrapper with automatic retry on transient errors."""

    def __init__(self, driver: Any, database: str | None) -> None:
        """Initialize the RetrySession.
        
        Args:
            driver: The Neo4j GraphDatabase driver instance.
            database: The target Neo4j database name.
        """
        self.driver = driver
        self.database = database
        self._session = None

    def __enter__(self) -> "RetrySession":
        """Open the session upon entering the context."""
        self._session = self.driver.session(database=self.database)
        return self

    def __exit__(self, *args: Any) -> None:
        """Close the session upon exiting the context."""
        if self._session:
            self._session.close()

    def run(self, query: str, params: dict[str, Any] | None = None) -> Any:
        """Execute a Cypher query with automatic retry logic.
        
        Args:
            query: The Cypher query string to execute.
            params: Optional parameters dictionary for the query.
            
        Returns:
            The result of the Cypher execution.
        """
        last = None
        for attempt in range(MAX_RETRIES):
            try:
                if self._session is None:
                    self._session = self.driver.session(database=self.database)
                return self._session.run(query, params or {})
            except Exception as e:
                last = e
                if _is_transient(e) and attempt < MAX_RETRIES - 1:
                    wait = RETRY_DELAY * (attempt + 1)
                    print(f"  Transient error (attempt {attempt + 1}), retrying in {wait}s…")
                    time.sleep(wait)
                    try:
                        self._session.close()
                    except Exception:
                        pass
                    self._session = None
                else:
                    raise
        raise last


# ── Connection helpers ───────────────────────────────────────────────────

def _connect(uri: str, user: str, password: str, database: str | None) -> Any:
    """Establish and verify a connection to a Neo4j database.
    
    Args:
        uri: The Neo4j database URI.
        user: The username for authentication.
        password: The password for authentication.
        database: The target database name (for logging context).
        
    Returns:
        An authenticated and verified Neo4j driver instance.
    """
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    print(f"Connected to {uri} (database: {database})")
    return driver


def _resolve_connection(args: argparse.Namespace, prefix: str) -> tuple[str | None, str | None, str | None, str | None]:
    """Resolve connection details falling back through defined sources.
    
    Precedence: CLI flags > NEO4J_{PREFIX}* env > NEO4J_* env > defaults.
    
    Args:
        args: The CLI arguments parsed by argparse.
        prefix: The connection prefix (e.g., 'EXPORT_', 'IMPORT_').
        
    Returns:
        A tuple containing (uri, user, password, database).
    """
    uri = (getattr(args, "uri", None)
           or _env(f"NEO4J_{prefix}URI") or NEO4J_URI)
    user = (getattr(args, "user", None)
            or _env(f"NEO4J_{prefix}USER")
            or _env(f"NEO4J_{prefix}USERNAME") or NEO4J_USER)
    pwd = (getattr(args, "password", None)
           or _env(f"NEO4J_{prefix}PASSWORD") or NEO4J_PASSWORD)
    db = (getattr(args, "database", None)
          or _env(f"NEO4J_{prefix}DATABASE") or NEO4J_DATABASE)
    return uri, user, pwd, db


# ── Property serialization ──────────────────────────────────────────────

def _serialize(val: Any) -> Any:
    """Convert a Neo4j property value to a JSON-safe representation.
    
    Args:
        val: The raw property value straight from Neo4j (e.g., temporal, spatial).
        
    Returns:
        A JSON-serializable version of the property mapping the type structure.
    """
    if val is None or isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, list):
        return [_serialize(v) for v in val]
    if isinstance(val, bytes):
        return {"__neo4j__": "bytes", "v": base64.b64encode(val).decode()}
    # Temporal / spatial types → tagged string
    return {"__neo4j__": type(val).__name__, "v": str(val)}


def _deserialize(val: Any) -> Any:
    """Restore a serialized property value into its original Neo4j target type.
    
    Args:
        val: The serialized mapping extracted from the JSON backup.
        
    Returns:
        The Python object ready to be safely submitted back to Neo4j operations.
    """
    if val is None or isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, list):
        return [_deserialize(v) for v in val]
    if isinstance(val, dict):
        if "__neo4j__" in val:
            t, v = val["__neo4j__"], val["v"]
            if t == "bytes":
                return base64.b64decode(v)
            try:
                import neo4j.time as nt
                cls = getattr(nt, t, None)
                if cls and hasattr(cls, "from_iso_format"):
                    return cls.from_iso_format(v)
            except Exception:
                pass
            try:
                import neo4j.spatial as ns
                if t in ("CartesianPoint", "WGS84Point"):
                    m = re.match(r"^\w+\((.*)\)$", v)
                    if m:
                        coords = [float(x.strip()) for x in m.group(1).split(",")]
                        return getattr(ns, t)(coords)
            except Exception:
                pass
            return v  # fallback → string
        # Unknown dict → JSON string (Neo4j can't store maps as properties)
        return json.dumps(val, ensure_ascii=False)
    return str(val)


# ── Cypher helpers ───────────────────────────────────────────────────────

def _esc(name: str) -> str:
    """Backtick-escape a Cypher identifier safely.
    
    Args:
        name: The raw identifier string (e.g., label or property key).
        
    Returns:
        The safely escaped Cypher identifier.
    """
    return f"`{name.replace('`', '``')}`"


def _labels(lst: list[str]) -> str:
    """Build a Cypher label chain from a list of label names.
    
    Args:
        lst: Collection of label strings.
        
    Returns:
        A unified string for MATCH patterns (e.g., '`Label1`:`Label2`').
    """
    return ":".join(_esc(l) for l in lst)


def _if_not_exists(stmt: str | None) -> str | None:
    """Inject IF NOT EXISTS into a CREATE CONSTRAINT/INDEX statement safely.
    
    Args:
        stmt: The original structural CREATE statement.
        
    Returns:
        The adjusted statement formatted dynamically to be idempotent.
    """
    if not stmt or "IF NOT EXISTS" in stmt:
        return stmt
    return re.sub(
        r"(CREATE\s+(?:(?:VECTOR|RANGE|TEXT|FULLTEXT|POINT)\s+)?"
        r"(?:INDEX|CONSTRAINT)\s+(?:`[^`]+`|\S+))\s+(FOR|ON)",
        r"\1 IF NOT EXISTS \2",
        stmt,
    )


# ── Schema discovery ────────────────────────────────────────────────────

def _discover_schema(session: Any, all_labels: set[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (constraints, indexes) relevant to the given label set.
    
    Args:
        session: An active Neo4j (retry) session or standard transaction.
        all_labels: The collective set of all node labels queried.
        
    Returns:
        A tuple of (constraints_list, indexes_list) mapped directly to matching labels.
    """
    constraints, indexes = [], []

    # Constraints
    try:
        for rec in session.run(
            "SHOW CONSTRAINTS "
            "YIELD name, type, entityType, labelsOrTypes, properties, createStatement"
        ):
            if rec["entityType"] != "NODE":
                continue
            if any(l in all_labels for l in rec["labelsOrTypes"]):
                constraints.append({
                    "name": rec["name"],
                    "type": rec["type"],
                    "labels": list(rec["labelsOrTypes"]),
                    "properties": list(rec["properties"]),
                    "create_statement": rec["createStatement"],
                })
    except Exception as e:
        print(f"  Warning: constraint discovery failed: {e}")

    # Indexes (skip LOOKUP and constraint-owned)
    try:
        for rec in session.run(
            "SHOW INDEXES "
            "YIELD name, type, entityType, labelsOrTypes, properties, "
            "owningConstraint, options, createStatement"
        ):
            if rec["entityType"] != "NODE" or rec["type"] == "LOOKUP":
                continue
            if rec["owningConstraint"] is not None:
                continue
            if any(l in all_labels for l in rec["labelsOrTypes"]):
                indexes.append({
                    "name": rec["name"],
                    "type": rec["type"],
                    "labels": list(rec["labelsOrTypes"]),
                    "properties": list(rec["properties"]),
                    "options": dict(rec["options"]) if rec["options"] else {},
                    "create_statement": rec["createStatement"],
                })
    except Exception as e:
        print(f"  Warning: index discovery failed: {e}")

    return constraints, indexes


def _build_merge_map(constraints: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Map label -> list of merge-key property names from uniqueness constraints.
    
    Args:
        constraints: The raw constraint dictionary elements.
        
    Returns:
        Mapping from label names to lists of their respective unique keys.
    """
    m = {}
    for c in constraints:
        if c["type"] in ("UNIQUENESS", "NODE_KEY", "NODE_PROPERTY_UNIQUENESS"):
            for lbl in c["labels"]:
                if lbl not in m:
                    m[lbl] = c["properties"]
    return m


def _merge_keys_for(node_labels: list[str], merge_map: dict[str, list[str]]) -> list[str] | None:
    """Return the merge-key property names for a node, based on its labels.
    
    Args:
        node_labels: Sequential labels applied to a distinct node.
        merge_map: Constraint mappings mapping label names to their properties.
        
    Returns:
        Mapped merge-keys or None if constraints fail to register exactly.
    """
    for lbl in node_labels:
        if lbl in merge_map:
            return merge_map[lbl]
    return None


# ── Export ───────────────────────────────────────────────────────────────

def _build_label_clause(labels_and: list[str], labels_or: list[str]) -> tuple[str, list[str], str]:
    """Build (match_clause, where_fragment, display_str) for label selection.

    Args:
        labels_and: Required labels mapped concurrently using basic MATCH expressions.
        labels_or: Any matching properties structured across OR sequences iteratively.
        
    Returns:
        A tuple mapping to the main MATCH clause strings: (match_clause, parts_where, display).
    """
    parts_where = []
    if labels_and:
        match_clause = ":".join(_esc(l) for l in labels_and)
    else:
        match_clause = ""

    if labels_or:
        or_frag = " OR ".join(f"n:{_esc(l)}" for l in labels_or)
        parts_where.append(f"({or_frag})")

    if not match_clause and not parts_where:
        raise ValueError("At least one --label or --label-or is required")

    # Display string
    display_parts = []
    if labels_and:
        display_parts.append(":".join(labels_and))
    if labels_or:
        display_parts.append("(" + " | ".join(labels_or) + ")")
    display = " + ".join(display_parts)

    return match_clause, parts_where, display


def do_export(args: argparse.Namespace) -> None:
    """Execute the export process mapping metadata, variables, constraints and relationships.
    
    Args:
        args: Terminally matched argument namespace parsing via python's `argparse`.
    """
    labels_and = args.label or []
    labels_or = args.label_or or []
    require = args.require_label or []
    exclude = args.exclude_label or []
    output = args.output

    if not labels_and and not labels_or:
        print("Error: at least one --label or --label-or is required.")
        sys.exit(1)

    uri, user, pwd, db = _resolve_connection(args, "EXPORT_")
    if not uri or not pwd:
        print("Error: Neo4j connection not configured. "
              "Set NEO4J_URI / NEO4J_PASSWORD or use --uri / --password.")
        sys.exit(1)

    driver = _connect(uri, user, pwd, db)
    match_clause, label_where, display = _build_label_clause(labels_and, labels_or)
    try:
        with RetrySession(driver, db) as session:
            # ── 1. Export nodes ──────────────────────────────────────
            where_parts = list(label_where)
            where_parts += [f"n:{_esc(r)}" for r in require]
            where_parts += [f"NOT n:{_esc(e)}" for e in exclude]
            where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

            match_expr = f"(n:{match_clause})" if match_clause else "(n)"

            print(f"\nExporting {display}"
                  + (f" (require: {require})" if require else "")
                  + (f" (exclude: {exclude})" if exclude else ""))

            records = session.run(
                f"MATCH {match_expr} {where} "
                f"RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props"
            )

            nodes = []
            eid_to_idx = {}
            all_labels = set()

            for rec in records:
                eid = rec["eid"]
                lbls = sorted(rec["labels"])
                props = {k: _serialize(v) for k, v in rec["props"].items()}
                eid_to_idx[eid] = len(nodes)
                all_labels.update(lbls)
                nodes.append({"labels": lbls, "properties": props})

            print(f"  {len(nodes)} nodes ({len(all_labels)} labels: {sorted(all_labels)})")
            if not nodes:
                print("Nothing to export.")
                return

            # ── 2. Schema ────────────────────────────────────────────
            print("Discovering schema …")
            constraints, indexes = _discover_schema(session, all_labels)
            print(f"  {len(constraints)} constraints, {len(indexes)} indexes")

            # ── 3. Merge keys ────────────────────────────────────────
            merge_map = _build_merge_map(constraints)
            no_key_count = 0
            for n in nodes:
                mk = _merge_keys_for(n["labels"], merge_map)
                if mk:
                    n["merge_keys"] = mk
                else:
                    no_key_count += 1
                    n["merge_keys"] = list(n["properties"].keys())

            if no_key_count:
                print(f"  Warning: {no_key_count} nodes lack uniqueness constraints "
                      f"→ all properties used as merge keys")

            # ── 4. Internal relationships ────────────────────────────
            print("Exporting internal relationships …")
            rels = []
            eid_list = list(eid_to_idx.keys())
            # Use the exported elementIds to find internal rels (batching to avoid parameter limits)
            rel_batch_size = 10000
            for i in range(0, len(eid_list), rel_batch_size):
                batch_eids = eid_list[i:i + rel_batch_size]
                for rec in session.run(
                    "UNWIND $eids AS eid "
                    "MATCH (a)-[r]->(b) "
                    "WHERE elementId(a) = eid "
                    "RETURN elementId(a) AS a_eid, elementId(b) AS b_eid, "
                    "       type(r) AS rtype, properties(r) AS rprops",
                    {"eids": batch_eids},
                ):
                    a_idx = eid_to_idx.get(rec["a_eid"])
                    b_idx = eid_to_idx.get(rec["b_eid"])
                    if a_idx is not None and b_idx is not None:
                        rprops = {k: _serialize(v) for k, v in rec["rprops"].items()}
                        rels.append({
                            "type": rec["rtype"],
                            "start_node": a_idx,
                            "end_node": b_idx,
                            "properties": rprops,
                        })

            print(f"  {len(rels)} relationships")

            # ── 5. Write JSON ────────────────────────────────────────
            data = {
                "metadata": {
                    "exported_at": datetime.utcnow().isoformat() + "Z",
                    "labels": labels_and,
                    "labels_or": labels_or,
                    "require_labels": require,
                    "exclude_labels": exclude,
                    "source_uri": uri,
                    "source_database": db,
                    "node_count": len(nodes),
                    "relationship_count": len(rels),
                    "label_summary": {
                        l: sum(1 for n in nodes if l in n["labels"])
                        for l in sorted(all_labels)
                    },
                },
                "constraints": constraints,
                "indexes": indexes,
                "nodes": nodes,
                "relationships": rels,
            }

            with open(output, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)

            size_mb = os.path.getsize(output) / (1024 * 1024)
            print(f"\nExported to {output} ({size_mb:.1f} MB)")
            print(f"  {len(nodes)} nodes, {len(rels)} relationships, "
                  f"{len(constraints)} constraints, {len(indexes)} indexes")
    finally:
        driver.close()


# ── Import ───────────────────────────────────────────────────────────────

def do_import(args: argparse.Namespace) -> None:
    """Execute the sequential import process returning data, schema, variables and bindings.
    
    Args:
        args: Terminally matched argument namespace variables initialized from file properties.
    """
    input_file = args.input
    batch_size = args.batch_size

    print(f"Reading {input_file} …")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    meta = data["metadata"]
    # Support old format ("label": str), "labels" (AND list), "labels_or" (OR list)
    labels_and = meta.get("labels") or ([meta["label"]] if "label" in meta else [])
    labels_or = meta.get("labels_or", [])
    nodes = data["nodes"]
    rels = data["relationships"]
    constraints_data = data.get("constraints", [])
    indexes_data = data.get("indexes", [])

    _, _, display = _build_label_clause(labels_and, labels_or)
    print(f"  Labels: {display}")
    print(f"  {meta['node_count']} nodes, {meta['relationship_count']} relationships")
    print(f"  From: {meta.get('source_uri', '?')} @ {meta.get('exported_at', '?')}")

    uri, user, pwd, db = _resolve_connection(args, "IMPORT_")
    if not uri or not pwd:
        print("Error: Neo4j connection not configured.")
        sys.exit(1)

    driver = _connect(uri, user, pwd, db)
    try:
        with RetrySession(driver, db) as session:
            # ── Clear ────────────────────────────────────────────────
            if args.clear:
                require = meta.get("require_labels", [])
                exclude = meta.get("exclude_labels", [])
                clear_clause, clear_where, clear_display = _build_label_clause(
                    labels_and, labels_or)
                clear_match_expr = f"(n:{clear_clause})" if clear_clause else "(n)"
                # Apply the same require/exclude filters used during export
                where_parts = list(clear_where)
                where_parts += [f"n:{_esc(r)}" for r in require]
                where_parts += [f"NOT n:{_esc(e)}" for e in exclude]
                clear_where_str = (
                    "WHERE " + " AND ".join(where_parts)) if where_parts else ""
                filter_desc = ""
                if require:
                    filter_desc += f" (require: {require})"
                if exclude:
                    filter_desc += f" (exclude: {exclude})"
                print(f"\nClearing {clear_display}{filter_desc} nodes …")
                total = 0
                while True:
                    result = session.run(
                        f"MATCH {clear_match_expr} {clear_where_str} "
                        f"WITH n LIMIT 10000 DETACH DELETE n "
                        f"RETURN count(*) AS deleted"
                    )
                    deleted = result.single()["deleted"]
                    total += deleted
                    if deleted == 0:
                        break
                    print(f"  deleted {total} …")
                print(f"  Cleared {total} nodes")

            # ── Constraints ──────────────────────────────────────────
            if constraints_data:
                print(f"\nRecreating {len(constraints_data)} constraints …")
                for c in constraints_data:
                    stmt = _if_not_exists(c.get("create_statement"))
                    if not stmt:
                        continue
                    try:
                        session.run(stmt)
                    except Exception as e:
                        if "already exists" not in str(e).lower():
                            print(f"  Warning: {c['name']}: {e}")

            # ── Indexes ──────────────────────────────────────────────
            if indexes_data:
                print(f"Recreating {len(indexes_data)} indexes …")
                for idx in indexes_data:
                    stmt = _if_not_exists(idx.get("create_statement"))
                    if not stmt:
                        continue
                    try:
                        session.run(stmt)
                    except Exception as e:
                        if "already exists" not in str(e).lower():
                            print(f"  Warning: {idx['name']}: {e}")

            # Wait for indexes
            if constraints_data or indexes_data:
                print("  Waiting for indexes to come online …")
                for _ in range(120):
                    result = session.run(
                        "SHOW INDEXES YIELD state "
                        "WHERE state <> 'ONLINE' RETURN count(*) AS pending"
                    )
                    if result.single()["pending"] == 0:
                        break
                    time.sleep(1)
                else:
                    print("  Warning: some indexes may not be online yet")

            # ── Nodes ────────────────────────────────────────────────
            print(f"\nImporting {len(nodes)} nodes …")

            # Group by (labels, merge_keys) for efficient batching
            node_groups = defaultdict(list)
            for node in nodes:
                key = (tuple(node["labels"]), tuple(node["merge_keys"]))
                props = {k: _deserialize(v) for k, v in node["properties"].items()
                         if v is not None}
                node_groups[key].append(props)

            imported = 0
            for (lbls, mkeys), group in node_groups.items():
                label_clause = _labels(lbls)
                merge_clause = ", ".join(
                    f"{_esc(k)}: item.{_esc(k)}" for k in mkeys
                )
                cypher = (
                    f"UNWIND $batch AS item "
                    f"MERGE (n:{label_clause} {{{merge_clause}}}) "
                    f"SET n += item"
                )
                for i in range(0, len(group), batch_size):
                    batch = group[i:i + batch_size]
                    session.run(cypher, {"batch": batch})
                    imported += len(batch)
                    print(f"  {imported}/{len(nodes)} nodes", end="\r")

            print(f"  {imported}/{len(nodes)} nodes")

            # ── Relationships ────────────────────────────────────────
            if rels:
                print(f"\nImporting {len(rels)} relationships …")

                # Group by (start_labels, start_mkeys, end_labels, end_mkeys, rel_type)
                rel_groups = defaultdict(list)
                for rel in rels:
                    sn = nodes[rel["start_node"]]
                    en = nodes[rel["end_node"]]
                    gkey = (
                        tuple(sn["labels"]), tuple(sn["merge_keys"]),
                        tuple(en["labels"]), tuple(en["merge_keys"]),
                        rel["type"],
                    )
                    item = {
                        "s": {mk: _deserialize(sn["properties"].get(mk))
                              for mk in sn["merge_keys"]},
                        "e": {mk: _deserialize(en["properties"].get(mk))
                              for mk in en["merge_keys"]},
                        "p": {k: _deserialize(v)
                              for k, v in rel["properties"].items()
                              if v is not None},
                    }
                    rel_groups[gkey].append(item)

                rel_imported = 0
                for (sl, smk, el, emk, rtype), group in rel_groups.items():
                    s_match = ", ".join(
                        f"{_esc(k)}: item.s.{_esc(k)}" for k in smk
                    )
                    e_match = ", ".join(
                        f"{_esc(k)}: item.e.{_esc(k)}" for k in emk
                    )
                    # Use CREATE (not MERGE) to preserve parallel
                    # relationships between the same node pair.
                    cypher = (
                        f"UNWIND $batch AS item "
                        f"MATCH (a:{_labels(sl)} {{{s_match}}}) "
                        f"MATCH (b:{_labels(el)} {{{e_match}}}) "
                        f"CREATE (a)-[r:{_esc(rtype)}]->(b) "
                        f"SET r += item.p"
                    )
                    for i in range(0, len(group), batch_size):
                        batch = group[i:i + batch_size]
                        session.run(cypher, {"batch": batch})
                        rel_imported += len(batch)
                        print(f"  {rel_imported}/{len(rels)} relationships", end="\r")

                print(f"  {rel_imported}/{len(rels)} relationships")

        print("\nImport complete.")
    finally:
        driver.close()


# ── CLI ──────────────────────────────────────────────────────────────────

def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    """Register uniform database connection arguments to an executable argument namespace."""
    g = parser.add_argument_group("Neo4j Connection (overrides .env)")
    g.add_argument("--uri", help="Neo4j URI")
    g.add_argument("--user", help="Neo4j user")
    g.add_argument("--password", help="Neo4j password")
    g.add_argument("--database", help="Neo4j database name")


def build_parser() -> argparse.ArgumentParser:
    """Build and configure the command-line argument parser for terminal use."""
    parser = argparse.ArgumentParser(
        description="Neo4j label-level backup & restore.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export all JIRA nodes
  uv run neo4j_backup.py export --label JIRA -o jira_backup.json

  # AND mode: nodes must have ALL labels
  uv run neo4j_backup.py export --label JIRA --label Issue -o jira_issues.json

  # OR mode: nodes with ANY of the listed labels
  uv run neo4j_backup.py export --label-or Episodic --label-or Entity -o episodic_or_entity.json

  # Mix AND + OR: must have JIRA AND (Issue OR Epic)
  uv run neo4j_backup.py export --label JIRA --label-or Issue --label-or Epic -o issues_and_epics.json

  # Exclude filter
  uv run neo4j_backup.py export --label JIRA --exclude-label Comment -o jira_no_comments.json

  # Import into a different database, clearing existing data first
  uv run neo4j_backup.py import -i jira_backup.json --clear \\
      --uri neo4j+ssc://other.databases.neo4j.io --password xxx

  # Use .env overrides for different export/import targets:
  #   NEO4J_EXPORT_URI=neo4j+ssc://source.databases.neo4j.io
  #   NEO4J_EXPORT_PASSWORD=...
  #   NEO4J_IMPORT_URI=neo4j+ssc://target.databases.neo4j.io
  #   NEO4J_IMPORT_PASSWORD=...
""",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ── export ──
    exp = sub.add_parser("export", help="Export nodes and internal relationships")
    exp.add_argument("--label", action="append", metavar="LABEL",
                     help="Label to export — AND mode (repeatable, nodes must have ALL)")
    exp.add_argument("--label-or", action="append", metavar="LABEL",
                     help="Label to export — OR mode (repeatable, nodes with ANY match)")
    exp.add_argument("--require-label", action="append", metavar="LABEL",
                     help="Only include nodes that also have this label (repeatable)")
    exp.add_argument("--exclude-label", action="append", metavar="LABEL",
                     help="Exclude nodes that also have this label (repeatable)")
    exp.add_argument("-o", "--output", required=True, metavar="FILE",
                     help="Output JSON file path")
    _add_connection_args(exp)

    # ── import ──
    imp = sub.add_parser("import", help="Import from a backup file")
    imp.add_argument("-i", "--input", required=True, metavar="FILE",
                     help="Input JSON file path")
    imp.add_argument("--clear", action="store_true",
                     help="Delete all nodes with the backup's primary label before importing")
    imp.add_argument("--batch-size", type=int, default=500,
                     help="Nodes per MERGE batch (default: 500)")
    _add_connection_args(imp)

    return parser


def main() -> None:
    """Primary execution trigger mapping CLI calls to application logic branches."""
    args = build_parser().parse_args()
    if args.command == "export":
        do_export(args)
    elif args.command == "import":
        do_import(args)


if __name__ == "__main__":
    main()
