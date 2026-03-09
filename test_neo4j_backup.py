#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "neo4j",
#     "pytest",
# ]
# ///
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
import neo4j.time as nt
import neo4j.spatial as ns

from neo4j_backup import (
    _serialize,
    _deserialize,
    _esc,
    _labels,
    _if_not_exists,
    _build_label_clause,
    load_env_file,
    _is_transient,
    RetrySession
)


# ── Cypher helpers tests ──────────────────────────────────────────────────

def test_esc():
    assert _esc("SimpleLabel") == "`SimpleLabel`"
    assert _esc("Label`With`Ticks") == "`Label``With``Ticks`"


def test_labels():
    assert _labels(["User", "Active"]) == "`User`:`Active`"
    assert _labels(["JIRA_Issue"]) == "`JIRA_Issue`"


def test_if_not_exists():
    # Constraints
    stmt = "CREATE CONSTRAINT `constraint_name` FOR (n:Label) REQUIRE n.id IS UNIQUE"
    expected = "CREATE CONSTRAINT `constraint_name` IF NOT EXISTS FOR (n:Label) REQUIRE n.id IS UNIQUE"
    assert _if_not_exists(stmt) == expected

    # Indexes
    stmt2 = "CREATE INDEX `index_name` FOR (n:Label) ON (n.name)"
    expected2 = "CREATE INDEX `index_name` IF NOT EXISTS FOR (n:Label) ON (n.name)"
    assert _if_not_exists(stmt2) == expected2

    # Should not touch if already there
    assert _if_not_exists(expected) == expected


def test_build_label_clause():
    # Only AND
    match, where, display = _build_label_clause(["JIRA", "Issue"], [])
    assert match == "`JIRA`:`Issue`"
    assert where == []
    assert display == "JIRA:Issue"

    # Only OR
    match, where, display = _build_label_clause([], ["Epic", "Task"])
    assert match == ""
    assert where == ["(n:`Epic` OR n:`Task`)"]
    assert display == "(Epic | Task)"

    # Mixed
    match, where, display = _build_label_clause(["JIRA"], ["Bug", "Task"])
    assert match == "`JIRA`"
    assert where == ["(n:`Bug` OR n:`Task`)"]
    assert display == "JIRA + (Bug | Task)"

    # Empty
    with pytest.raises(ValueError):
        _build_label_clause([], [])


# ── Property Serialization tests ─────────────────────────────────────────

def test_serialize_primitives():
    assert _serialize(123) == 123
    assert _serialize(45.6) == 45.6
    assert _serialize("test") == "test"
    assert _serialize(True) is True
    assert _serialize(None) is None

def test_serialize_collections():
    assert _serialize([1, "two", False]) == [1, "two", False]

def test_serialize_bytes():
    b = b"hello world"
    res = _serialize(b)
    assert isinstance(res, dict)
    assert res["__neo4j__"] == "bytes"
    # base64 of "hello world" is "aGVsbG8gd29ybGQ="
    assert res["v"] == "aGVsbG8gd29ybGQ="

def test_deserialize_primitives():
    assert _deserialize(123) == 123
    assert _deserialize("test") == "test"
    assert _deserialize([1, 2]) == [1, 2]

def test_deserialize_bytes():
    serialized = {"__neo4j__": "bytes", "v": "aGVsbG8gd29ybGQ="}
    assert _deserialize(serialized) == b"hello world"

def test_deserialize_spatial():
    # CartesianPoint
    serialized_cartesian = {
        "__neo4j__": "CartesianPoint", 
        "v": "CartesianPoint(1.2, 3.4)"
    }
    pt1 = _deserialize(serialized_cartesian)
    assert isinstance(pt1, ns.CartesianPoint)
    assert pt1.x == 1.2
    assert pt1.y == 3.4

    # WGS84Point
    serialized_wgs84 = {
        "__neo4j__": "WGS84Point", 
        "v": "WGS84Point(10.0, 20.0, 30.0)"
    }
    pt2 = _deserialize(serialized_wgs84)
    assert isinstance(pt2, ns.WGS84Point)
    assert pt2.longitude == 10.0
    assert pt2.latitude == 20.0
    assert pt2.height == 30.0

def test_unknown_dict_deserialization():
    unknown = {"foo": "bar"}
    # Should fallback to JSON string representation
    assert _deserialize(unknown) == '{"foo": "bar"}'


# ── .env Loader tests ────────────────────────────────────────────────────

def test_load_env_file():
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write("# Comment\n")
        f.write("TEST_VAR_1=value1\n")
        f.write("TEST_VAR_2 = value2 \n")
        env_path = f.name

    try:
        with patch.dict(os.environ, clear=True):
            # Normal load (ignores pre-existing if not overriden, but here environment is clear)
            load_env_file(env_path)
            assert os.environ.get("TEST_VAR_1") == "value1"
            assert os.environ.get("TEST_VAR_2") == "value2"

            # Test no override
            os.environ["TEST_VAR_1"] = "existing_value"
            load_env_file(env_path, override=False)
            assert os.environ.get("TEST_VAR_1") == "existing_value"  # Not overridden

            # Test override
            load_env_file(env_path, override=True)
            assert os.environ.get("TEST_VAR_1") == "value1"  # Overridden!
    finally:
        os.remove(env_path)


# ── RetrySession tests ───────────────────────────────────────────────────

def test_is_transient():
    assert _is_transient(Exception("ServiceUnavailable! Please retry.")) is True
    assert _is_transient(Exception("Session expired.")) is True
    assert _is_transient(Exception("SyntaxError: Invalid Cypher")) is False

@patch("neo4j_backup.time.sleep") # Prevent actual sleep in tests
def test_retry_session_success_on_retry(mock_sleep):
    mock_driver = MagicMock()
    mock_session = MagicMock()
    mock_driver.session.return_value = mock_session
    
    # Setup mock to fail transiently once, then succeed
    mock_session.run.side_effect = [
        Exception("ServiceUnavailable"), 
        "Success result"
    ]
    
    with RetrySession(mock_driver, "neo4j") as session:
        result = session.run("MATCH (n) RETURN n")
        
    assert result == "Success result"
    assert mock_session.run.call_count == 2
    mock_sleep.assert_called_once()  # Slept before retry

@patch("neo4j_backup.time.sleep")
def test_retry_session_fails_on_non_transient(mock_sleep):
    mock_driver = MagicMock()
    mock_session = MagicMock()
    mock_driver.session.return_value = mock_session
    
    # Fails with a non-transient error
    mock_session.run.side_effect = Exception("Syntax error")
    
    with RetrySession(mock_driver, "neo4j") as session:
        with pytest.raises(Exception, match="Syntax error"):
            session.run("MATCH (n)")
            
    assert mock_session.run.call_count == 1
    mock_sleep.assert_not_called()
