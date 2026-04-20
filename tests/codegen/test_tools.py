"""Tests for agent_arena.codegen.tools — guarded LLM file operations."""

import pytest

from agent_arena.codegen.tools import (
    _is_protected,
    _resolve_and_contain,
    edit_file_tool,
    list_files_tool,
    process_tool_call,
    read_file_tool,
)


@pytest.fixture
def project(tmp_path):
    """Create a minimal project structure for testing."""
    # Create some files
    (tmp_path / "agent_arena").mkdir()
    (tmp_path / "agent_arena" / "core").mkdir()
    (tmp_path / "agent_arena" / "agents").mkdir()
    (tmp_path / "agent_arena" / "storage").mkdir()
    (tmp_path / "agent_arena" / "codegen").mkdir()
    (tmp_path / "configs").mkdir()

    (tmp_path / "agent_arena" / "core" / "arena.py").write_text("# arena\n")
    (tmp_path / "agent_arena" / "core" / "runner.py").write_text("# runner\n")
    (tmp_path / "agent_arena" / "agents" / "llm_trader.py").write_text(
        'character = "aggressive"\n'
    )
    (tmp_path / "agent_arena" / "storage" / "sqlite.py").write_text(
        "# storage\n"
    )
    (tmp_path / "configs" / "production.yaml").write_text(
        "name: test\nagents:\n  - id: a1\n"
    )
    (tmp_path / "agent_arena" / "cli.py").write_text("# cli\n")
    return tmp_path


# ---------------------------------------------------------------------------
# Path containment
# ---------------------------------------------------------------------------

class TestPathContainment:
    def test_normal_path_allowed(self, project):
        _, err = _resolve_and_contain("configs/production.yaml", str(project))
        assert err is None

    def test_traversal_blocked(self, project):
        _, err = _resolve_and_contain("../../etc/passwd", str(project))
        assert err is not None
        assert "REFUSED" in err

    def test_dot_dot_in_middle_blocked(self, project):
        _, err = _resolve_and_contain(
            "configs/../../../.env", str(project),
        )
        assert err is not None
        assert "REFUSED" in err

    def test_absolute_path_outside_blocked(self, project):
        _, err = _resolve_and_contain("/etc/passwd", str(project))
        assert err is not None
        assert "REFUSED" in err


# ---------------------------------------------------------------------------
# Protected paths
# ---------------------------------------------------------------------------

class TestProtectedPaths:
    def test_core_arena_protected(self, project):
        assert _is_protected("agent_arena/core/arena.py", str(project))

    def test_core_runner_protected(self, project):
        assert _is_protected("agent_arena/core/runner.py", str(project))

    def test_storage_dir_protected(self, project):
        assert _is_protected(
            "agent_arena/storage/sqlite.py", str(project),
        )

    def test_cli_protected(self, project):
        assert _is_protected("agent_arena/cli.py", str(project))

    def test_codegen_dir_protected(self, project):
        assert _is_protected(
            "agent_arena/codegen/something.py", str(project),
        )

    def test_agents_not_protected(self, project):
        assert not _is_protected(
            "agent_arena/agents/llm_trader.py", str(project),
        )

    def test_configs_not_protected(self, project):
        assert not _is_protected("configs/production.yaml", str(project))


# ---------------------------------------------------------------------------
# read_file_tool
# ---------------------------------------------------------------------------

class TestReadFile:
    def test_read_existing_file(self, project):
        result = read_file_tool(
            "configs/production.yaml", project_root=str(project),
        )
        assert "name: test" in result

    def test_read_nonexistent_file(self, project):
        result = read_file_tool("nope.txt", project_root=str(project))
        assert "ERROR" in result

    def test_read_path_traversal_blocked(self, project):
        result = read_file_tool(
            "../../etc/passwd", project_root=str(project),
        )
        assert "REFUSED" in result

    def test_read_large_file_blocked(self, project):
        big = project / "bigfile.txt"
        big.write_text("x" * 200_000)
        result = read_file_tool("bigfile.txt", project_root=str(project))
        assert "REFUSED" in result
        assert "too large" in result


# ---------------------------------------------------------------------------
# edit_file_tool
# ---------------------------------------------------------------------------

class TestEditFile:
    def test_edit_success(self, project):
        result = edit_file_tool(
            "agent_arena/agents/llm_trader.py",
            'character = "aggressive"',
            'character = "cautious"',
            project_root=str(project),
        )
        assert result.startswith("OK")
        content = (
            project / "agent_arena" / "agents" / "llm_trader.py"
        ).read_text()
        assert 'character = "cautious"' in content

    def test_edit_protected_file_refused(self, project):
        result = edit_file_tool(
            "agent_arena/core/arena.py",
            "# arena",
            "# hacked",
            project_root=str(project),
        )
        assert "REFUSED" in result
        # Verify file unchanged
        content = (project / "agent_arena" / "core" / "arena.py").read_text()
        assert content == "# arena\n"

    def test_edit_storage_refused(self, project):
        result = edit_file_tool(
            "agent_arena/storage/sqlite.py",
            "# storage",
            "# hacked",
            project_root=str(project),
        )
        assert "REFUSED" in result

    def test_edit_path_traversal_refused(self, project):
        result = edit_file_tool(
            "../../etc/hosts",
            "localhost",
            "evil",
            project_root=str(project),
        )
        assert "REFUSED" in result

    def test_edit_nonexistent_file(self, project):
        result = edit_file_tool(
            "agent_arena/agents/nope.py",
            "old",
            "new",
            project_root=str(project),
        )
        assert "ERROR" in result

    def test_edit_old_string_not_found(self, project):
        result = edit_file_tool(
            "agent_arena/agents/llm_trader.py",
            "this does not exist",
            "new",
            project_root=str(project),
        )
        assert "ERROR" in result
        assert "not found" in result

    def test_edit_non_unique_old_string_refused(self, project):
        f = project / "agent_arena" / "agents" / "dupe.py"
        f.write_text("foo\nfoo\nbar\n")
        result = edit_file_tool(
            "agent_arena/agents/dupe.py",
            "foo",
            "baz",
            project_root=str(project),
        )
        assert "ERROR" in result
        assert "2 times" in result

    def test_edit_too_many_lines_refused(self, project):
        f = project / "agent_arena" / "agents" / "big.py"
        old_content = "\n".join(f"line{i}" for i in range(30))
        f.write_text(old_content)
        result = edit_file_tool(
            "agent_arena/agents/big.py",
            old_content,
            "replacement",
            project_root=str(project),
        )
        assert "REFUSED" in result
        assert "lines" in result

    def test_edit_large_file_refused(self, project):
        f = project / "agent_arena" / "agents" / "huge.py"
        f.write_text("x" * 200_000)
        result = edit_file_tool(
            "agent_arena/agents/huge.py",
            "x",
            "y",
            project_root=str(project),
        )
        assert "REFUSED" in result
        assert "too large" in result


# ---------------------------------------------------------------------------
# list_files_tool
# ---------------------------------------------------------------------------

class TestListFiles:
    def test_list_files(self, project):
        result = list_files_tool(
            "agent_arena/agents/*.py", project_root=str(project),
        )
        assert "llm_trader.py" in result

    def test_list_no_matches(self, project):
        result = list_files_tool(
            "nope/**/*.xyz", project_root=str(project),
        )
        assert "No files matched" in result

    def test_list_does_not_escape_root(self, project):
        result = list_files_tool(
            "../../*", project_root=str(project),
        )
        # Should return no results (filtered out) or only in-root matches
        if "No files matched" not in result:
            for line in result.strip().split("\n"):
                # All returned paths should be relative, not absolute
                assert not line.startswith("/")


# ---------------------------------------------------------------------------
# process_tool_call dispatcher
# ---------------------------------------------------------------------------

class TestProcessToolCall:
    def test_dispatch_read(self, project):
        result = process_tool_call(
            "read_file",
            {"path": "configs/production.yaml"},
            project_root=str(project),
        )
        assert "name: test" in result

    def test_dispatch_edit(self, project):
        result = process_tool_call(
            "edit_file",
            {
                "path": "agent_arena/agents/llm_trader.py",
                "old_string": 'character = "aggressive"',
                "new_string": 'character = "patient"',
            },
            project_root=str(project),
        )
        assert result.startswith("OK")

    def test_dispatch_list(self, project):
        result = process_tool_call(
            "list_files",
            {"pattern": "configs/*.yaml"},
            project_root=str(project),
        )
        assert "production.yaml" in result

    def test_dispatch_unknown(self, project):
        result = process_tool_call(
            "delete_file",
            {"path": "foo"},
            project_root=str(project),
        )
        assert "ERROR" in result
        assert "unknown tool" in result

    def test_dispatch_missing_key_returns_error(self, project):
        result = process_tool_call(
            "edit_file",
            {"path": "agent_arena/agents/llm_trader.py"},
            project_root=str(project),
        )
        assert "ERROR" in result
        assert "missing required parameter" in result

    def test_dispatch_empty_inputs_returns_error(self, project):
        result = process_tool_call(
            "read_file",
            {},
            project_root=str(project),
        )
        assert "ERROR" in result
        assert "missing required parameter" in result
