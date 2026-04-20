"""Guarded tools exposed to the codegen LLM via Anthropic tool_use.

Three tools: read_file, edit_file, list_files.
All operations are confined to the project root via path resolution.
Writes go through an exclusion list, uniqueness check, and a
max-lines-changed guard.
"""

from __future__ import annotations

import glob as globmod
from pathlib import Path

# ---------------------------------------------------------------------------
# Guardrails
# ---------------------------------------------------------------------------

PROTECTED_PATHS = [
    "core/arena.py",
    "core/runner.py",
    "storage/",
    "api/",
    "scripts/deploy.sh",
    "cli.py",
    "codegen/",
]

MAX_LINES_CHANGED = 20
MAX_READ_BYTES = 100_000  # 100 KB


def _resolve_and_contain(
    path: str, project_root: str,
) -> tuple[Path, str | None]:
    """Resolve *path* and verify it stays inside *project_root*.

    Returns (resolved_path, error_msg).  error_msg is None on success.
    """
    root = Path(project_root).resolve()
    resolved = (root / path).resolve()
    if not resolved.is_relative_to(root):
        return resolved, f"REFUSED: path escapes project root: {path}"
    return resolved, None


def _is_protected(path: str, project_root: str) -> bool:
    """Return True if *path* is protected from edits.

    Normalizes both sides via Path.resolve() so that ``./``, ``../``,
    and redundant separators cannot bypass the check.
    """
    root = Path(project_root).resolve()
    resolved = (root / path).resolve()
    try:
        rel = resolved.relative_to(root)
    except ValueError:
        return True  # outside project root → treat as protected

    for prot in PROTECTED_PATHS:
        if prot.endswith("/"):
            # Directory prefix — check if rel is under agent_arena/<prot>
            dir_path = Path("agent_arena") / prot.rstrip("/")
            if rel.is_relative_to(dir_path):
                return True
        else:
            # Exact file — check both bare and under agent_arena/
            if rel == Path(prot) or rel == Path("agent_arena") / prot:
                return True
    return False


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def read_file_tool(path: str, *, project_root: str = ".") -> str:
    """Read a file and return its contents."""
    full, err = _resolve_and_contain(path, project_root)
    if err:
        return err
    if not full.exists():
        return f"ERROR: file not found: {path}"
    if full.stat().st_size > MAX_READ_BYTES:
        return (
            f"REFUSED: {path} is too large "
            f"({full.stat().st_size:,} bytes, max {MAX_READ_BYTES:,})."
        )
    try:
        return full.read_text()
    except Exception as exc:
        return f"ERROR reading {path}: {exc}"


def edit_file_tool(
    path: str,
    old_string: str,
    new_string: str,
    *,
    project_root: str = ".",
) -> str:
    """Replace *old_string* with *new_string* in *path*.

    Returns a status message.  Refuses if:
    - path escapes the project root
    - path matches the exclusion list
    - old_string appears more than once (ambiguous edit)
    - the edit changes more than MAX_LINES_CHANGED lines
    - old_string is not found in the file
    """
    full, err = _resolve_and_contain(path, project_root)
    if err:
        return err
    if _is_protected(path, project_root):
        return f"REFUSED: {path} is a protected file."
    if not full.exists():
        return f"ERROR: file not found: {path}"

    if full.stat().st_size > MAX_READ_BYTES:
        return (
            f"REFUSED: {path} is too large to edit "
            f"({full.stat().st_size:,} bytes, max {MAX_READ_BYTES:,})."
        )

    content = full.read_text()
    count = content.count(old_string)
    if count == 0:
        return f"ERROR: old_string not found in {path}."
    if count > 1:
        return (
            f"ERROR: old_string appears {count} times in {path}. "
            "Provide more surrounding context to make it unique."
        )

    # Line-change guard — counts newlines as a rough proxy for changed
    # lines.  A 20-line block replaced with a 20-line block counts as
    # "20 lines" even if only one character differs.
    old_lines = old_string.count("\n")
    new_lines = new_string.count("\n")
    changed = max(old_lines, new_lines)
    if changed > MAX_LINES_CHANGED:
        return (
            f"REFUSED: edit touches {changed} lines "
            f"(max {MAX_LINES_CHANGED})."
        )

    updated = content.replace(old_string, new_string, 1)
    full.write_text(updated)
    return f"OK: edited {path} ({changed} lines changed)."


def list_files_tool(pattern: str, *, project_root: str = ".") -> str:
    """Glob for files matching *pattern* relative to project root."""
    root = Path(project_root).resolve()
    matches = sorted(globmod.glob(str(root / pattern), recursive=True))
    # Filter to only paths inside the project root
    result = []
    for m in matches[:50]:  # cap output size
        p = Path(m).resolve()
        if not p.is_relative_to(root):
            continue
        try:
            result.append(str(p.relative_to(root)))
        except ValueError:
            continue
    if not result:
        return f"No files matched pattern: {pattern}"
    return "\n".join(result)


# ---------------------------------------------------------------------------
# Anthropic tool schemas
# ---------------------------------------------------------------------------

TOOL_SCHEMAS = [
    {
        "name": "read_file",
        "description": (
            "Read a file from the project. "
            "Path is relative to the project root."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Relative file path to read.",
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "edit_file",
        "description": (
            "Replace an exact string in a file with a new string. "
            "Path is relative to the project root. "
            "The old_string must appear exactly once in the file. "
            "Protected files (core/arena.py, storage/, api/, etc.) "
            "will be refused. Max 20 lines changed per edit. "
            "If a finding has target '_escalate', do NOT attempt "
            "edits — it will be escalated to a GitHub issue instead."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Relative file path to edit.",
                },
                "old_string": {
                    "type": "string",
                    "description": (
                        "Exact string to find and replace. "
                        "Must appear exactly once in the file."
                    ),
                },
                "new_string": {
                    "type": "string",
                    "description": "Replacement string.",
                },
            },
            "required": ["path", "old_string", "new_string"],
        },
    },
    {
        "name": "list_files",
        "description": (
            "List files matching a glob pattern relative to the "
            "project root (e.g. 'agent_arena/agents/*.py')."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Glob pattern (supports **).",
                },
            },
            "required": ["pattern"],
        },
    },
]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def process_tool_call(
    name: str,
    inputs: dict,
    *,
    project_root: str = ".",
) -> str:
    """Dispatch a tool call and return the result string."""
    try:
        if name == "read_file":
            return read_file_tool(
                inputs["path"], project_root=project_root,
            )
        elif name == "edit_file":
            return edit_file_tool(
                inputs["path"],
                inputs["old_string"],
                inputs["new_string"],
                project_root=project_root,
            )
        elif name == "list_files":
            return list_files_tool(
                inputs["pattern"], project_root=project_root,
            )
        else:
            return f"ERROR: unknown tool '{name}'"
    except KeyError as exc:
        return f"ERROR: missing required parameter {exc} for tool '{name}'"
