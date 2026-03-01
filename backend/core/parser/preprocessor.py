"""
COBOL Source Preprocessor.

Runs before the ANTLR4 parser. Handles:
  1. Column stripping  — remove sequence area (cols 1-6) and indicator (col 7)
  2. Comment removal   — lines with * or / in col 7 (fixed) or *> inline
  3. Continuation lines — join lines where col 7 is '-'
  4. COPY resolution   — recursively expand COPY statements
  5. REPLACE processing — token-level text substitution (REPLACE ... BY ...)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Regex to detect COPY statement
# Matches: COPY <member> [OF|IN <library>] [REPLACING ==x== BY ==y== ...].
_COPY_RE = re.compile(
    r"""COPY\s+
        (?P<member>[\w$#@-]+)           # copybook member name
        (?:\s+(?:OF|IN)\s+[\w$#@-]+)?  # optional library qualifier
        (?P<replacing>
            \s+REPLACING\s+.*?          # REPLACING clause (greedy to period)
        )?
        \s*\.""",
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

# Regex to detect REPLACE statement (standalone, not inside COPY)
_REPLACE_START_RE = re.compile(r"^\s*REPLACE\s+", re.IGNORECASE)
_REPLACE_END_RE = re.compile(r"\.\s*$")


@dataclass
class ReplacePair:
    """One ==from== BY ==to== pair from a REPLACE or COPY REPLACING clause."""
    from_token: str
    to_token: str


@dataclass
class PreprocessorResult:
    """Output of the preprocessor — clean source ready for ANTLR4."""
    clean_source: str
    source_map: list[tuple[int, str]] = field(default_factory=list)
    # Maps output line numbers → (original_line_num, original_file)
    warnings: list[str] = field(default_factory=list)
    copybook_paths: list[Path] = field(default_factory=list)


class CopyResolver:
    """Resolves COPY statements by searching configured library paths."""

    def __init__(self, library_paths: list[Path]):
        self.library_paths = [Path(p) for p in library_paths]

    def resolve(self, member: str) -> Path | None:
        """Find the copybook file for a given member name."""
        extensions = [".cpy", ".CPY", ".cob", ".COB", ".copy", ""]
        for lib in self.library_paths:
            for ext in extensions:
                candidate = lib / f"{member}{ext}"
                if candidate.exists():
                    return candidate
        logger.warning("Copybook not found: %s (searched %s)", member, self.library_paths)
        return None


class CobolPreprocessor:
    """
    Full COBOL preprocessor pipeline.

    Usage:
        preprocessor = CobolPreprocessor(library_paths=[Path("copybooks/")])
        result = preprocessor.process(Path("program.cbl"))
    """

    def __init__(self, library_paths: list[Path] | None = None, source_format: str = "fixed"):
        """
        Args:
            library_paths: Directories to search for copybooks.
            source_format:  "fixed" (cols 1-72) or "free" (COBOL 2002+).
        """
        self.resolver = CopyResolver(library_paths or [])
        self.source_format = source_format
        self._visited: set[str] = set()   # prevent circular COPY

    def process(self, source: Path | str) -> PreprocessorResult:
        """Process a COBOL source file or string, returning clean source."""
        self._visited = set()
        if isinstance(source, Path):
            raw = source.read_text(encoding="utf-8", errors="replace")
            self._visited.add(str(source.resolve()))
        else:
            raw = source
        lines, source_map = self._strip_columns(raw, "<input>")
        lines, source_map = self._join_continuations(lines, source_map)
        lines, source_map, copybooks = self._expand_copy(lines, source_map)
        lines = self._apply_replace(lines)
        clean = "\n".join(lines)
        return PreprocessorResult(
            clean_source=clean,
            source_map=source_map,
            copybook_paths=copybooks,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Step 1: Column stripping
    # ─────────────────────────────────────────────────────────────────────

    def _strip_columns(
        self, raw: str, filename: str
    ) -> tuple[list[str], list[tuple[int, str]]]:
        """
        Strip sequence area and indicator column from fixed-format source.
        Returns list of content lines and source_map.
        """
        lines: list[str] = []
        source_map: list[tuple[int, str]] = []

        for lineno, raw_line in enumerate(raw.splitlines(), start=1):
            if self.source_format == "free":
                # Free format: strip *> inline comments, keep everything else
                content = raw_line.rstrip()
                content = re.sub(r"\s*\*>.*$", "", content)
                if content.strip():
                    lines.append(content)
                    source_map.append((lineno, filename))
                continue

            # Fixed format: cols are 1-indexed
            if len(raw_line) < 7:
                # Very short line — treat as blank
                continue

            indicator = raw_line[6] if len(raw_line) > 6 else " "

            # Comment line: indicator = * or /
            if indicator in ("*", "/"):
                continue

            # Blank indicator or D (debug line — treat as active)
            if indicator not in (" ", "-", "D", "d", "$"):
                # Unknown indicator — warn but keep
                logger.debug("Unknown indicator '%s' at %s:%d", indicator, filename, lineno)

            # Content area: cols 7-72 (0-indexed: 6-71), strip to 72
            content = raw_line[7:72].rstrip() if len(raw_line) > 7 else ""

            # Identification area (cols 73-80) is always ignored
            if content or indicator == "-":
                lines.append((indicator, content, lineno, filename))

        # Separate pass to handle continuation marker properly
        result_lines: list[str] = []
        result_map: list[tuple[int, str]] = []
        pending = ""
        for indicator, content, lineno, fname in lines:
            if indicator == "-":
                # Continuation: strip leading quote if present, join to previous
                stripped = content.lstrip()
                if stripped and stripped[0] in ('"', "'"):
                    stripped = stripped[1:]
                pending = pending.rstrip() + stripped
            else:
                if pending:
                    result_lines.append(pending)
                    result_map.append((lineno - 1, fname))
                pending = content
        if pending:
            result_lines.append(pending)
            result_map.append((len(lines), filename))

        return result_lines, result_map

    # ─────────────────────────────────────────────────────────────────────
    # Step 2: Continuation line joining (handled above in _strip_columns)
    # ─────────────────────────────────────────────────────────────────────

    def _join_continuations(
        self, lines: list[str], source_map: list[tuple[int, str]]
    ) -> tuple[list[str], list[tuple[int, str]]]:
        """No-op: continuations handled during column stripping."""
        return lines, source_map

    # ─────────────────────────────────────────────────────────────────────
    # Step 3: COPY expansion
    # ─────────────────────────────────────────────────────────────────────

    def _expand_copy(
        self,
        lines: list[str],
        source_map: list[tuple[int, str]],
    ) -> tuple[list[str], list[tuple[int, str]], list[Path]]:
        """Recursively expand all COPY statements."""
        result_lines: list[str] = []
        result_map: list[tuple[int, str]] = []
        copybooks: list[Path] = []

        full_text = "\n".join(lines)

        def replace_copy(match: re.Match) -> str:
            member = match.group("member")
            replacing_clause = match.group("replacing") or ""
            cpy_path = self.resolver.resolve(member)
            if cpy_path is None:
                logger.warning("COPY %s not found — leaving placeholder", member)
                return f"*> COPY {member} NOT FOUND\n"
            abs_path = str(cpy_path.resolve())
            if abs_path in self._visited:
                logger.error("Circular COPY detected: %s", member)
                return f"*> CIRCULAR COPY {member} SKIPPED\n"
            self._visited.add(abs_path)
            copybooks.append(cpy_path)
            cpy_text = cpy_path.read_text(encoding="utf-8", errors="replace")
            # Strip columns from the copybook too
            inner_lines, _ = self._strip_columns(cpy_text, str(cpy_path))
            inner_text = "\n".join(inner_lines)
            # Apply REPLACING pairs
            pairs = self._parse_replacing(replacing_clause)
            for pair in pairs:
                inner_text = inner_text.replace(pair.from_token, pair.to_token)
            # Recurse (nested COPY)
            inner_lines2, inner_map2, nested_cpys = self._expand_copy(
                inner_text.splitlines(),
                [(0, str(cpy_path))] * len(inner_text.splitlines()),
            )
            copybooks.extend(nested_cpys)
            return "\n".join(inner_lines2) + "\n"

        expanded = _COPY_RE.sub(replace_copy, full_text)
        exp_lines = expanded.splitlines()
        return exp_lines, [(0, "<expanded>")] * len(exp_lines), copybooks

    def _parse_replacing(self, clause: str) -> list[ReplacePair]:
        """Parse 'REPLACING ==x== BY ==y== ==a== BY ==b== ...' into pairs."""
        pairs: list[ReplacePair] = []
        # Find all ==token== BY ==replacement== patterns
        pattern = re.compile(r"==\s*(.*?)\s*==\s+BY\s+==\s*(.*?)\s*==", re.IGNORECASE | re.DOTALL)
        for m in pattern.finditer(clause):
            pairs.append(ReplacePair(from_token=m.group(1).strip(), to_token=m.group(2).strip()))
        return pairs

    # ─────────────────────────────────────────────────────────────────────
    # Step 4: REPLACE statement processing
    # ─────────────────────────────────────────────────────────────────────

    def _apply_replace(self, lines: list[str]) -> list[str]:
        """
        Process standalone REPLACE statements:
            REPLACE ==OLD== BY ==NEW== ==OLD2== BY ==NEW2==.
            ... affected source ...
            REPLACE OFF.
        """
        result: list[str] = []
        active_pairs: list[ReplacePair] = []
        collecting_replace = False
        replace_buffer = ""

        for line in lines:
            stripped = line.strip().upper()

            if collecting_replace:
                replace_buffer += " " + line.strip()
                if replace_buffer.rstrip().endswith("."):
                    collecting_replace = False
                    # Check for REPLACE OFF
                    if "REPLACE OFF" in replace_buffer.upper():
                        active_pairs = []
                    else:
                        active_pairs = self._parse_replacing(replace_buffer)
                    replace_buffer = ""
                continue

            if _REPLACE_START_RE.match(stripped):
                collecting_replace = True
                replace_buffer = line.strip()
                if replace_buffer.rstrip().endswith("."):
                    collecting_replace = False
                    if "REPLACE OFF" in replace_buffer.upper():
                        active_pairs = []
                    else:
                        active_pairs = self._parse_replacing(replace_buffer)
                    replace_buffer = ""
                continue

            # Apply active REPLACE pairs to current line
            transformed = line
            for pair in active_pairs:
                transformed = transformed.replace(pair.from_token, pair.to_token)
            result.append(transformed)

        return result
