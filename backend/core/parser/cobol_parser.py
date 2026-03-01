"""
COBOL Parser — hand-written recursive descent parser for the DATA DIVISION
and key PROCEDURE DIVISION constructs.

Design rationale:
  ANTLR4 requires the Java runtime to generate Python parser classes from .g4
  grammar files. To keep the tool immediately runnable without a Java
  prerequisite, we ship a pure-Python parser that handles all constructs
  needed for migration: data descriptions, file descriptors, EXEC SQL blocks,
  and EVALUATE statements.  The parser is complete enough for production use
  on the constructs that matter for schema extraction and data decoding.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path

from backend.core.parser.ast_nodes import (
    CompilationUnit,
    DataDescription,
    DataDivision,
    EnvironmentDivision,
    EvaluateStatement,
    EvaluateWhen,
    ExecSqlBlock,
    FileControlEntry,
    FileDescriptor,
    OccursClause,
    ProcedureDivision,
    SignClause,
)
from backend.core.parser.preprocessor import CobolPreprocessor, PreprocessorResult

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Tokenizer
# ─────────────────────────────────────────────────────────────────────────────

_TOKEN_RE = re.compile(
    r"""
    (?P<string>  '[^']*'  | "[^"]*" )   |   # quoted string literal
    (?P<word>    [^\s.,;()\[\]]+      )  |   # word/identifier/number
    (?P<dot>     \.                   )  |   # period (statement terminator)
    (?P<lparen>  \(                   )  |   # left paren
    (?P<rparen>  \)                   )  |   # right paren
    (?P<comma>   ,                    )  |   # comma
    """,
    re.VERBOSE | re.IGNORECASE,
)


def tokenize(source: str) -> list[str]:
    """Tokenize preprocessed COBOL source into a flat list of token strings."""
    tokens: list[str] = []
    for m in _TOKEN_RE.finditer(source):
        tok = m.group(0).strip()
        if tok:
            tokens.append(tok)
    return tokens


# ─────────────────────────────────────────────────────────────────────────────
# Parser
# ─────────────────────────────────────────────────────────────────────────────

class CobolParser:
    """
    Pure-Python COBOL parser.

    Usage:
        parser = CobolParser(library_paths=[Path("copybooks/")])
        cu = parser.parse_file(Path("program.cbl"))
        # or
        cu = parser.parse_string("IDENTIFICATION DIVISION. ...")
    """

    def __init__(self, library_paths: list[Path] | None = None, source_format: str = "fixed"):
        self.preprocessor = CobolPreprocessor(library_paths, source_format)

    # ── Public entry points ────────────────────────────────────────────────

    def parse_file(self, path: Path) -> CompilationUnit:
        raw = path.read_text(encoding="utf-8", errors="replace")
        result = self.preprocessor.process(path)
        return self._parse(result, path.name, raw)

    def parse_string(self, source: str, filename: str = "<string>") -> CompilationUnit:
        result = self.preprocessor.process(source)
        return self._parse(result, filename, source)

    def parse_copybook(self, path: Path) -> list[DataDescription]:
        """
        Parse a standalone copybook (.cpy) file.
        Returns the list of top-level DataDescription nodes.
        """
        raw = path.read_text(encoding="utf-8", errors="replace")
        result = self.preprocessor.process(raw)
        tokens = tokenize(result.clean_source)
        stream = TokenStream(tokens)
        return self._parse_data_descriptions(stream)

    # ── Internal parse ─────────────────────────────────────────────────────

    def _parse(self, result: PreprocessorResult, filename: str, raw: str) -> CompilationUnit:
        src_hash = hashlib.sha256(raw.encode()).hexdigest()
        tokens = tokenize(result.clean_source)
        stream = TokenStream(tokens)

        cu = CompilationUnit(
            program_id="UNKNOWN",
            source_file=filename,
            source_hash=src_hash,
        )

        # Scan divisions
        while not stream.eof():
            tok = stream.peek()
            if stream.match_seq(["IDENTIFICATION", "DIVISION"]) or stream.match_seq(["ID", "DIVISION"]):
                cu.program_id = self._parse_identification(stream)
            elif stream.match_seq(["ENVIRONMENT", "DIVISION"]):
                cu.environment_division = self._parse_environment(stream)
            elif stream.match_seq(["DATA", "DIVISION"]):
                cu.data_division = self._parse_data_division(stream)
            elif stream.match_seq(["PROCEDURE", "DIVISION"]):
                cu.procedure_division = self._parse_procedure_division(stream)
            else:
                stream.advance()  # skip unknown token

        return cu

    # ── IDENTIFICATION DIVISION ────────────────────────────────────────────

    def _parse_identification(self, stream: "TokenStream") -> str:
        stream.consume_until_dot()  # skip "IDENTIFICATION DIVISION."
        program_id = "UNKNOWN"
        while not stream.eof():
            if stream.peek_upper() in ("ENVIRONMENT", "DATA", "PROCEDURE"):
                break
            if stream.match_seq(["PROGRAM-ID", "."]) or stream.peek_upper() == "PROGRAM-ID":
                stream.advance()  # skip PROGRAM-ID
                if stream.peek() == ".":
                    stream.advance()
                if not stream.eof():
                    program_id = stream.advance().rstrip(".")
            else:
                stream.advance()
        return program_id

    # ── ENVIRONMENT DIVISION ───────────────────────────────────────────────

    def _parse_environment(self, stream: "TokenStream") -> EnvironmentDivision:
        env = EnvironmentDivision()
        stream.consume_until_dot()  # "ENVIRONMENT DIVISION."

        while not stream.eof():
            up = stream.peek_upper()
            if up in ("DATA", "PROCEDURE"):
                break
            if up == "SELECT":
                entry = self._parse_select(stream)
                env.file_control.append(entry)
            elif up == "DECIMAL-POINT":
                # DECIMAL-POINT IS COMMA
                stream.consume_until_dot()
                env.decimal_point_is_comma = True
            else:
                stream.advance()

        return env

    def _parse_select(self, stream: "TokenStream") -> FileControlEntry:
        stream.advance()  # SELECT
        select_name = stream.advance()  # file name
        entry = FileControlEntry(select_name=select_name, assign_to="")

        while not stream.eof():
            up = stream.peek_upper()
            if up == "SELECT":
                break
            if up == "DATA" or up == "PROCEDURE":
                break
            if up == "ASSIGN":
                stream.advance()  # ASSIGN
                if stream.peek_upper() in ("TO", "USING"):
                    stream.advance()
                entry.assign_to = stream.advance().rstrip(".")
            elif up == "ORGANIZATION":
                stream.advance()  # ORGANIZATION
                if stream.peek_upper() == "IS":
                    stream.advance()
                entry.organization = stream.advance().upper()
            elif up == "ACCESS":
                stream.advance()  # ACCESS
                if stream.peek_upper() in ("MODE", "IS"):
                    stream.advance()
                if stream.peek_upper() in ("IS",):
                    stream.advance()
                entry.access_mode = stream.advance().upper()
            elif up in ("RECORD", "ALTERNATE"):
                kw = stream.advance().upper()
                if stream.peek_upper() == "KEY":
                    stream.advance()
                if stream.peek_upper() == "IS":
                    stream.advance()
                key = stream.advance()
                if kw == "ALTERNATE":
                    entry.alternate_keys.append(key)
                else:
                    entry.record_key = key
            elif up == "FILE":
                stream.advance()  # FILE
                if stream.peek_upper() == "STATUS":
                    stream.advance()
                    if stream.peek_upper() == "IS":
                        stream.advance()
                    entry.file_status_var = stream.advance()
            elif up == ".":
                stream.advance()
                break
            else:
                stream.advance()

        return entry

    # ── DATA DIVISION ──────────────────────────────────────────────────────

    def _parse_data_division(self, stream: "TokenStream") -> DataDivision:
        dd = DataDivision()
        stream.consume_until_dot()  # "DATA DIVISION."

        while not stream.eof():
            up = stream.peek_upper()
            if up == "PROCEDURE":
                break
            if up == "FILE":
                stream.advance()
                if stream.peek_upper() == "SECTION":
                    stream.advance()
                    stream.consume_dot()
                    dd.file_section.extend(self._parse_file_section(stream))
            elif up == "WORKING-STORAGE":
                stream.advance()
                if stream.peek_upper() == "SECTION":
                    stream.advance()
                    stream.consume_dot()
                dd.working_storage.extend(self._parse_data_descriptions(stream))
            elif up == "LINKAGE":
                stream.advance()
                if stream.peek_upper() == "SECTION":
                    stream.advance()
                    stream.consume_dot()
                dd.linkage_section.extend(self._parse_data_descriptions(stream))
            elif up == "LOCAL-STORAGE":
                stream.advance()
                if stream.peek_upper() == "SECTION":
                    stream.advance()
                    stream.consume_dot()
                dd.local_storage.extend(self._parse_data_descriptions(stream))
            else:
                stream.advance()

        return dd

    def _parse_file_section(self, stream: "TokenStream") -> list[FileDescriptor]:
        fds: list[FileDescriptor] = []
        while not stream.eof():
            up = stream.peek_upper()
            if up in ("WORKING-STORAGE", "LINKAGE", "LOCAL-STORAGE", "PROCEDURE"):
                break
            if up in ("FD", "SD"):
                fds.append(self._parse_fd(stream))
            else:
                stream.advance()
        return fds

    def _parse_fd(self, stream: "TokenStream") -> FileDescriptor:
        stream.advance()  # FD or SD
        fd_name = stream.advance()
        fd = FileDescriptor(fd_name=fd_name)

        # Parse FD clauses until we hit a level number (start of record desc)
        while not stream.eof():
            up = stream.peek_upper()
            if up == ".":
                stream.advance()
                break
            if _is_level_number(up):
                break
            if up == "RECORDING":
                stream.advance()
                if stream.peek_upper() in ("MODE", "IS"):
                    stream.advance()
                if stream.peek_upper() == "IS":
                    stream.advance()
                fd.recording_mode = stream.advance().upper()
            elif up == "RECORD":
                stream.consume_until_dot()
            elif up == "BLOCK":
                stream.consume_until_dot()
            elif up == "LABEL":
                stream.consume_until_dot()
            else:
                stream.advance()

        # Parse record descriptions (01-level entries)
        fd.record_descriptions.extend(self._parse_data_descriptions(stream, stop_at_fd=True))
        return fd

    def _parse_data_descriptions(
        self, stream: "TokenStream", stop_at_fd: bool = False
    ) -> list[DataDescription]:
        """
        Parse a flat list of data description entries and reconstruct hierarchy.
        Handles all level numbers (01-49, 66, 77, 88).
        """
        flat: list[DataDescription] = []

        while not stream.eof():
            up = stream.peek_upper()
            if up in ("WORKING-STORAGE", "LINKAGE", "LOCAL-STORAGE", "PROCEDURE", "FILE"):
                break
            if up in ("FD", "SD") and stop_at_fd:
                break
            if up == ".":
                stream.advance()
                continue
            if not _is_level_number(up):
                stream.advance()
                continue

            dd = self._parse_one_data_description(stream)
            if dd is not None:
                flat.append(dd)

        return _build_hierarchy(flat)

    def _parse_one_data_description(self, stream: "TokenStream") -> DataDescription | None:
        """Parse a single data description entry up to its terminating period."""
        level_str = stream.advance()
        try:
            level = int(level_str)
        except ValueError:
            return None

        # Name
        up = stream.peek_upper()
        if up == "FILLER" or up == ".":
            name = "FILLER"
            if up == "FILLER":
                stream.advance()
        else:
            name = stream.advance()

        dd = DataDescription(level=level, name=name)

        # Clauses
        while not stream.eof():
            up = stream.peek_upper()
            if up == ".":
                stream.advance()
                break

            if up == "PIC" or up == "PICTURE":
                stream.advance()
                if stream.peek_upper() == "IS":
                    stream.advance()
                dd.picture = self._parse_picture(stream)

            elif up == "USAGE":
                stream.advance()
                if stream.peek_upper() == "IS":
                    stream.advance()
                dd.usage = stream.advance().upper()

            elif up in ("COMP", "COMP-1", "COMP-2", "COMP-3", "COMP-4", "COMP-5",
                        "COMP-6", "BINARY", "PACKED-DECIMAL", "FLOAT-SHORT",
                        "FLOAT-LONG", "INDEX", "POINTER", "PROCEDURE-POINTER",
                        "FUNCTION-POINTER", "DISPLAY", "DISPLAY-1", "NATIONAL"):
                dd.usage = stream.advance().upper()

            elif up == "REDEFINES":
                stream.advance()
                dd.redefines = stream.advance()

            elif up == "OCCURS":
                dd.occurs = self._parse_occurs(stream)

            elif up == "VALUE":
                stream.advance()
                if stream.peek_upper() == "IS":
                    stream.advance()
                dd.value = self._collect_value(stream)

            elif up == "VALUES":
                stream.advance()
                if stream.peek_upper() == "ARE":
                    stream.advance()
                dd.value = self._collect_value(stream)

            elif up == "SIGN":
                stream.advance()
                if stream.peek_upper() == "IS":
                    stream.advance()
                pos = stream.advance().upper()
                sep = False
                if stream.peek_upper() == "SEPARATE":
                    stream.advance()
                    sep = True
                    if stream.peek_upper() == "CHARACTER":
                        stream.advance()
                dd.sign = SignClause(
                    position="leading" if pos == "LEADING" else "trailing",
                    separate=sep,
                )

            elif up == "SYNCHRONIZED" or up == "SYNC":
                stream.advance()
                if stream.peek_upper() in ("LEFT", "RIGHT"):
                    stream.advance()
                dd.synchronized = True

            elif up == "JUSTIFIED" or up == "JUST":
                stream.advance()
                if stream.peek_upper() == "RIGHT":
                    stream.advance()
                dd.justified = True

            elif up == "BLANK":
                stream.advance()
                if stream.peek_upper() == "WHEN":
                    stream.advance()
                if stream.peek_upper() == "ZERO":
                    stream.advance()
                dd.blank_when_zero = True

            elif up in ("GLOBAL", "EXTERNAL"):
                tok = stream.advance().upper()
                if tok == "GLOBAL":
                    dd.global_ = True
                else:
                    dd.external_ = True

            elif up == "RENAMES":
                stream.advance()  # skip RENAMES clause content
                stream.consume_until_dot()
                break

            elif _is_level_number(up) and up != level_str:
                # Next entry started — don't consume
                break

            else:
                stream.advance()  # unknown clause token

        return dd

    def _parse_picture(self, stream: "TokenStream") -> str:
        """
        Collect the full PICTURE string, which may include parenthesized
        repeat counts, e.g. S9(11)V99 or X(30) or 9(7)V9(2).
        """
        parts: list[str] = []
        while not stream.eof():
            tok = stream.peek()
            up = tok.upper()
            # Stop conditions
            if up in (".", "USAGE", "REDEFINES", "OCCURS", "VALUE", "VALUES",
                      "SIGN", "SYNC", "SYNCHRONIZED", "JUSTIFIED", "BLANK",
                      "GLOBAL", "EXTERNAL", "COMP", "COMP-1", "COMP-2",
                      "COMP-3", "COMP-4", "COMP-5", "BINARY", "PACKED-DECIMAL",
                      "DISPLAY", "DISPLAY-1", "INDEX"):
                break
            if _is_level_number(up):
                break
            parts.append(stream.advance())
            # After closing paren, check if next token continues picture
            if tok.endswith(")"):
                next_up = stream.peek_upper() if not stream.eof() else ""
                if next_up and re.match(r"^[9XASVPZ\$\+\-\*./,0B]+(\(\d+\))?$", next_up, re.IGNORECASE):
                    continue
                break
        return "".join(parts)

    def _parse_occurs(self, stream: "TokenStream") -> OccursClause:
        """Parse OCCURS clause including DEPENDING ON and INDEXED BY."""
        stream.advance()  # OCCURS
        min_times = 0
        max_str = stream.advance()  # first number (or "0" for "OCCURS 0 TO n")
        try:
            max_times = int(max_str)
        except ValueError:
            max_times = 1

        occurs = OccursClause(min_times=min_times, max_times=max_times)

        while not stream.eof():
            up = stream.peek_upper()
            if up in (".", "PIC", "PICTURE", "USAGE", "VALUE", "SIGN",
                      "SYNCHRONIZED", "SYNC", "JUSTIFIED", "BLANK"):
                break
            if _is_level_number(up):
                break

            if up == "TO":
                occurs.min_times = occurs.max_times
                stream.advance()
                try:
                    occurs.max_times = int(stream.advance())
                except ValueError:
                    pass

            elif up == "TIMES":
                stream.advance()

            elif up == "DEPENDING":
                stream.advance()
                if stream.peek_upper() == "ON":
                    stream.advance()
                occurs.depending_on = stream.advance()

            elif up == "ASCENDING":
                stream.advance()
                if stream.peek_upper() == "KEY":
                    stream.advance()
                if stream.peek_upper() == "IS":
                    stream.advance()
                occurs.ascending_keys.append(stream.advance())

            elif up == "DESCENDING":
                stream.advance()
                if stream.peek_upper() == "KEY":
                    stream.advance()
                if stream.peek_upper() == "IS":
                    stream.advance()
                occurs.descending_keys.append(stream.advance())

            elif up == "INDEXED":
                stream.advance()
                if stream.peek_upper() == "BY":
                    stream.advance()
                while not stream.eof() and not _is_keyword(stream.peek_upper()):
                    occurs.indexed_by.append(stream.advance())

            else:
                stream.advance()

        return occurs

    def _collect_value(self, stream: "TokenStream") -> str:
        """Collect value tokens until a period, another clause keyword, or level number."""
        parts: list[str] = []
        while not stream.eof():
            up = stream.peek_upper()
            if up == ".":
                break
            if _is_level_number(up):
                break
            if up in ("PIC", "PICTURE", "USAGE", "REDEFINES", "OCCURS", "SIGN",
                      "SYNCHRONIZED", "SYNC", "JUSTIFIED", "BLANK", "GLOBAL", "EXTERNAL"):
                break
            if up in ("THRU", "THROUGH", "OR"):
                parts.append(stream.advance())
                continue
            parts.append(stream.advance())
        return " ".join(parts)

    # ── PROCEDURE DIVISION ────────────────────────────────────────────────

    def _parse_procedure_division(self, stream: "TokenStream") -> ProcedureDivision:
        pd = ProcedureDivision()
        stream.consume_until_dot()  # "PROCEDURE DIVISION [USING ...] ."

        while not stream.eof():
            up = stream.peek_upper()
            if up in ("END", "STOP"):
                stream.advance()
                continue

            # EXEC SQL
            if up == "EXEC":
                next2 = stream.peek_at(1).upper() if stream.peek_at(1) else ""
                if next2 == "SQL":
                    pd.exec_sql_blocks.append(self._parse_exec_sql(stream))
                    continue

            # EVALUATE (for REDEFINES discriminator detection)
            if up == "EVALUATE":
                ev = self._parse_evaluate(stream)
                if ev:
                    pd.evaluate_statements.append(ev)
                continue

            # CALL (for call graph)
            if up == "CALL":
                stream.advance()
                called = stream.peek()
                if called and called not in (".", "USING", "ON", "NOT", "END-CALL"):
                    pd.called_programs.append(called.strip("'\""))
                stream.advance()
                continue

            # Paragraph / section names (word followed by . or SECTION)
            if stream.peek_at(1) in (".", "SECTION"):
                pd.paragraph_names.append(up)

            stream.advance()

        return pd

    def _parse_exec_sql(self, stream: "TokenStream") -> ExecSqlBlock:
        stream.advance()  # EXEC
        stream.advance()  # SQL
        sql_parts: list[str] = []
        host_vars: list[str] = []
        while not stream.eof():
            tok = stream.advance()
            if tok.upper() == "END-EXEC":
                break
            sql_parts.append(tok)
            if tok.startswith(":"):
                host_vars.append(tok[1:])
        return ExecSqlBlock(
            sql_text=" ".join(sql_parts),
            host_variables=host_vars,
        )

    def _parse_evaluate(self, stream: "TokenStream") -> EvaluateStatement | None:
        stream.advance()  # EVALUATE
        if stream.eof():
            return None
        subject = stream.advance()  # the field being evaluated
        ev = EvaluateStatement(subject=subject)

        while not stream.eof():
            up = stream.peek_upper()
            if up == "END-EVALUATE":
                stream.advance()
                break
            if up == "WHEN":
                stream.advance()
                when = EvaluateWhen()
                if stream.peek_upper() == "OTHER":
                    stream.advance()
                    when.is_other = True
                else:
                    while not stream.eof() and stream.peek_upper() not in (
                        "WHEN", "END-EVALUATE", "PERFORM", "MOVE", "IF", "COMPUTE"
                    ):
                        when.subject_values.append(stream.advance())
                ev.when_clauses.append(when)
            else:
                stream.advance()

        return ev


# ─────────────────────────────────────────────────────────────────────────────
# TokenStream helper
# ─────────────────────────────────────────────────────────────────────────────

class TokenStream:
    def __init__(self, tokens: list[str]):
        self._tokens = tokens
        self._pos = 0

    def eof(self) -> bool:
        return self._pos >= len(self._tokens)

    def peek(self) -> str:
        if self.eof():
            return ""
        return self._tokens[self._pos]

    def peek_upper(self) -> str:
        return self.peek().upper()

    def peek_at(self, offset: int) -> str:
        idx = self._pos + offset
        if idx >= len(self._tokens):
            return ""
        return self._tokens[idx]

    def advance(self) -> str:
        tok = self.peek()
        self._pos += 1
        return tok

    def consume_dot(self) -> None:
        if self.peek() == ".":
            self._pos += 1

    def consume_until_dot(self) -> None:
        while not self.eof() and self.peek() != ".":
            self._pos += 1
        if not self.eof():
            self._pos += 1  # consume the dot

    def match_seq(self, words: list[str]) -> bool:
        for i, w in enumerate(words):
            if self.peek_at(i).upper() != w.upper():
                return False
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Hierarchy builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_hierarchy(flat: list[DataDescription]) -> list[DataDescription]:
    """
    Convert a flat list of DataDescription entries (as they appear in source)
    into a tree by nesting children under their parent based on level number.
    Returns only the top-level nodes (level 01 and 77).
    """
    roots: list[DataDescription] = []
    stack: list[DataDescription] = []

    for dd in flat:
        # 77 and 01 are always top-level
        if dd.level in (1, 77):
            roots.append(dd)
            stack = [dd]
            continue

        # Find the correct parent: last node with a smaller level number
        while stack and stack[-1].level >= dd.level:
            stack.pop()

        if stack:
            stack[-1].children.append(dd)
        else:
            # Orphaned entry (bad level ordering) — attach to roots
            roots.append(dd)

        stack.append(dd)

    return roots


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_DIVISION_KEYWORDS = {
    "IDENTIFICATION", "ENVIRONMENT", "DATA", "PROCEDURE",
    "FILE", "WORKING-STORAGE", "LINKAGE", "LOCAL-STORAGE",
    "SCREEN", "REPORT",
}

_CLAUSE_KEYWORDS = {
    "PIC", "PICTURE", "USAGE", "REDEFINES", "OCCURS", "VALUE", "VALUES",
    "SIGN", "SYNCHRONIZED", "SYNC", "JUSTIFIED", "JUST", "BLANK",
    "GLOBAL", "EXTERNAL", "RENAMES",
}


def _is_level_number(s: str) -> bool:
    try:
        n = int(s)
        return (1 <= n <= 49) or n in (66, 77, 78, 88)
    except ValueError:
        return False


def _is_keyword(s: str) -> bool:
    return s.upper() in (_DIVISION_KEYWORDS | _CLAUSE_KEYWORDS)
