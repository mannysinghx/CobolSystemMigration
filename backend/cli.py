"""
CobolShift CLI.

Usage examples:
    cobolshift parse  my.cpy
    cobolshift ddl    my.cpy --dialect postgresql --out schema.sql
    cobolshift load   my.dat --copybook my.cpy --target postgres://... --table customer
    cobolshift run    --project <id> --config run.json
    cobolshift status --run <run-id>
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

app = typer.Typer(
    name="cobolshift",
    help="End-to-end COBOL → SQL migration tool",
    no_args_is_help=True,
)
console = Console()


# ---------------------------------------------------------------------------
# parse — copybook → Schema IR
# ---------------------------------------------------------------------------

@app.command()
def parse(
    copybook: Path = typer.Argument(..., help="Path to COBOL copybook (.cpy)"),
    redefines: str = typer.Option("wide_table", help="wide_table|subtype_tables|jsonb"),
    occurs: str = typer.Option("child_table", help="child_table|json_array|wide_columns"),
    filler: str = typer.Option("skip", help="skip|include"),
    out: Optional[Path] = typer.Option(None, "--out", "-o", help="Write JSON to file"),
) -> None:
    """Parse a COBOL copybook and print the Schema IR as JSON."""
    from backend.core.parser.cobol_parser import CobolParser
    from backend.core.parser.layout_calculator import LayoutCalculator
    from backend.core.analyzer.schema_analyzer import SchemaAnalyzer, AnalyzerConfig
    import dataclasses

    console.print(f"[bold cyan]Parsing[/] {copybook}")

    parser = CobolParser()
    items = parser.parse_copybook(copybook)

    calc = LayoutCalculator()
    items = calc.calculate(items)

    config = AnalyzerConfig(
        filler_strategy=filler,
        redefines_strategy=redefines,
        occurs_strategy=occurs,
    )
    schema = SchemaAnalyzer(config).analyze(items, table_name=copybook.stem)

    schema_dict = dataclasses.asdict(schema)
    output = json.dumps(schema_dict, indent=2, default=str)

    if out:
        out.write_text(output)
        console.print(f"[green]Schema IR written to[/] {out}")
    else:
        console.print_json(output)

    # Summary table
    tbl = Table(title="Tables found", show_lines=True)
    tbl.add_column("Table")
    tbl.add_column("Columns", justify="right")
    tbl.add_column("Type")
    for t in schema.tables:
        tbl.add_row(t.name, str(len(t.columns)), t.table_type)
    console.print(tbl)


# ---------------------------------------------------------------------------
# ddl — copybook → SQL DDL
# ---------------------------------------------------------------------------

@app.command()
def ddl(
    copybook: Path = typer.Argument(..., help="Path to COBOL copybook (.cpy)"),
    dialect: str = typer.Option("postgresql", help="postgresql|sqlserver"),
    schema_name: str = typer.Option("public", "--schema", help="Target schema name"),
    flyway_version: str = typer.Option("001", help="Flyway version prefix"),
    flyway_desc: str = typer.Option("initial_load", help="Flyway description"),
    no_comments: bool = typer.Option(False, "--no-comments", help="Omit column comments"),
    out: Optional[Path] = typer.Option(None, "--out", "-o", help="Write SQL to file"),
) -> None:
    """Generate SQL DDL from a COBOL copybook."""
    from backend.core.parser.cobol_parser import CobolParser
    from backend.core.parser.layout_calculator import LayoutCalculator
    from backend.core.analyzer.schema_analyzer import SchemaAnalyzer, AnalyzerConfig
    from backend.core.generator.ddl_generator import DDLGenerator

    console.print(f"[bold cyan]Generating DDL[/] ({dialect}) from {copybook}")

    parser = CobolParser()
    items = parser.parse_copybook(copybook)
    items = LayoutCalculator().calculate(items)
    schema = SchemaAnalyzer(AnalyzerConfig()).analyze(items, table_name=copybook.stem)

    gen = DDLGenerator(
        dialect=dialect,
        schema_name=schema_name,
        include_comments=not no_comments,
        flyway_version=flyway_version,
        flyway_description=flyway_desc,
    )
    output = gen.generate(schema)

    if out:
        out.write_text(output.sql)
        console.print(f"[green]DDL written to[/] {out}")
    else:
        console.print(output.sql)

    console.print(f"\n[bold]Tables:[/] {', '.join(output.table_names)}")


# ---------------------------------------------------------------------------
# load — load a data file directly (no API / DB)
# ---------------------------------------------------------------------------

@app.command()
def load(
    data_file: Path = typer.Argument(..., help="Path to flat data file"),
    copybook: Path = typer.Option(..., "--copybook", "-c", help="Path to copybook"),
    table: str = typer.Option(..., "--table", "-t", help="Target table name"),
    target: str = typer.Option(..., "--target", help="Connection string"),
    target_type: str = typer.Option("postgresql", help="postgresql|sqlserver"),
    record_format: str = typer.Option("F", "--format", help="F|V|VB"),
    record_length: Optional[int] = typer.Option(None, "--length", help="Record length (RECFM=F)"),
    encoding: str = typer.Option("cp037", "--encoding", help="EBCDIC code page"),
    mode: str = typer.Option("truncate_load", "--mode", help="truncate_load|append|upsert"),
    schema_name: str = typer.Option("public", "--schema"),
    batch_size: int = typer.Option(10_000, "--batch-size"),
) -> None:
    """Directly load a flat COBOL data file into a database table."""

    async def _run():
        from backend.core.parser.cobol_parser import CobolParser
        from backend.core.parser.layout_calculator import LayoutCalculator
        from backend.core.analyzer.schema_analyzer import SchemaAnalyzer, AnalyzerConfig
        from backend.core.decoder.record_decoder import RecordDecoder
        from backend.core.pipeline.extraction import ExtractionConfig, ExtractionPipeline

        # Parse copybook
        items = CobolParser().parse_copybook(copybook)
        items = LayoutCalculator().calculate(items)
        schema = SchemaAnalyzer(AnalyzerConfig()).analyze(items, table_name=copybook.stem)
        table_ir = next((t for t in schema.tables if t.name == table), schema.tables[0])

        # Build loader
        if target_type == "postgresql":
            from backend.core.loader.pg_loader import PostgresLoader, LoadConfig
            loader = PostgresLoader(target)
        else:
            from backend.core.loader.sqlserver_loader import SqlServerLoader, LoadConfig
            loader = SqlServerLoader(target)

        load_cfg = LoadConfig(
            table_name=table,
            schema_name=schema_name,
            column_names=[c.name for c in table_ir.columns],
            mode=mode,
            batch_size=batch_size,
        )
        await loader.prepare_table(load_cfg)

        decoder = RecordDecoder(encoding=encoding)
        ext_cfg = ExtractionConfig(
            source_path=data_file,
            record_format=record_format,
            record_length=record_length or 0,
            encoding=encoding,
            table_name=table,
        )
        pipeline = ExtractionPipeline(ext_cfg)

        async def records():
            for result in pipeline.stream():
                if result.error:
                    console.print(f"[red]Extraction error line {result.line_number}:[/] {result.error}")
                    continue
                decoded = decoder.decode(result.raw_bytes, table_ir)
                yield decoded.values

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed} rows"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"Loading {table}…", total=None)

            def on_progress(loaded: int, rejected: int) -> None:
                progress.update(task, completed=loaded, description=f"Loading {table} (rejected={rejected})")

            stats = await loader.load_table(load_cfg, records(), on_progress)

        if hasattr(loader, "close"):
            result = loader.close()
            if asyncio.iscoroutine(result):
                await result

        console.print(
            Panel(
                f"[green]Loaded:[/] {stats.rows_loaded:,}\n"
                f"[red]Rejected:[/] {stats.rows_rejected:,}\n"
                f"Batches ok: {stats.batches_ok}  failed: {stats.batches_failed}",
                title="Load complete",
            )
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# status — show run summary
# ---------------------------------------------------------------------------

@app.command()
def status(
    run_id: str = typer.Option(..., "--run", help="Migration run UUID"),
    db_url: str = typer.Option(
        None,
        "--db",
        envvar="DATABASE_URL",
        help="Tool's own PostgreSQL URL",
    ),
) -> None:
    """Show the status of a migration run."""

    async def _run():
        import uuid
        from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
        from sqlalchemy.orm import sessionmaker

        engine = create_async_engine(db_url)
        Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with Session() as session:
            from backend.core.state.tracker import MigrationTracker

            tracker = MigrationTracker(session)
            summary = await tracker.run_summary(uuid.UUID(run_id))

        if not summary:
            console.print(f"[red]Run {run_id} not found.[/]")
            raise typer.Exit(1)

        tbl = Table(title=f"Run {summary['run_id'][:8]}…", show_lines=True)
        for k, v in summary.items():
            tbl.add_row(str(k), str(v))
        console.print(tbl)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# server — launch the FastAPI dev server
# ---------------------------------------------------------------------------

@app.command()
def server(
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(8000, help="Bind port"),
    reload: bool = typer.Option(False, help="Enable hot-reload (dev only)"),
) -> None:
    """Start the CobolShift API server."""
    import uvicorn

    uvicorn.run(
        "backend.main:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


if __name__ == "__main__":
    app()
