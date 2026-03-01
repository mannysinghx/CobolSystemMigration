"""
Validation routes.

Compares source extraction counts/aggregates against target after a migration.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.models import ValidationRequest, ValidationResponse, ValidationResult
from backend.core.state.tracker import MigrationTracker
from backend.db.connection import get_db

router = APIRouter(prefix="/validation", tags=["validation"])


@router.post("", response_model=ValidationResponse)
async def run_validation(
    body: ValidationRequest,
    db: AsyncSession = Depends(get_db),
) -> ValidationResponse:
    """
    Run post-migration validation checks.

    Currently implements:
      - row_count: source extracted vs target loaded
      - aggregate: rejection rate per table
    """
    tracker = MigrationTracker(db)
    run = await tracker.get_run(body.run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {body.run_id} not found")

    table_states = await tracker.list_table_states(body.run_id)
    if body.table_names:
        table_states = [t for t in table_states if t.table_name in body.table_names]

    results: list[ValidationResult] = []

    for state in table_states:
        if "row_count" in body.checks:
            extracted = state.rows_extracted or 0
            loaded = state.rows_loaded or 0
            rejected = state.rows_rejected or 0
            passed = extracted == (loaded + rejected)
            results.append(
                ValidationResult(
                    table_name=state.table_name,
                    check="row_count",
                    passed=passed,
                    source_value=extracted,
                    target_value=loaded + rejected,
                    details=f"extracted={extracted}, loaded={loaded}, rejected={rejected}",
                )
            )

        if "aggregate" in body.checks:
            extracted = state.rows_extracted or 0
            rejection_rate = (
                (state.rows_rejected or 0) / extracted if extracted > 0 else 0.0
            )
            passed = rejection_rate < 0.01  # <1% rejection
            results.append(
                ValidationResult(
                    table_name=state.table_name,
                    check="aggregate",
                    passed=passed,
                    source_value=f"{rejection_rate:.2%}",
                    target_value="<1%",
                    details=f"rejection rate: {rejection_rate:.4%}",
                )
            )

    return ValidationResponse(
        run_id=body.run_id,
        results=results,
        all_passed=all(r.passed for r in results),
    )
