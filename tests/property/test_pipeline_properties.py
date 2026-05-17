"""Property: the pipeline never *loses* records.

For any non-empty list of input lines, ``records_processed +
records_failed == lines_in``. This is the count-conservation property the
plan promises in its "zero data loss under backpressure" goal.

We don't test backpressure-correctness here (that's covered by an explicit
benchmark); we test that nothing falls on the floor in the *normal* case.
"""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pipelinex.core.pipeline import PipelineExecutor
from pipelinex.core.sources import IterableLogSource
from pipelinex.persistence.memory_repo import InMemoryLogRepository
from pipelinex.stages.parsing.hdfs_parser import HDFSParser
from pipelinex.stages.parsing.parser_stage import ParserStage


@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(
    lines=st.lists(st.text(max_size=80), min_size=1, max_size=200),
    workers=st.integers(min_value=1, max_value=4),
    queue_size=st.integers(min_value=2, max_value=20),
    batch_size=st.integers(min_value=1, max_value=10),
)
def test_records_in_equals_records_out(
    lines: list[str],
    workers: int,
    queue_size: int,
    batch_size: int,
) -> None:
    asyncio.run(_run_and_assert(lines, workers, queue_size, batch_size))


async def _run_and_assert(
    lines: list[str], workers: int, queue_size: int, batch_size: int
) -> None:
    source = IterableLogSource(lines)
    repo = InMemoryLogRepository()
    executor = PipelineExecutor(
        source=source,
        stages=[ParserStage(HDFSParser())],
        repository=repo,
        num_workers=workers,
        queue_size=queue_size,
        batch_size=batch_size,
        flush_interval_s=0.05,
    )
    result = await executor.run()
    accounted = (
        result.records_processed + result.records_failed + result.records_shed
    )
    assert accounted == len(lines), (
        f"records_in={len(lines)} processed={result.records_processed} "
        f"failed={result.records_failed} shed={result.records_shed} "
        f"(lost={len(lines) - accounted})"
    )


# Sanity checks that make the property test less likely to be vacuously
# satisfied (e.g. by always having parser errors).


@pytest.mark.asyncio
async def test_well_formed_lines_all_succeed() -> None:
    line = (
        "081109 203615 148 INFO dfs.DataNode$PacketResponder: "
        "PacketResponder 1 for block blk_1 terminating"
    )
    source = IterableLogSource([line] * 100)
    repo = InMemoryLogRepository()
    executor = PipelineExecutor(
        source=source,
        stages=[ParserStage(HDFSParser())],
        repository=repo,
        num_workers=2,
        queue_size=10,
        batch_size=5,
        flush_interval_s=0.05,
    )
    result = await executor.run()
    assert result.records_processed == 100
    assert result.records_failed == 0
