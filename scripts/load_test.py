"""Load test: submit a bulk action over the API and measure real throughput.

Drives the whole system as a client would -- HTTP submission, queue, workers,
Postgres -- and reports sustained entities/minute from the platform's own
counters, so the number is the same one `/stats` reports rather than a
stopwatch held by the test.

    python scripts/load_test.py --account-id <uuid> --count 200000

Add `--concurrent 5` to submit several actions at once and watch how the
per-account rate limiter shapes aggregate throughput.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import statistics
import time
from typing import Any

import httpx

POLL_INTERVAL = 1.0


async def submit(client: httpx.AsyncClient, account_id: str, entity: str, status: str,
                 batch_size: int, dedup: str | None) -> str:
    payload: dict[str, Any] = {
        "account_id": account_id,
        "entity_type": entity,
        "action_type": "update",
        "payload": {"updates": {"status": status}, "filter": {}},
        "batch_size": batch_size,
    }
    if dedup:
        payload["payload"]["deduplicate_by"] = dedup

    response = await client.post("/bulk-actions", json=payload)
    response.raise_for_status()
    return response.json()["id"]


async def wait_for(client: httpx.AsyncClient, action_id: str, timeout: float) -> dict[str, Any]:
    """Poll until terminal, printing a progress line as it goes."""
    deadline = time.monotonic() + timeout
    samples: list[tuple[float, int]] = []
    last_line = ""

    while time.monotonic() < deadline:
        stats = (await client.get(f"/bulk-actions/{action_id}/stats")).json()
        samples.append((time.monotonic(), stats["processed_count"]))

        line = (
            f"  {stats['status']:<22} "
            f"{stats['processed_count']:>9,}/{stats['total_entities']:<9,} "
            f"({stats['progress_percent']:>6.2f}%)  "
            f"ok={stats['success_count']:,} fail={stats['failure_count']:,} "
            f"skip={stats['skipped_count']:,}"
        )
        if line != last_line:
            print(line, flush=True)
            last_line = line

        if stats["status"] in {"completed", "completed_with_errors", "failed", "cancelled"}:
            stats["_samples"] = samples
            return stats
        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Action {action_id} did not finish within {timeout}s.")


def _steady_state_rate(samples: list[tuple[float, int]]) -> float | None:
    """Throughput excluding the first and last sample.

    The first interval includes planning and the last is a partial batch;
    neither reflects sustained throughput.
    """
    if len(samples) < 4:
        return None
    rates = [
        (b[1] - a[1]) / (b[0] - a[0]) * 60
        for a, b in itertools.pairwise(samples[1:-1])
        if b[0] > a[0]
    ]
    return statistics.median(rates) if rates else None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk action load test.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--entity", default="contact")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--concurrent", type=int, default=1,
                        help="Submit N actions simultaneously.")
    parser.add_argument("--deduplicate-by", default=None)
    parser.add_argument("--timeout", type=float, default=900.0)
    args = parser.parse_args()

    async with httpx.AsyncClient(base_url=args.base_url, timeout=60.0) as client:
        health = await client.get("/readyz")
        if health.status_code != 200:
            raise SystemExit(f"API not ready: {health.text}")

        print(f"Submitting {args.concurrent} bulk action(s) on '{args.entity}' "
              f"with batch_size={args.batch_size}...")
        started = time.perf_counter()

        action_ids = await asyncio.gather(
            *(
                submit(
                    client,
                    args.account_id,
                    args.entity,
                    f"loadtest-{i}-{int(time.time())}",
                    args.batch_size,
                    args.deduplicate_by,
                )
                for i in range(args.concurrent)
            )
        )
        for action_id in action_ids:
            print(f"  queued {action_id}")
        print()

        results = await asyncio.gather(
            *(wait_for(client, aid, args.timeout) for aid in action_ids)
        )
        wall = time.perf_counter() - started

    total_processed = sum(r["processed_count"] for r in results)
    total_success = sum(r["success_count"] for r in results)
    total_failed = sum(r["failure_count"] for r in results)
    total_skipped = sum(r["skipped_count"] for r in results)

    print("\n" + "=" * 68)
    print("LOAD TEST RESULTS")
    print("=" * 68)
    print(f"  actions submitted   : {len(results)}")
    print(f"  entities processed  : {total_processed:,}")
    print(f"    success           : {total_success:,}")
    print(f"    failed            : {total_failed:,}")
    print(f"    skipped           : {total_skipped:,}")
    print(f"  wall clock          : {wall:.1f}s  (includes submission and polling)")

    for result in results:
        duration = result.get("duration_seconds") or 0
        print(f"\n  action {result['id']}")
        print(f"    status            : {result['status']}")
        print(f"    batches           : {result['completed_batches']}/{result['total_batches']}")
        print(f"    server duration   : {duration:.2f}s")
        if result.get("entities_per_minute"):
            print(f"    entities/minute   : {result['entities_per_minute']:,.0f}")
        steady = _steady_state_rate(result.get("_samples", []))
        if steady:
            print(f"    steady-state rate : {steady:,.0f} entities/minute")
        if result.get("failure_breakdown"):
            print(f"    failures          : {result['failure_breakdown']}")
        if result.get("skip_breakdown"):
            print(f"    skips             : {result['skip_breakdown']}")

    if wall > 0:
        print(f"\n  aggregate           : {total_processed / wall * 60:,.0f} entities/minute")
    print("=" * 68)


if __name__ == "__main__":
    asyncio.run(main())
