#!/usr/bin/env python3
"""Prints the README's benchmark tables from results/*.json."""
import re
import json
import os

root = os.path.join(os.path.dirname(__file__), "..", "results")


def kind():
    rows = json.load(open(os.path.join(root, "bench-kind.json")))
    rows += [dict(r, mode="cold, image pulled from Docker Hub") for r in json.load(open(os.path.join(root, "bench-kind-uncached.json")))]
    print("| mode | trials | p50 | max |\n|---|--:|--:|--:|")
    for r in rows:
        mode = {"cold": "cold, image on node", "warm": "warm claim"}.get(r["mode"], r["mode"])
        print(f"| {mode} | {len(r['seconds'])} | {r['p50']:.2f}s | {r['max']:.2f}s |")


def ms(q, key="p99_s"):
    return f"{q[key] * 1000:.0f}ms" if q[key] < 1 else f"{q[key]:.2f}s"


def le(q):
    b = q.get("p99_bucket_le", 0)
    return "> 1000s" if b == -1 else f"≤ {b:g}s"


def load(phases):
    """One row per run and phase, in the order hack/loadtest-matrix.sh ran them (later runs ran hotter)."""
    print("| # | config | pods | phase | wall | reconciles | reconcile p50 | reconcile p99 | queue wait p99 | create→Ready p50 / p99 |")
    print("|--:|---|--:|---|--:|--:|--:|--:|--:|--:|")
    matrix = open(os.path.join(os.path.dirname(__file__), "loadtest-matrix.sh")).read()
    order = re.findall(r"^run (\S+)", matrix, re.M)  # execution order
    for n, label in enumerate(order, 1):
        f = os.path.join(root, f"loadtest-{label}.json")
        if not os.path.exists(f):
            continue
        r = json.load(open(f))
        for p in r["phases"]:
            if p["name"] not in phases:
                continue
            e2e = p.get("create_to_ready") or {}
            e = f"{ms(e2e, 'p50_s')} / {ms(e2e)}" if e2e.get("count") else ""
            print(f"| {n} | {r['label']} | {r['pods_at_peak']} | {p['name']} | {p['seconds']:.0f}s | {p['reconcile_time']['count']} "
                  f"| {ms(p['reconcile_time'], 'p50_s')} | {ms(p['reconcile_time'])} | {le(p['queue_wait'])} | {e} |")


kind()
print()
load({"create"})
print()
load({"restart-resync", "delete"})
