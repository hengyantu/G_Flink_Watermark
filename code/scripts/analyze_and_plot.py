#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import glob
import os
import re
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def parse_trigger_lag_from_files(files):
    pattern = re.compile(r"triggerLagMs=([\-]?\d+)")
    values = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8", errors="ignore") as fh:
                for line in fh:
                    m = pattern.search(line)
                    if m:
                        try:
                            v = int(m.group(1))
                            values.append(v)
                        except Exception:
                            pass
        except Exception:
            continue
    return values


def summarize(values):
    if not values:
        return dict(count=0, avg=0.0, min=0, max=0)
    total = sum(values)
    return dict(
        count=len(values),
        avg=total / len(values),
        min=min(values),
        max=max(values),
    )


def analyze(base_dir):
    """
    base_dir 目录结构期望：
      base_dir/
        delay_2000/window_results/*
        delay_2000/late_events/*
        delay_5000/window_results/*
        ...
    """
    results = []
    delay_dirs = sorted(glob.glob(os.path.join(base_dir, "delay_*")))
    for dd in delay_dirs:
        delay_name = os.path.basename(dd)
        try:
            delay_ms = int(delay_name.split("_")[1])
        except Exception:
            continue

        window_files = glob.glob(os.path.join(dd, "window_results", "*"))
        late_files = glob.glob(os.path.join(dd, "late_events", "*"))

        lag_values = parse_trigger_lag_from_files(window_files)
        lag_sum = summarize(lag_values)
        late_count = 0
        for lf in late_files:
            try:
                with open(lf, "r", encoding="utf-8", errors="ignore") as fh:
                    for _ in fh:
                        late_count += 1
            except Exception:
                pass

        results.append({
            "delay_ms": delay_ms,
            "count": lag_sum["count"],
            "avg": lag_sum["avg"],
            "min": lag_sum["min"],
            "max": lag_sum["max"],
            "late": late_count
        })
    results.sort(key=lambda x: x["delay_ms"])
    return results


def save_csv(results, out_csv):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    with open(out_csv, "w", encoding="utf-8") as f:
        f.write("watermarkDelayMs,count,avgTriggerLagMs,minTriggerLagMs,maxTriggerLagMs,lateEvents\n")
        for r in results:
            f.write("{},{},{:.3f},{},{},{}\n".format(
                r["delay_ms"], r["count"], r["avg"], r["min"], r["max"], r["late"]
            ))


def plot_lines(results, out_png_lag, out_png_late):
    delays = [r["delay_ms"] for r in results]
    avgs = [r["avg"] for r in results]
    lates = [r["late"] for r in results]

    # Plot average trigger lag
    plt.figure(figsize=(7, 4))
    plt.plot(delays, avgs, marker="o")
    plt.title("Avg Trigger Lag vs Watermark Delay")
    plt.xlabel("Watermark Delay (ms)")
    plt.ylabel("Avg Trigger Lag (ms)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    os.makedirs(os.path.dirname(out_png_lag), exist_ok=True)
    plt.savefig(out_png_lag)
    plt.close()

    # Plot late events
    plt.figure(figsize=(7, 4))
    plt.plot(delays, lates, marker="o", color="tab:red")
    plt.title("Late Events vs Watermark Delay")
    plt.xlabel("Watermark Delay (ms)")
    plt.ylabel("Late Events (count)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    os.makedirs(os.path.dirname(out_png_late), exist_ok=True)
    plt.savefig(out_png_late)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Analyze Flink watermark experiments and plot images.")
    parser.add_argument("--base_dir", required=True, help="实验输出根目录，例如 code/outputs/20250101_120000")
    parser.add_argument("--out_dir", required=True, help="分析输出目录，例如 code/outputs/20250101_120000/analysis")
    args = parser.parse_args()

    results = analyze(args.base_dir)
    if not results:
        print("未发现任何 delay_* 子目录或没有有效结果。")
        return

    out_csv = os.path.join(args.out_dir, "summary.csv")
    out_png_lag = os.path.join(args.out_dir, "avg_trigger_lag.png")
    out_png_late = os.path.join(args.out_dir, "late_events.png")

    save_csv(results, out_csv)
    plot_lines(results, out_png_lag, out_png_late)

    print("分析完成：")
    print("CSV:", out_csv)
    print("图1（触发延迟）:", out_png_lag)
    print("图2（迟到数量）:", out_png_late)


if __name__ == "__main__":
    main()


