"""Оркестратор пайплайна: последовательный прогон всех шагов raw -> processed.
"""

import argparse
import os
import subprocess
import sys
import time
from collections import namedtuple
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent

Source = namedtuple("Source", "name raw processed")

PIPELINE = [
    Source("cards", "raw/load_cards_increment.py", "processed/update_cards.py"),
    Source("sales_cheki", "raw/load_sales_cheki_increment.py", "processed/update_sales_cheki.py"),
    Source("sales_sku", "raw/load_sales_sku_increment.py", "processed/update_sales_sku.py"),
    Source("vozvraty", "raw/load_vozvraty_increment.py", "processed/update_vozvraty.py"),
    Source("akzii", "raw/load_akzii_increment.py", "processed/update_akzii.py"),
    Source("oplata_sert", "raw/load_oplata_sert_increment.py", "processed/update_oplata_sert.py"),
    Source("rassrochka", "raw/load_rassrochka_increment.py", "processed/update_rassrochka.py"),
    Source("ostatki", "raw/load_ostatki_ftp.py", "processed/update_ostatki.py"),
    # посещения лежат в гугл-таблице, слоя raw у них нет
    Source("visits", None, "processed/update_visits.py"),
]

UPDATED = "ОБНОВЛЕНО"
UNCHANGED = "БЕЗ ИЗМЕНЕНИЙ"
FAILED = "ОШИБКА"
SKIPPED = "ПРОПУЩЕН"

DONE_MARKER = "Готово"

StepResult = namedtuple("StepResult", "source layer status detail seconds")


def run_step(source, layer, script, ds_run):
    """Запускает один скрипт подпроцессом и классифицирует результат."""
    started = time.monotonic()

    command = [sys.executable, script]
    if ds_run:
        command += ["--date", ds_run]

    # PYTHONIOENCODING, чтобы кириллица не побилась о кодировку консоли Windows
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    completed = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    seconds = time.monotonic() - started
    output = completed.stdout or ""
    lines = [line.strip() for line in output.splitlines() if line.strip()]

    if completed.returncode != 0:
        status = FAILED
    elif any(DONE_MARKER in line for line in lines):
        status = UPDATED
    else:
        status = UNCHANGED

    return StepResult(source, layer, status, last_meaningful_line(lines), seconds), output


def last_meaningful_line(lines):
    """Последняя содержательная строка лога — она объясняет исход шага.

    У скриптов это либо «Готово: …», либо «Нет новых дат … Остановка.», либо
    текст ошибки. Служебное предупреждение google-api-client отбрасываем.
    """
    for line in reversed(lines):
        if "file_cache is only supported" in line:
            continue
        # отрезаем префикс логгера "2026-08-01 18:00:00 | INFO | "
        parts = line.split(" | ", 2)
        return parts[-1] if len(parts) == 3 else line

    return ""


def build_summary(results, ds_run, total_seconds):
    """Собирает краткую сводку прогона (её же можно слать в телеграм)."""
    by_source = {}
    for result in results:
        by_source.setdefault(result.source, {})[result.layer] = result

    run_label = ds_run or datetime.now().strftime("%Y-%m-%d")
    lines = [
        "=" * 52,
        f"ИТОГ ПРОГОНА ПАЙПЛАЙНА за {run_label}",
        f"Длительность: {format_duration(total_seconds)}",
        "=" * 52,
        f"{'ИСТОЧНИК':<14}{'RAW':<16}PROCESSED",
    ]

    for source in PIPELINE:
        steps = by_source.get(source.name, {})
        raw_status = steps["raw"].status if "raw" in steps else "—"
        processed_status = steps["processed"].status if "processed" in steps else "—"
        lines.append(f"{source.name:<14}{raw_status:<16}{processed_status}")

    counts = {status: 0 for status in (UPDATED, UNCHANGED, FAILED, SKIPPED)}
    for result in results:
        counts[result.status] += 1

    lines += [
        "-" * 52,
        f"Обновлено: {counts[UPDATED]} · Без изменений: {counts[UNCHANGED]} · "
        f"Ошибок: {counts[FAILED]} · Пропущено: {counts[SKIPPED]}",
    ]

    failures = [r for r in results if r.status in (FAILED, SKIPPED)]
    if failures:
        lines.append("")
        lines.append("Требует внимания:")
        for result in failures:
            lines.append(f"  {result.source} / {result.layer}: {result.detail}")

    return "\n".join(lines)


def format_duration(seconds):
    minutes, seconds = divmod(int(seconds), 60)
    return f"{minutes} мин {seconds} с" if minutes else f"{seconds} с"


def run_pipeline(ds_run=None, verbose=False):
    """Прогоняет все источники по очереди и печатает сводку.

    Возвращает код возврата: 1, если хотя бы один шаг упал, иначе 0.
    """
    started = time.monotonic()
    results = []
    total_steps = sum(2 if source.raw else 1 for source in PIPELINE)
    step_number = 0

    print(f"Старт пайплайна: {datetime.now():%Y-%m-%d %H:%M:%S}", flush=True)

    try:
        for source in PIPELINE:
            raw_failed = False

            if source.raw:
                step_number += 1
                print(f"[{step_number}/{total_steps}] {source.name} / raw …", flush=True)
                result, output = run_step(source.name, "raw", source.raw, ds_run)
                results.append(result)
                report_step(result, output, verbose)
                raw_failed = result.status == FAILED

            step_number += 1
            if raw_failed:
                print(
                    f"[{step_number}/{total_steps}] {source.name} / processed — пропущен "
                    f"(упал raw)",
                    flush=True,
                )
                results.append(
                    StepResult(
                        source.name,
                        "processed",
                        SKIPPED,
                        "не запускался: упал шаг raw",
                        0.0,
                    )
                )
                continue

            print(f"[{step_number}/{total_steps}] {source.name} / processed …", flush=True)
            result, output = run_step(source.name, "processed", source.processed, ds_run)
            results.append(result)
            report_step(result, output, verbose)
    except KeyboardInterrupt:
        print("\nПрогон прерван вручную — печатаю сводку по уже выполненным шагам.")

    print()
    print(build_summary(results, ds_run, time.monotonic() - started))

    return 1 if any(r.status == FAILED for r in results) else 0


def report_step(result, output, verbose):
    """Печатает исход шага сразу после его завершения."""
    if verbose:
        print(output.rstrip(), flush=True)

    print(
        f"    {result.status} ({format_duration(result.seconds)}): {result.detail}",
        flush=True,
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Последовательный прогон всего пайплайна: raw -> processed"
    )
    parser.add_argument(
        "--date",
        dest="ds_run",
        default=None,
        help=(
            "Дата запуска в формате YYYY-MM-DD (по умолчанию сегодня). "
            "Передаётся всем скриптам как есть."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Печатать полный лог каждого шага, а не только его исход",
    )
    parser.add_argument(
        "--only",
        dest="only",
        default=None,
        help=(
            "Прогнать только указанные источники через запятую, "
            "например --only ostatki,visits"
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.only:
        wanted = {name.strip() for name in args.only.split(",") if name.strip()}
        unknown = wanted - {source.name for source in PIPELINE}
        if unknown:
            print(f"Неизвестные источники: {', '.join(sorted(unknown))}")
            sys.exit(1)
        PIPELINE = [source for source in PIPELINE if source.name in wanted]

    sys.exit(run_pipeline(args.ds_run, args.verbose))
