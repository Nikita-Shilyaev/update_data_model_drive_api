"""Чтение и обновление сервисной таблицы pipeline_state.csv на Google Drive.

Таблица — общий файл для всех шагов пайплайна, по одной строке на пару
источник+слой (`source`, `layer`, `updated_at`).
"""

import io

import pandas as pd
from googleapiclient.http import MediaIoBaseUpload

from lib.drive_client import download_bytes

STATE_COLUMNS = ["source", "layer", "updated_at"]


def read_last_updated(service, file_id, source, layer):
    """Возвращает последнюю дату обновления (source, layer) из pipeline_state.csv.

    Возвращает pd.Timestamp или None, если записи по этой паре нет.
    """
    buffer = download_bytes(service, file_id)
    try:
        state = pd.read_csv(buffer)
    except pd.errors.EmptyDataError:
        return None

    if not set(STATE_COLUMNS).issubset(state.columns):
        return None

    row = state.loc[(state["source"] == source) & (state["layer"] == layer), "updated_at"]
    if row.empty:
        return None
    return pd.to_datetime(row.iloc[0]).normalize()


def update_pipeline_state(service, file_id, source, layer, updated_at):
    """Идемпотентно обновляет строку (source, layer) в pipeline_state.csv."""
    buffer = download_bytes(service, file_id)
    try:
        state = pd.read_csv(buffer)
    except pd.errors.EmptyDataError:
        state = pd.DataFrame(columns=STATE_COLUMNS)

    if not set(STATE_COLUMNS).issubset(state.columns):
        state = pd.DataFrame(columns=STATE_COLUMNS)

    # upsert: убираем старую строку по паре источник+слой и добавляем новую
    state = state[~((state["source"] == source) & (state["layer"] == layer))]
    new_row = pd.DataFrame([{"source": source, "layer": layer, "updated_at": updated_at}])
    state = pd.concat([state, new_row], ignore_index=True)

    out = io.BytesIO()
    state.to_csv(out, index=False)
    out.seek(0)
    media = MediaIoBaseUpload(out, mimetype="text/csv", resumable=True)
    service.files().update(
        fileId=file_id, media_body=media, supportsAllDrives=True
    ).execute()
    return state
