"""Обновление Sales_sku_target.csv инкрементами из raw/sales_sku на Google Drive.

Инкременты по продажам в разрезе штрихкодов — суточные выгрузки из 1С, уже
лежащие в raw/sales_sku/<дата>/. Скрипт обрабатывает даты из диапазона
(processed-watermark + 1 день … min(raw-watermark, ds_run − 1 день)), приводит
инкременты к схеме целевого файла и вливает их в базовый файл.

Идемпотентность обеспечивается заменой по дате продажи, а не снятием дубликатов
по ключу: в этом источнике нет натурального ключа строки (номера чека нет, а
`штрихкод` не уникален — у платков и прочих групповых позиций один код на всю
модель, в истории 1762 повтора). Поэтому из базового файла удаляются все строки
с датами, встретившимися в инкрементах, и на их место кладётся содержимое
инкрементов. Каждая суточная выгрузка — полный срез за свой день, так что замена
корректна, а повторный прогон за ту же дату даёт тот же результат.

Даты берутся из поля `Период день` самих инкрементов, а не из имён папок: на FTP
файл за 22.06.2026 выкладывается в папку 2026-06-23, и опора на имя папки
удаляла бы из целевого файла не тот день.

Файл перезаписывается целиком одной операцией в конце: если что-то упало на
середине диапазона, ни целевой файл, ни watermark не меняются.
"""

import argparse
import io
import logging
import re
import sys
from configparser import ConfigParser
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR))

import numpy as np
import pandas as pd

from lib.drive_client import (
    download_bytes,
    get_drive_service,
    list_children,
    update_file_bytes,
)
from lib.pipeline_state import read_last_updated, update_pipeline_state

CONFIG_PATH = PROJECT_DIR / "config.ini"

SOURCE = "sales_sku"
LAYER = "processed"
RAW_LAYER = "raw"

FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
EXCEL_SUFFIXES = {".xlsx", ".xls"}

NO_DATA = "нет данных"

COLUMN_RENAMES = {
    "Период день": "date",
    "Магазин.Наименование": "магазин",
    "Продавец.ФИО": "продавец",
    "Номенклатура.БИТ Основной штрихкод": "штрихкод",
    "Номенклатура.Артикул": "артикул",
    "Номенклатура.Наименование": "номенклатура",
    "Номенклатура.БИТ Дата прихода АО": "дата_прихода",
    "Номенклатура.БИТ Литера.Наименование": "бит_литера",
    "Номенклатура.Входит в группу": "поставщик",
    "Номенклатура.Камень.Наименование": "вставка",
    "Номенклатура.Категория.Наименование": "категория",
    "Номенклатура.Подкатегория.Наименование": "подкатегория",
    "Номенклатура.Коллекция.Наименование": "коллекция",
    "Номенклатура.БИТ Вес": "вес",
    "Номенклатура.БИТ размер": "размер",
    "Номенклатура.Материал.Наименование": "материал",
    "Номенклатура.Плетение.Наименование": "плетение",
    "Номенклатура.Цвет.Наименование": "цвет",
    "Номенклатура.Ценовая группа.Наименование": "ценовая_группа",
    "Номенклатура.Пол": "пол",
    "Количество товаров": "количество",
    "Сумма продаж без скидки": "сумма_без_скидки",
    "Сумма продаж со скидкой": "сумма_со_скидкой",
}

TARGET_COLUMNS = [
    "date",
    "магазин",
    "продавец",
    "штрихкод",
    "артикул",
    "номенклатура",
    "дата_прихода",
    "бит_литера",
    "поставщик",
    "вставка",
    "категория",
    "подкатегория",
    "коллекция",
    "вес",
    "размер",
    "материал",
    "плетение",
    "цвет",
    "ценовая_группа",
    "пол",
    "количество",
    "сумма_без_скидки",
    "сумма_со_скидкой",
]

TEXT_COLUMNS = [
    "магазин",
    "продавец",
    "артикул",
    "номенклатура",
    "бит_литера",
    "поставщик",
    "вставка",
    "категория",
    "подкатегория",
    "коллекция",
    "материал",
    "плетение",
    "цвет",
    "пол",
]

# заполняются заглушкой; `продавец`, `дата_прихода`, `вес`, `размер` и
# `ценовая_группа` намеренно остаются пустыми
FILL_NO_DATA_COLUMNS = [
    "артикул",
    "номенклатура",
    "бит_литера",
    "вставка",
    "категория",
    "подкатегория",
    "материал",
    "плетение",
    "цвет",
    "пол",
]

# значения-заглушки из 1С, неотличимые от пропуска
EMPTY_TOKENS = {"", "-", "--", "---", "<>", "не указан", "не указано", "нет"}

CATEGORY_MAP = {
    "Кольцо": "Кольца",
    "Цепь": "Цепи",
    "Подвеска": "Подвески",
    "Аксессуары для волос": "Аксессуары",
    **dict.fromkeys(
        [
            "Кольца обручальные",
            "Кольцо обручальное",
            "Кольцаобручальные",
            "Кольцасвадьба",
        ],
        "Кольца свадьба",
    ),
    **dict.fromkeys(["Серьги фасонные", "Серги"], "Серьги"),
    **dict.fromkeys(["Посуда", "Сувениры, Подарки", "Другие"], "Другое"),
}

# казахские написания схлопываем в русские, разнобой в числе — в единственное
COLLECTION_MAP = {
    "Сәтті": "Сатти",
    "Қарлығаш": "Карлыгаш",
    "Қарлыгаш": "Карлыгаш",
    "Анаға тағзым": "Анага тагзым",
    "Batyr": "Батыр",
    "Без коллекций": "Без коллекции",
    "Штамповки": "Штамповка",
    "Браслеты штамповка": "Штамповка",
    "Браслеты нац. штамповка": "Штамповка",
    "Монеты": "Монета",
    "Монета. Национальные изделия": "Монета",
    "Обручальные кольца СКИФ": "Кольца обручальные Скиф",
    "Аксессуар для волос": "Аксессуары для волос",
}

GENDER_MAP = {
    "мужской": "Мужской",
    "Мужское": "Мужской",
    "женское": "Женский",
    "Женское": "Женский",
    "Женкий": "Женский",
    "детские": "Детский",
    "Детские": "Детский",
    "детский": "Детский",
    "унисекс": "Унисекс",
}

# цвет металла ведём в мужском роде; "Серебро" — существительное, не склоняем
COLOR_MAP = {
    "Красное": "Красный",
    "Желтое": "Желтый",
    "ЖЕЛТЫЙ": "Желтый",
    "золото": "Желтый",
    "Белое": "Белый",
    "Розовое": "Розовый",
    "Комбинированное": "Комбинированный",
}

WEAVE_MAP = {"Якорь": "Якорное", "якорь": "Якорное"}

SUPPLIER_MAP = {
    "ДАНИЛОВА (ГРАНЖ)": "ГРАНЖ",
    "ВОЛКОВА (ГРАНЖ)": "ГРАНЖ",
    '"ГРАНЖ" (ГРАНАТ)': "ГРАНЖ",
}

MAIN_SUPPLIERS = [
    "KY FACTORY",
    "СКИФ",
    "ДЖИ КЕЙ ЭКСПОРТС АРМЕНИЯ",
    "ЭСТЕТ",
    "ТАЙЛАНД MAINLY SILVER DESIGN",
    "КРАСЦВЕТМЕТ",
    "ЛАКСА ТРЕЙДИНГ",
    'АРМЕНИЯ "АЛМАНДИН"',
    "ГРАНЖ",
]

CYRILLIC_TO_LATIN = {
    "А": "A", "В": "B", "Е": "E", "К": "K",
    "М": "M", "Н": "H", "О": "O", "Р": "P",
    "С": "C", "Т": "T", "У": "Y", "Х": "X",
}

LITERA_RE = re.compile(r"\b(СГ|ЗИ|ЮИ)\b")
WHITESPACE_RE = re.compile(r"\s+")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

config = ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

CREDS_PATH = PROJECT_DIR / config["DRIVE_API"]["creds"]
TOKEN_PATH = PROJECT_DIR / config["DRIVE_API"]["token"]

RAW_SALES_SKU_ID = config["PIPELINE_RAW"]["pipeline_raw_sales_sku_id"]
SALES_SKU_TARGET_ID = config["PIPELINE_PROCESSED"]["sales_sku_target_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def clean_text(value):
    """Убирает неразрывные пробелы и повторы, заглушки 1С приводит к NaN."""
    if pd.isna(value):
        return np.nan

    text = WHITESPACE_RE.sub(" ", str(value).replace(" ", " ")).strip()
    if text in EMPTY_TOKENS:
        return np.nan

    return text


def upper_first(value):
    """Поднимает регистр первой буквы, остальные не трогает.

    Именно первой буквы, а не .capitalize(): иначе развалятся легитимные
    написания вроде DQ, ЮРТ, Invictus.
    """
    if pd.isna(value):
        return value

    return value[0].upper() + value[1:]


def sku_processor(sku):
    """Приводит артикул к канону: верхний регистр, латиница, без ведущих нулей."""
    if pd.isna(sku):
        return sku

    sku = str(sku).strip().upper()

    if sku.isdigit():
        return sku.lstrip("0")

    sku = "".join(CYRILLIC_TO_LATIN.get(char, char) for char in sku)

    return sku.lstrip("0")


def skif_processor(row):
    """У СКИФа в артикуле значимы только цифры, буквенные суффиксы — мусор."""
    sku = row["артикул"]
    if pd.isna(sku):
        return sku

    if str(row["поставщик"]) == "СКИФ":
        return "".join(filter(str.isdigit, str(sku)))

    return sku


def strip_litera(value):
    """Убирает литеру склада (СГ/ЗИ/ЮИ) из названия группы поставщика.

    Через re.sub, а не .str.replace(regex=True): в pandas 3 регулярки на
    pyarrow-строках исполняет RE2, где `\\b` работает только по ASCII и на
    кириллице не срабатывает вовсе.
    """
    if pd.isna(value):
        return value

    return LITERA_RE.sub("", str(value)).strip().upper()


def find_increment_files(service, date_strs):
    """Ищет в raw/sales_sku эксель-файл для каждой даты из date_strs.

    Возвращает {дата: file_id}. Останавливает пайплайн, если хотя бы за одну
    дату нет папки или в папке нет эксель-файла.
    """
    date_folders = {
        item["name"]: item["id"]
        for item in list_children(service, RAW_SALES_SKU_ID)
        if item["mimeType"] == FOLDER_MIMETYPE
    }

    file_ids = {}
    missing = []
    for ds in date_strs:
        folder_id = date_folders.get(ds)
        if folder_id is None:
            missing.append(ds)
            continue

        excels = [
            item
            for item in list_children(service, folder_id)
            if Path(item["name"]).suffix.lower() in EXCEL_SUFFIXES
        ]
        if not excels:
            missing.append(ds)
            continue

        file_ids[ds] = excels[0]["id"]

    if missing:
        logger.error(
            "В raw/sales_sku нет инкрементов за даты: %s. "
            "Пайплайн не допускает пропущенных дней — остановка без обновления.",
            ", ".join(missing),
        )
        sys.exit(1)

    return file_ids


def transform_increment(raw):
    """Приводит инкремент sales_sku к схеме целевого файла.

    Выгрузка всегда заканчивается служебной строкой "Итого" в первом столбце —
    отбрасываем её до приведения числовых колонок к типам. Если за дату не было
    продаж, кроме этой строки в файле ничего нет.
    """
    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    first_column = raw.columns[0]
    is_totals_row = raw[first_column].astype(str).str.strip().str.lower().eq("итого")
    raw = raw[~is_totals_row].copy()

    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    incr = raw.rename(columns=COLUMN_RENAMES)

    for column in TEXT_COLUMNS:
        incr[column] = incr[column].map(clean_text)

    # Без штрихкода строка не соединяется ни с одной другой таблицей модели,
    # так что аналитической ценности не несёт — это брак заполнения в 1С.
    # Пустое количество, наоборот, не повод удалять: такая строка несёт суммы.
    incr = incr.dropna(subset=["штрихкод"]).copy()

    if incr.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    # dayfirst=True: эксель отдаёт ячейку и строкой "22.06.2026 0:00:00",
    # и уже готовым datetime — обе формы разбираются одинаково
    incr["date"] = pd.to_datetime(incr["date"], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
    incr["дата_прихода"] = pd.to_datetime(
        incr["дата_прихода"], format="%d.%m.%Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    incr["штрихкод"] = incr["штрихкод"].astype("Int64").astype(str)

    incr["магазин"] = (
        incr["магазин"]
        .str.strip()
        .str.replace("Ювелирный салон ", "", regex=False)
        .str.replace('"', "", regex=False)
        .str.strip()
    )

    incr["категория"] = incr["категория"].replace(CATEGORY_MAP).map(upper_first)

    incr["поставщик"] = incr["поставщик"].map(strip_litera).replace(SUPPLIER_MAP)
    incr["поставщик"] = incr["поставщик"].where(
        incr["поставщик"].isin(MAIN_SUPPLIERS), "Другие"
    )

    # порядок важен: skif_processor смотрит на уже нормализованного поставщика
    incr["артикул"] = incr["артикул"].apply(sku_processor)
    incr["артикул"] = incr.apply(skif_processor, axis=1)

    incr["коллекция"] = (
        incr["коллекция"].replace(COLLECTION_MAP).map(upper_first).fillna("Без коллекции")
    )

    incr["пол"] = incr["пол"].replace(GENDER_MAP).map(upper_first)
    incr["цвет"] = incr["цвет"].replace(COLOR_MAP).map(upper_first)
    incr["плетение"] = incr["плетение"].replace(WEAVE_MAP).map(upper_first)

    for column in ["бит_литера", "вставка", "подкатегория", "материал", "номенклатура"]:
        incr[column] = incr[column].map(upper_first)

    # Int64 (nullable), а не int: пустое количество остаётся пустым, но целые
    # пишутся в CSV как "1", а не "1.0". .round() перед приведением — эксель
    # может отдать 1.0000000001, и .astype() молча срезал бы это в 0
    incr["количество"] = pd.to_numeric(incr["количество"], errors="coerce").round().astype("Int64")

    # суммы держим дробными: скидка часто даёт копейки, и округление построчно
    # уводит дневной итог от контрольной суммы выгрузки. fillna не делаем —
    # пустая сумма должна остаться пустой, на .sum() это не влияет
    incr["сумма_без_скидки"] = pd.to_numeric(incr["сумма_без_скидки"], errors="coerce").round(2)
    incr["сумма_со_скидкой"] = pd.to_numeric(incr["сумма_со_скидкой"], errors="coerce").round(2)
    incr["вес"] = pd.to_numeric(incr["вес"], errors="coerce")
    incr["размер"] = pd.to_numeric(incr["размер"], errors="coerce")

    for column in FILL_NO_DATA_COLUMNS:
        incr[column] = incr[column].fillna(NO_DATA)

    return incr[TARGET_COLUMNS]


def update_sales_sku(ds_run=None):
    """Вливает инкременты sales_sku из raw/sales_sku в Sales_sku_target.csv.

    ds_run — дата запуска (строка YYYY-MM-DD); по умолчанию сегодня.
    Обрабатываются даты до ds_run − 1 включительно и не дальше raw-watermark.
    """
    run_date = (
        pd.to_datetime(ds_run).normalize() if ds_run else pd.Timestamp.today().normalize()
    )

    service = get_drive_service(CREDS_PATH, TOKEN_PATH)

    last_updated = read_last_updated(service, PIPELINE_STATE_ID, SOURCE, LAYER)
    if last_updated is None:
        logger.error(
            "В pipeline_state.csv нет записи о последнем обновлении source=%s, layer=%s. "
            "Задайте базовую дату вручную и повторите запуск.",
            SOURCE,
            LAYER,
        )
        sys.exit(1)

    raw_updated = read_last_updated(service, PIPELINE_STATE_ID, SOURCE, RAW_LAYER)
    if raw_updated is None:
        logger.error(
            "В pipeline_state.csv нет записи о загрузке сырых инкрементов "
            "source=%s, layer=%s. Сначала запустите raw/load_sales_sku_increment.py.",
            SOURCE,
            RAW_LAYER,
        )
        sys.exit(1)

    start_date = last_updated + pd.Timedelta(days=1)
    end_date = min(run_date, raw_updated)

    if start_date > end_date:
        logger.info(
            "Нет новых дат для обработки sales_sku: последнее обновление %s, "
            "в raw загружено по %s, дата запуска %s. Остановка.",
            last_updated.date(),
            raw_updated.date(),
            run_date.date(),
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date, freq="D")]
    logger.info(
        "Диапазон обработки sales_sku: с %s по %s (%s дн.)",
        date_strs[0],
        date_strs[-1],
        len(date_strs),
    )

    # 1. Проверяем, что инкременты есть за все даты диапазона, до их чтения
    file_ids = find_increment_files(service, date_strs)

    # 2. Читаем и приводим к целевой схеме каждый инкремент, в хронологическом порядке
    increments = []
    for ds in date_strs:
        raw = pd.read_excel(download_bytes(service, file_ids[ds]))
        incr = transform_increment(raw)
        increments.append(incr)
        logger.info(
            "Инкремент sales_sku за %s: строк после обработки %s, продаж %s",
            ds,
            len(incr),
            int(incr["количество"].sum()) if len(incr) else 0,
        )

    combined_increments = pd.concat(increments, ignore_index=True)
    if combined_increments.empty:
        logger.info("В инкрементах за диапазон нет ни одной строки продаж.")

    # 3. Читаем базовый файл
    target = pd.read_csv(
        download_bytes(service, SALES_SKU_TARGET_ID),
        dtype={"штрихкод": str, "артикул": str, "date": str, "дата_прихода": str},
    )
    logger.info("Базовый файл Sales_sku_target.csv: строк %s", len(target))

    missing_columns = [column for column in TARGET_COLUMNS if column not in target.columns]
    if missing_columns:
        logger.error(
            "В Sales_sku_target.csv нет колонок: %s. "
            "Приведите базовый файл к целевой схеме и повторите запуск.",
            ", ".join(missing_columns),
        )
        sys.exit(1)

    # 4. Заменяем в базе дни, покрытые инкрементами: суточная выгрузка — полный
    #    срез за свой день, натурального ключа строки в этом источнике нет
    sale_dates = sorted(combined_increments["date"].dropna().unique())
    if sale_dates:
        logger.info(
            "Даты продаж в инкрементах: %s … %s (%s дн.)",
            sale_dates[0],
            sale_dates[-1],
            len(sale_dates),
        )

    replaced = target["date"].isin(sale_dates)
    logger.info("Из базового файла вытеснено строк за эти даты: %s", int(replaced.sum()))

    combined = pd.concat(
        [target[~replaced], combined_increments], ignore_index=True
    ).sort_values("date").reset_index(drop=True)

    combined = combined[TARGET_COLUMNS]

    # 5. Перезаписываем целевой файл на Google Drive
    out = io.BytesIO()
    combined.to_csv(out, index=False)
    update_file_bytes(service, SALES_SKU_TARGET_ID, out.getvalue(), "text/csv")
    logger.info(
        "Sales_sku_target.csv перезаписан: строк всего %s (было %s), продаж всего %s",
        len(combined),
        len(target),
        int(combined["количество"].sum()),
    )

    # 6. Двигаем watermark в pipeline_state до последней обработанной даты
    watermark = date_strs[-1]
    update_pipeline_state(service, PIPELINE_STATE_ID, SOURCE, LAYER, watermark)
    logger.info(
        "pipeline_state.csv обновлён: source=%s, layer=%s, updated_at=%s",
        SOURCE,
        LAYER,
        watermark,
    )
    logger.info(
        "Готово: обновление sales_sku за %s … %s выполнено", date_strs[0], date_strs[-1]
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Обновление Sales_sku_target.csv инкрементами из raw/sales_sku на Google Drive"
    )
    parser.add_argument(
        "--date",
        dest="ds_run",
        default=None,
        help=(
            "Дата запуска в формате YYYY-MM-DD (по умолчанию сегодня). "
            "Обрабатываются даты до дня запуска включительно."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    update_sales_sku(args.ds_run)
