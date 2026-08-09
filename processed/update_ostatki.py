"""Обновление Ostatki_target.csv инкрементами из raw/ostatki на Google Drive.

Остатки — не поток событий, а серия срезов склада на дату. Поэтому логика
слияния отличается от остальных источников processed:

- обрабатываются не все дни диапазона, а только понедельники и последние дни
  месяца (тот же отбор, что и на слое raw — lib/weekly_increment.target_dates);
- недельный срез кладётся в целевой файл со своей датой (2026-07-20);
- срез на конец месяца заменяет собой все недельные срезы этого месяца:
  перед его добавлением из целевого файла удаляются ВСЕ строки месяца, а сам
  он записывается с датой первого дня месяца (31.07.2026 -> date=2026-07-01).
  Так история хранится помесячно, а внутри текущего месяца — понедельно.

Идемпотентность — заменой по колонке `date`, а не снятием дубликатов по
ключу: перед добавлением среза из целевого файла удаляются строки с его датой
(для конца месяца — весь месяц). Повторный прогон за ту же дату даёт тот же
результат.

Даты берутся из имени папки raw/ostatki/<дата>/ (оно же — дата в имени файла
на FTP): в отличие от суточных выгрузок 1С, у остатков дата данных совпадает
с датой папки.

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
from lib.weekly_increment import target_dates

CONFIG_PATH = PROJECT_DIR / "config.ini"

SOURCE = "ostatki"
LAYER = "processed"
RAW_LAYER = "raw"

FOLDER_MIMETYPE = "application/vnd.google-apps.folder"
EXCEL_SUFFIXES = {".xlsx", ".xls"}

NO_DATA = "нет данных"

COLUMN_RENAMES = {
    "Номенклатура.БИТ Дата прихода АО": "дата_прихода",
    "Номенклатура.БИТ Основной штрихкод": "штрихкод",
    "Номенклатура": "наименование",
    "Номенклатура.Комплект": "комплект",
    "Номенклатура.Артикул": "артикул",
    "Номенклатура.БИТ Литера": "бит_литера",
    "Номенклатура.Категория": "категория",
    "Номенклатура.Материалы.Материал": "материал",
    "Номенклатура.Цвета.Цвет": "цвет",
    "Номенклатура.БИТ размер": "размер",
    "Номенклатура.Коллекция": "коллекция",
    "Номенклатура.Камни.Камень": "камень",
    "Склад": "склад",
    "Номенклатура.Входит в группу": "поставщик",
    "Остаток на складе": "остаток",
    # не унитарная цена, а стоимость ВСЕЙ строки (остаток * цена) — розничная_цена
    # считается делением на остаток в transform_increment, столбец сам в схему не входит
    "Стоимость ( в розничных ценах)": "стоимость_строки",
}

TARGET_COLUMNS = [
    "date",
    "дата_прихода",
    "штрихкод",
    "наименование",
    "комплект",
    "артикул",
    "бит_литера",
    "категория",
    "материал",
    "цвет",
    "размер",
    "коллекция",
    "камень",
    "розничная_цена",
    "магазин",
    "поставщик",
    "остаток",
]

TEXT_COLUMNS = [
    "наименование",
    "комплект",
    "артикул",
    "бит_литера",
    "категория",
    "материал",
    "цвет",
    "коллекция",
    "камень",
    "склад",
    "поставщик",
]

# заглушкой заполняются только те поля, где она уже стоит в накопленной
# истории; остальные (в т.ч. новые для схемы) остаются пустыми
FILL_NO_DATA_COLUMNS = ["артикул", "категория", "бит_литера", "коллекция"]

# значения-заглушки из 1С, неотличимые от пропуска
EMPTY_TOKENS = {"", "-", "--", "---", "<>", "не указан", "не указано", "нет", NO_DATA}

CATEGORY_MAP = {
    "Кольцо": "Кольца",
    "Цепь": "Цепи",
    "Подвеска": "Подвески",
    "Аксессуары для волос": "Аксессуары",
    "Аксессуар для волос": "Аксессуары",
    **dict.fromkeys(
        [
            "Кольца обручальные",
            "Кольцо обручальное",
            "Кольцообручальное",
            "Кольцаобручальные",
            "Кольцасвадьба",
        ],
        "Кольца свадьба",
    ),
    **dict.fromkeys(["Серьги фасонные", "Серги", "Серьга"], "Серьги"),
    **dict.fromkeys(["Посуда", "Сувениры, Подарки", "Другие"], "Другое"),
    "колье буква": "Колье",
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
    # приводим к написаниям, уже накопленным в целевом файле
    "Таиланд": "Коллекции Тайланд",
    "Детская коллекция": "Детская",
    "Другое. Помолвочное": "Помолвочные",
    "Остальное": "Другое",
}

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

# в выгрузке нет отдельного поля "Магазин" — магазин строится по коду склада
# ("Склад" вида "1 (Алмаз-Алматы)"). Собственные склады ("ВС (...)") в словаре
# намеренно отсутствуют: такие строки остаются без магазина (см. ниже).
# Словарь построен по фактическому справочнику складов 1С — при появлении
# нового магазина понадобится новая запись (см. предупреждение о неизвестном
# складе в transform_increment).
SKLAD_STORE_MAP = {
    "22 (Esentai - Алматы)": "Алматы Esentai Mall",
    "16 (Актау)": "Актау",
    "3 (Изумруд-Актобе)": "Актобе Изумруд",
    "1 (Mega-Алматы)": "Алматы Mega",
    "1 (Алмаз-Алматы)": "Алматы Алмаз",
    "2 (Гаухар-Алматы)": "Алматы Гаухар",
    "4 (Гаухар-Нур-Султан)": "Астана Гаухар",
    "19 (г.Нур-Султан Левый берег)": "Астана Левый берег",
    "5 (Янтарь-Жезказган)": "Жезказган Янтарь",
    "6 (Аметист-Караганда)": "Караганда Аметист",
    "7 (Рубин-Костанай)": "Костанай Рубин",
    "9 (Агат-Орал)": "Орал Агат",
    "8 (Янтарь-Оскемен)": "Оскемен Янтарь",
    "10 (Кристал-Павлодар)": "Павлодар Кристалл",
    "11 (Алмаз-Петропавловск)": "Петропавловск Алмаз",
    "12 (Изумруд-Семей)": "Семей Изумруд",
    "13 (Алмаз-Тараз)": "Тараз Алмаз",
    "14 (Алмаз-Шымкент)": "Шымкент Алмаз",
    "15 (Жемчуг-Экибастуз)": "Экибастуз Жемчуг",
}

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

RAW_OSTATKI_ID = config["PIPELINE_RAW"]["pipeline_raw_ostatki_id"]
OSTATKI_TARGET_ID = config["PIPELINE_PROCESSED"]["ostatki_target_id"]
PIPELINE_STATE_ID = config["PIPELINE_STATE"]["pipeline_state_id"]


def clean_text(value):
    """Убирает неразрывные пробелы и повторы, заглушки 1С приводит к NaN."""
    if pd.isna(value):
        return np.nan

    text = WHITESPACE_RE.sub(" ", str(value).replace("\xa0", " ")).strip()
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


def strip_litera(value):
    """Убирает литеру склада (СГ/ЗИ/ЮИ) из названия группы поставщика.

    Через re.sub, а не .str.replace(regex=True): в pandas 3 регулярки на
    pyarrow-строках исполняет RE2, где `\\b` работает только по ASCII и на
    кириллице не срабатывает вовсе.
    """
    if pd.isna(value):
        return value

    return LITERA_RE.sub("", str(value)).strip().upper()


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


def increment_label(ds):
    """Дата, под которой срез ложится в целевой файл.

    Недельный срез хранится под своей датой, срез на конец месяца — под первым
    днём этого месяца: он заменяет собой все недельные срезы месяца.
    """
    date = pd.Timestamp(ds)
    if date.is_month_end:
        return date.replace(day=1).strftime("%Y-%m-%d")

    return date.strftime("%Y-%m-%d")


def find_increment_files(service, date_strs):
    """Ищет в raw/ostatki эксель-файл для каждой даты из date_strs.

    Возвращает {дата: file_id}. Останавливает пайплайн, если хотя бы за одну
    дату нет папки или в папке нет эксель-файла.
    """
    date_folders = {
        item["name"]: item["id"]
        for item in list_children(service, RAW_OSTATKI_ID)
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
            "В raw/ostatki нет инкрементов за даты: %s. "
            "Пайплайн не допускает пропущенных дат — остановка без обновления.",
            ", ".join(missing),
        )
        sys.exit(1)

    return file_ids


def transform_increment(raw, label):
    """Приводит срез остатков к схеме целевого файла, проставляя date=label.

    Выгрузка всегда заканчивается служебной строкой "Итого" в первом столбце —
    отбрасываем её до приведения числовых колонок к типам.
    """
    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    first_column = raw.columns[0]
    is_totals_row = raw[first_column].astype(str).str.strip().str.lower().eq("итого")
    raw = raw[~is_totals_row].copy()

    if raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    incr = raw.rename(columns=COLUMN_RENAMES)

    # из выгрузки в модель идёт не все колонки
    incr = incr[[c for c in COLUMN_RENAMES.values() if c in incr.columns]].copy()

    for column in TEXT_COLUMNS:
        if column in incr.columns:
            incr[column] = incr[column].map(clean_text)

    barcode = pd.to_numeric(incr["штрихкод"], errors="coerce").astype("Int64")
    incr["штрихкод"] = barcode.astype(str).where(barcode.notna())

    # без штрихкода строка не соединяется ни с одной другой таблицей модели —
    # брак заполнения в 1С, как и в остальных источниках модели
    incr = incr.dropna(subset=["штрихкод"]).copy()

    if incr.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    incr["date"] = label

    incr["дата_прихода"] = pd.to_datetime(
        incr["дата_прихода"], format="%d.%m.%Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # магазин строится по коду склада, а не по отдельному полю (в выгрузке
    # его больше нет). "ВС (...)" — обособленный склад, а не торговая точка:
    # такие строки останутся без магазина, потому что их кода нет в словаре
    incr["магазин"] = incr["склад"].map(SKLAD_STORE_MAP)

    unmapped = (
        incr["склад"].notna()
        & incr["магазин"].isna()
        & ~incr["склад"].str.upper().str.startswith("ВС")
    )
    if unmapped.any():
        logger.warning(
            "Неизвестные коды склада — магазин не определён: %s",
            sorted(incr.loc[unmapped, "склад"].unique()),
        )

    incr["категория"] = incr["категория"].replace(CATEGORY_MAP).map(upper_first)
    incr["коллекция"] = incr["коллекция"].replace(COLLECTION_MAP).map(upper_first)

    incr["поставщик"] = incr["поставщик"].map(strip_litera).replace(SUPPLIER_MAP)
    incr["поставщик"] = incr["поставщик"].where(
        incr["поставщик"].isin(MAIN_SUPPLIERS), "Другие"
    )
    # порядок важен: skif_processor смотрит на уже нормализованного поставщика
    incr["артикул"] = incr["артикул"].apply(sku_processor)
    incr["артикул"] = incr.apply(skif_processor, axis=1)

    # цвет намеренно не приводится к мужскому роду (Желтое -> Желтый), как в
    # update_sales_sku.py: в накопленной истории остатков он лежит в исходном
    # написании, и нормализация только новых срезов разъехала бы справочник
    for column in ["бит_литера", "наименование", "материал", "цвет", "камень"]:
        incr[column] = incr[column].map(upper_first)

    ostatok = pd.to_numeric(incr["остаток"], errors="coerce")
    stoimost = pd.to_numeric(incr["стоимость_строки"], errors="coerce")

    # "Стоимость ( в розничных ценах)" — стоимость ВСЕЙ строки (остаток * цена
    # за единицу), а не цена за единицу; розничная_цена восстанавливается
    # делением. При остатке 0 (или отсутствующем) деление не выполняется
    incr["розничная_цена"] = (stoimost / ostatok.mask(ostatok == 0)).round(2)

    # Int64 (nullable), а не int: пустой остаток остаётся пустым, но целые
    # пишутся в CSV как "1", а не "1.0"
    incr["остаток"] = ostatok.round().astype("Int64")
    incr["размер"] = pd.to_numeric(incr["размер"], errors="coerce")

    for column in FILL_NO_DATA_COLUMNS:
        incr[column] = incr[column].fillna(NO_DATA)

    return incr.reindex(columns=TARGET_COLUMNS)


def merge_increment(target, incr, label, is_month_end):
    """Вливает срез в целевой файл, вытесняя то, что он собой заменяет.

    Недельный срез вытесняет строки со своей датой, срез на конец месяца —
    строки всех дат этого месяца (включая уже загруженные понедельники).
    """
    if is_month_end:
        replaced = target["date"].astype(str).str.startswith(label[:7])
    else:
        replaced = target["date"].astype(str).eq(label)

    logger.info(
        "Срез %s (%s): вытеснено строк из целевого файла %s, добавляется %s",
        label,
        "конец месяца" if is_month_end else "неделя",
        int(replaced.sum()),
        len(incr),
    )

    return pd.concat([target[~replaced], incr], ignore_index=True)


def update_ostatki(ds_run=None):
    """Вливает срезы остатков из raw/ostatki в Ostatki_target.csv.

    ds_run — дата запуска (строка YYYY-MM-DD); по умолчанию сегодня.
    Обрабатываются понедельники и концы месяцев до ds_run − 1 включительно
    и не дальше raw-watermark.
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
            "В pipeline_state.csv нет записи о загрузке сырых срезов "
            "source=%s, layer=%s. Сначала запустите raw/load_ostatki_ftp.py.",
            SOURCE,
            RAW_LAYER,
        )
        sys.exit(1)

    start_date = last_updated + pd.Timedelta(days=1)
    end_date = min(run_date, raw_updated)

    if start_date > end_date:
        logger.info(
            "Нет новых дат для обработки ostatki: последнее обновление %s, "
            "в raw загружено по %s, дата запуска %s. Остановка.",
            last_updated.date(),
            raw_updated.date(),
            run_date.date(),
        )
        sys.exit(0)

    dates = target_dates(start_date, end_date)
    if not dates:
        logger.info(
            "В диапазоне %s … %s нет понедельников и концов месяца. Остановка.",
            start_date.date(),
            end_date.date(),
        )
        sys.exit(0)

    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    logger.info(
        "Диапазон обработки ostatki (понедельники и концы месяцев): %s (%s шт.)",
        ", ".join(date_strs),
        len(date_strs),
    )

    # 1. Проверяем, что срезы есть за все даты диапазона, до их чтения
    file_ids = find_increment_files(service, date_strs)

    # 2. Читаем базовый файл
    target = pd.read_csv(
        download_bytes(service, OSTATKI_TARGET_ID),
        dtype={"штрихкод": str, "артикул": str, "date": str, "дата_прихода": str},
    )
    logger.info("Базовый файл Ostatki_target.csv: строк %s", len(target))

    missing_columns = [column for column in TARGET_COLUMNS if column not in target.columns]
    if missing_columns:
        logger.error(
            "В Ostatki_target.csv нет колонок: %s. "
            "Приведите базовый файл к целевой схеме и повторите запуск.",
            ", ".join(missing_columns),
        )
        sys.exit(1)

    # 3. Срезы обрабатываем по одному в хронологическом порядке: срез на конец
    #    месяца должен вытеснить понедельники этого месяца, в том числе
    #    добавленные на предыдущих шагах этого же прогона
    for ds in date_strs:
        raw = pd.read_excel(download_bytes(service, file_ids[ds]))
        label = increment_label(ds)
        incr = transform_increment(raw, label)
        logger.info(
            "Срез ostatki за %s: строк %s, остаток %s, магазинов %s",
            ds,
            len(incr),
            int(incr["остаток"].sum()) if len(incr) else 0,
            incr["магазин"].nunique(),
        )
        target = merge_increment(
            target, incr, label, is_month_end=pd.Timestamp(ds).is_month_end
        )

    target = target.sort_values("date").reset_index(drop=True)[TARGET_COLUMNS]

    # 4. Перезаписываем целевой файл на Google Drive
    out = io.BytesIO()
    target.to_csv(out, index=False)
    update_file_bytes(service, OSTATKI_TARGET_ID, out.getvalue(), "text/csv")
    logger.info(
        "Ostatki_target.csv перезаписан: строк всего %s, срезов %s (%s … %s)",
        len(target),
        target["date"].nunique(),
        target["date"].min(),
        target["date"].max(),
    )

    # 5. Двигаем watermark в pipeline_state до последней обработанной даты.
    #    Пишем дату среза, а не дату, под которой он лёг в файл: следующий
    #    прогон должен продолжить с неё, а не с начала месяца
    watermark = date_strs[-1]
    update_pipeline_state(service, PIPELINE_STATE_ID, SOURCE, LAYER, watermark)
    logger.info(
        "pipeline_state.csv обновлён: source=%s, layer=%s, updated_at=%s",
        SOURCE,
        LAYER,
        watermark,
    )
    logger.info("Готово: обновление ostatki за %s … %s выполнено", date_strs[0], date_strs[-1])


def parse_args():
    parser = argparse.ArgumentParser(
        description="Обновление Ostatki_target.csv срезами из raw/ostatki на Google Drive"
    )
    parser.add_argument(
        "--date",
        dest="ds_run",
        default=None,
        help=(
            "Дата запуска в формате YYYY-MM-DD (по умолчанию сегодня). "
            "Обрабатываются понедельники и концы месяцев до дня запуска включительно."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    update_ostatki(args.ds_run)
