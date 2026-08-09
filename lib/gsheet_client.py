"""Чтение листов Google Sheets в pandas.DataFrame через сервисный аккаунт."""

import gspread
from gspread_dataframe import get_as_dataframe


def read_sheet_as_dataframe(credentials_json, sheet_name, tab_name):
    """Читает лист tab_name таблицы sheet_name в DataFrame.

    Полностью пустые строки и столбцы отбрасываются.
    """
    gc = gspread.service_account(filename=str(credentials_json))
    sh = gc.open(sheet_name)
    wks = sh.worksheet(tab_name)

    df = get_as_dataframe(wks, evaluate_formulas=True)
    df = df.dropna(how="all").loc[:, df.notna().any()]
    return df
