import datetime
import glob
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from pikepdf import Pdf
from tabula.io import read_pdf


def is_valid_date(value):
    if value is None:
        return False
    val_type = type(value)
    if val_type is str:
        if len(value) == 5 or len(value) == 6:
            return True
        else:
            return False
    if isinstance(value, datetime):
        return True
    return False


def is_valid_amount(value):
    if value is None:
        return False
    val_type = type(value)
    if val_type is float:
        return True
    elif val_type is str:
        try:
            float(value)
            return True
        except ValueError:
            return False
    else:
        return False


def extract_year_from_filepath(filepath: str) -> str | None:
    path = Path(filepath)
    # filename = path.name
    filename_without_extension = path.stem.strip()
    possible_statement_date = filename_without_extension[-11:].strip().upper()
    try:
        statement_date = datetime.strptime(possible_statement_date, "%b_%d-%Y")
        return statement_date
    except Exception as error:
        print(error)
        return None


def is_valid_row(row: pd.core.series.Series):
    return (
        is_valid_date(row["TransactionDate"])
        and is_valid_date(row["PostingDate"])
        and is_valid_amount(row["Amount"])
    )


def transform_date_in_df(value: str, statement_date: datetime) -> datetime | None:
    format_string = "%b %d %Y"
    to_return = datetime.strptime(
        value.strip() + " " + str(statement_date.year), format_string
    )
    if to_return > statement_date:
        to_return = datetime.strptime(
            value.strip() + " " + str(statement_date.year - 1), format_string
        )
        return to_return
    else:
        return to_return


def transform_data_frame(x: pd.DataFrame, filepath: str, source: str):
    x.columns = ["TransactionDate", "PostingDate", "Description", "Amount"]
    x["Amount"] = x["Amount"].replace("[\\$,]", "", regex=True)
    x["IsValidRow"] = x.apply(is_valid_row, axis=1)
    x = x[x["IsValidRow"] == True].drop("IsValidRow", axis=1)  # noqa: E712
    x["Amount"] = x["Amount"].astype(float, errors="raise")
    statement_date = extract_year_from_filepath(filepath)
    x["TransactionDate"] = x["TransactionDate"].apply(
        lambda value: transform_date_in_df(value, statement_date)
    )
    x["PostingDate"] = x["PostingDate"].apply(
        lambda value: transform_date_in_df(value, statement_date)
    )
    x["StatementFile"] = Path(filepath).name
    x.insert(0, "Source", source)
    return x


def generate_csv_for_statement(pdf_file: str, source: str):
    pdf = Pdf.open(pdf_file)
    num_pages = len(pdf.pages)
    print("Processing file {pdf_file}".format(pdf_file=pdf_file))
    first_dfs = read_pdf(
        pdf_file,
        stream=True,
        area=[200, 45, 525, 350],
        pandas_options={"dtype": str, "header": None},
        columns=[94, 125, 309, 350],
        guess=False,
        pages=1,
        java_options=[
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
            "-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog",
        ],
    )
    rest_dfs = read_pdf(
        pdf_file,
        stream=True,
        area=[175, 45, 745, 350],
        pandas_options={"dtype": str, "header": None},
        columns=[94, 125, 309, 350],
        guess=False,
        pages="3-{last_page}".format(last_page=num_pages),
        java_options=[
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
            "-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog",
        ],
    )
    all_dfs = first_dfs + rest_dfs
    # all_dfs = first_dfs
    to_return = pd.concat([transform_data_frame(x, pdf_file, source) for x in all_dfs])
    return to_return


files_info: dict[str, str] = {
    "Chandu-TD-CC": "/mnt/projects/self/bank/chandu-td-cc/statements/credit/chandu-td",
    "Siri-TD-CC": "/mnt/projects/self/bank/chandu-td-cc/statements/credit/siri-td",
}


def generate_csv_from_statements_of_source(
    source: str, files_path: str
) -> pd.DataFrame:
    pdf_files = glob.glob("{base_path}/*.pdf".format(base_path=files_path))
    # pdf_files = [pdf_files[1]]
    csv_rows_df = pd.concat(
        [
            x
            for x in [
                generate_csv_for_statement(pdf_file, source) for pdf_file in pdf_files
            ]
        ]
    )
    return csv_rows_df


def run():
    all_rows_df = pd.concat(
        [
            generate_csv_from_statements_of_source(source, files_path)
            for source, files_path in files_info.items()
        ]
    )
    # with pd.option_context(
    #     "display.max_rows", None, "display.max_columns", None
    # ):  # more options can be specified also
    #     print(all_rows_df)
    output_folder = os.path.join(os.getcwd(), ".generated")
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    output_file_name = os.path.join(
        output_folder,
        "TD-Statement-Data-Extracted-{id}.csv".format(
            id=datetime.now().strftime("%m-%d-%Y-%H-%M-%S")
        ),
    )
    all_rows_df.to_csv(output_file_name, index=False)


run()
