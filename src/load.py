from typing import Dict, List
import pandas as pd
import os
import datetime


def dynamic_table_return(table: str, df: pd.DataFrame, columns: List[str]):
    try:
        print(f"✅ {table.upper()} return:\n{df[columns].head().to_string(index=False)}\n")
    except KeyError as e:
        print(f"☠️ Column not found in {table.upper()}: {e}\n")


def page_table(table: str, df: pd.DataFrame):
    print(f"✅ {table.upper()} return:\n{df[['id', 'name', 'course', 'content_link']].head().to_string(index=False)}\n")


def choice_table(table: str, df: pd.DataFrame):
    matching = df[df["match"]]
    not_matching = df[~df["match"]]
    print(f"✅ {table.upper()} rows matching 'Política de Assinatura' or 'Signature Policy':\n{matching[['id', 'name', 'course']].head().to_string(index=False)}\n")
    print(f"❌ There {'is' if len(not_matching) == 1 else 'are'} {len(not_matching)} row{'s' if len(not_matching) != 1 else ''} not matching.\n")


def get_unique_filename(output_dir: str) -> str:
    date_str = datetime.datetime.now().strftime("%m_%d_%Y")
    version = 1
    while True:
        filename = f"loaded_data_{date_str}_v{version}.xlsx"
        output_path = os.path.join(output_dir, filename)
        if not os.path.exists(output_path):
            return output_path
        version += 1


def load(dataframes: Dict[str, pd.DataFrame]):
    print(f"📰 Starting the loading process...\n\n")

    output_dir = "src/loaded"
    os.makedirs(output_dir, exist_ok=True)
    output_path = get_unique_filename(output_dir)
    
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for table, df in dataframes.items():
            if df.empty:
                print(f"⚠️ Warning: DataFrame {table.upper()} is empty.\n")
                continue

            print(f"✅ DataFrame {table.upper()} extracted successfully with {len(df)} rows.")

            if table == "course":
                dynamic_table_return(table, df, ['id', 'fullname'])
            elif table == "course_sections":
                dynamic_table_return(table, df, ['id', 'name', 'course'])
            elif table == "course_modules":
                dynamic_table_return(table, df, ['id', 'course', 'module', 'instance', 'section'])
            elif table == "modules":
                dynamic_table_return(table, df, ['id', 'name'])
            elif table == "feedback":
                dynamic_table_return(table, df, ['id', 'name', 'course'])
            elif table == "quiz":
                dynamic_table_return(table, df, ['id', 'name', 'course'])
            elif table == "forum":
                dynamic_table_return(table, df, ['id', 'name', 'course'])
            elif table == "reengagement":
                dynamic_table_return(table, df, ['id', 'name', 'course'])
            elif table == "url":
                dynamic_table_return(table, df, ['id', 'name', 'course', 'externalurl'])
            elif table == "page":
                page_table(table, df)
            elif table == "choice":
                choice_table(table, df)

            print(f"✅ DataFrame {table.upper()} has {len(df.columns)} columns: {df.columns.tolist()}.\n")

            df.to_excel(writer, sheet_name=table, index=False)
    print(f"📰 End of loading process\n\n")
