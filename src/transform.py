from typing import Dict
import re
import html
import pandas as pd
from sqlalchemy import text
from src.logging import start

logger = start()


def extract_href_or_src(html_text):  # if i need to return only htmls
    if not isinstance(html_text, str):
        return "No external integration."
    
    # look for 'href'
    match_href = re.search(r'href="([^"]+)"', html_text)
    if match_href:
        return html.unescape(match_href.group(1))
    
    # look for 'src'
    match_src = re.search(r'src="([^"]+)"', html_text)
    if match_src:
        return html.unescape(match_src.group(1))

    return "No external integration."


def transform_page(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(f"Transforming content of PAGE table...")
    df = df.copy()
    df["content_link"] = df["content"].apply(extract_href_or_src)
    logger.info(f"PAGE table content transformed successfully.")
    return df


def transform_sections(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    logger.debug("Filtering out specific sections to remove from course_sections...")
    sections_df = dataframes.get("course_sections", pd.DataFrame())
    modules_df = dataframes.get("course_modules", pd.DataFrame())
    cfo_df = dataframes.get("course_format_options", pd.DataFrame())

    if sections_df.empty or modules_df.empty:
        logger.warning("course_sections or course_modules is empty. Skipping transform_sections.")
        return dataframes

    to_remove_sections = sections_df[sections_df["name"].isin(["Avaliação Inicial", "Initial Assessment", "Avaliação das Atividades Práticas", "Avaliação das Atividades Prática", "Practical Activities Assessment", "Evaluation of Practical Activities"])].copy()
    if to_remove_sections.empty:
        logger.info("No sections found for removal.")
        return dataframes
    logger.info(f"Found {len(to_remove_sections)} sections to remove.")

    module_ids_to_remove = set()
    for sequence in to_remove_sections["sequence"].dropna():
        module_ids = str(sequence).split(",")
        module_ids_to_remove.update(int(mid) for mid in module_ids if mid.isdigit())
    logger.info(f"Identified {len(module_ids_to_remove)} module(s) to remove based on removed sections.")

    dataframes["course_sections"] = sections_df[~sections_df["name"].isin(["Avaliação Inicial", "Initial Assessment", "Avaliação das Atividades Práticas", "Avaliação das Atividades Prática", "Practical Activities Assessment", "Evaluation of Practical Activities"])].copy()
    dataframes["course_modules"] = modules_df[~modules_df["id"].isin(module_ids_to_remove)].copy()

    if not cfo_df.empty:
        section_ids_to_remove = to_remove_sections["id"].tolist()
        before_count = len(cfo_df)
        dataframes["course_format_options"] = cfo_df[~cfo_df["sectionid"].isin(section_ids_to_remove)].copy()
        after_count = len(dataframes["course_format_options"])
        logger.info(f"Removed {before_count - after_count} course_format_options related to removed sections.")
    logger.info("Sections and related course_modules removed successfully.")

    return dataframes


def transform_sequence(sequence_str, map, hvp_ids=None):
    if pd.isna(sequence_str) or sequence_str == "":
        return sequence_str
    
    old_ids = sequence_str.split(",")
    new_ids = []
    for old_id in old_ids:
        old_id = old_id.strip()
        if not old_id:
            continue

        int_old_id = int(old_id)

        if hvp_ids and int_old_id in hvp_ids:
            continue

        new_id = map.get(int_old_id, old_id)
        new_ids.append(str(new_id))
    return ",".join(new_ids)


def transform_quiz(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(f"Removing unwanted quizzes from QUIZ table...")
    df = df.copy()
    before = len(df)
    df = df[~df["name"].isin(["Avaliação Inicial", "Initial Assessment", "Avaliação das Atividades Práticas", "Avaliação das Atividades Prática", "Practical Activities Assessment", "Evaluation of Practical Activities"])]
    after = len(df)
    logger.info(f"Removed {before - after} unwanted quiz entries.")
    return df


def transform_reengagement(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(f"Removing 'Alerta de Início do Curso' or 'Course Start Alert' from REENGAGEMENT table...")
    df = df.copy()
    before = len(df)
    df = df[~df["name"].isin(["Alerta de Início do Curso", "Course Start Alert"])]
    after = len(df)
    logger.info(f"Removed {before - after} reengagement entries named 'Alerta de Início do Curso' or 'Course Start Alert'.")
    return df


def transform(dataframes):
    has_relevant_dataframe = "page" in dataframes or "choice" in dataframes or "quiz" in dataframes
    if not has_relevant_dataframe:
        logger.info("No relevant dataframes to transform.")
        return dataframes
    logger.debug(f"-------------------- Starting the transforming process... --------------------")

    if "page" in dataframes:
        try:
            dataframes["page"] = transform_page(dataframes["page"])
        except Exception as e:
            logger.error(f"Error transforming PAGE: {e}.")
        
    dataframes = transform_sections(dataframes)

    if "quiz" in dataframes:
        try:
            dataframes["quiz"] = transform_quiz(dataframes["quiz"])
        except Exception as e:
            logger.error(f"Error transforming QUIZ: {e}")
    
    if "reengagement" in dataframes:
        try:
            dataframes["reengagement"] = transform_reengagement(dataframes["reengagement"])
        except Exception as e:
            logger.error(f"Error transforming REENGAGEMENT: {e}")

    logger.info(f"-------------------- End of transforming process. --------------------")

    return dataframes
