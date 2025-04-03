import re
import html
import pandas as pd
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


def transform_choice(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(f"Transforming content of the CHOICE table...")
    df = df.copy()
    df["match"] = df["name"].isin(["Política de Assinatura", "Signature Policy"])
    logger.info(f"CHOICE table content transformed successfully.")
    return df


def transform(dataframes):
    logger.debug("Starting the transforming process...")

    if "page" in dataframes:
        try:
            dataframes["page"] = transform_page(dataframes["page"])
        except Exception as e:
            logger.error(f"Error transforming PAGE: {e}.")

    if "choice" in dataframes:
        try:
            dataframes["choice"] = transform_choice(dataframes["choice"])
        except Exception as e:
            logger.error(f"Error transforming CHOICE: {e}.")

    logger.info("End of transforming process.")
    return dataframes
