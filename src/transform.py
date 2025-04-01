import re
import html
import pandas as pd


def extract_href_or_src(html_text):  # if i need to return only htmls
    if not isinstance(html_text, str):
        return "Sem integração externa"
    
    # tenta primeiro o href
    match_href = re.search(r'href="([^"]+)"', html_text)
    if match_href:
        return html.unescape(match_href.group(1))
    
    # tenta o src se não encontrar href
    match_src = re.search(r'src="([^"]+)"', html_text)
    if match_src:
        return html.unescape(match_src.group(1))

    return "Sem integração externa"


def transform_page(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["content_link"] = df["content"].apply(extract_href_or_src)
    return df


def transform_choice(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["match"] = df["name"].isin(["Política de Assinatura", "Signature Policy"])
    return df
