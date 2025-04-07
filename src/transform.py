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


def transform_choice(df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(f"Transforming content of the CHOICE table...")
    df = df.copy()
    df["match_name_filtering"] = df["name"].isin(["Política de Assinatura", "Signature Policy"])
    logger.info(f"CHOICE table content transformed successfully.")
    return df


def transform_sequence(sequence_str, conn, course_id):
    if pd.isna(sequence_str) or sequence_str == "":  # Verifica se a sequência é vazia ou nula
        return sequence_str  # Retorna a sequência original (NULL ou vazio) sem transformação
    
    # Divida a sequência original em IDs antigos
    old_ids = sequence_str.split(",")
    new_ids = []  # Lista para armazenar os novos IDs

    # Buscar o mapeamento dos IDs antigos para novos do banco de dados
    old_to_new_ids = {}

    # Buscando o mapeamento para os course_modules (você pode otimizar isso dependendo do banco)
    result = conn.execute(text(f"""
        SELECT cm.id AS old_id, cm.module AS module_id
        FROM mdl_course_modules cm
        WHERE cm.course = :course_id
    """), {"course_id": course_id}).mappings().fetchall()

    # Gerar um dicionário de mapeamento de IDs antigos para novos
    for row in result:
        old_to_new_ids[row["old_id"]] = row["module_id"]

    # Agora, para cada ID antigo, buscamos o novo
    for old_id in old_ids:
        old_id = old_id.strip()  # Remove espaços extras ao redor do ID
        if not old_id:  # Ignora valores vazios (caso existam valores vazios na sequência)
            new_ids.append("")  # Adiciona uma string vazia para valores vazios
            continue
        
        # Aqui substituímos o old_id pelo novo ID usando o mapeamento
        new_id = old_to_new_ids.get(int(old_id), old_id)  # Caso não encontre o mapeamento, mantém o original
        new_ids.append(str(new_id))  # Adiciona o novo ID à lista

    # Retorna a nova sequência com os novos IDs
    return ",".join(new_ids)


def transform(dataframes):
    has_relevant_dataframe = "page" in dataframes or "choice" in dataframes
    if not has_relevant_dataframe:
        logger.info("No relevant dataframes to transform.")
        return dataframes

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
