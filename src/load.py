from typing import Dict, List
import pandas as pd
import os
from src.logging import start
from sqlalchemy import text, bindparam
from src.transform import transform_sequence
from datetime import datetime
from ftplib import FTP
from dotenv import load_dotenv
from PIL import Image
import easyocr
import cv2

load_dotenv()
logger = start()
timestamp = int(datetime.now().timestamp())


def insert_and_mapping(conn, loop_id, new_course_id, instance_name, mapping, df, table,
                       param_1=None, param_2=None, param_3=None, param_4=None, param_5=None, param_6=None, param_7=None, param_8=None, param_9=None, param_10=None):
    if not df.empty:
        df_filtered = df[df[param_3] == param_1].copy()
        if not df_filtered.empty:
            old_ids_list = df_filtered["id"].tolist()
            df_filtered[param_3] = param_2
            df_filtered = df_filtered.drop(columns=["id"])
            if param_10 in df_filtered.columns:
                df_filtered = df_filtered.drop(columns=[param_10])
            try:
                df_filtered.to_sql(table, conn, if_exists="append", index=False)
                result = conn.execute(text(f"SELECT id FROM {table} WHERE {param_3} = :param ORDER BY id"), {"param": param_2}).fetchall()
                new_ids_list = [row[0] for row in result]
                mapping.update(dict(zip(old_ids_list, new_ids_list)))
                if param_9 is not None and not param_9.empty:
                    df_filtered_2 = param_9[param_9[param_8].isin(old_ids_list)].copy()
                    if not df_filtered_2.empty:
                        df_filtered_2 = df_filtered_2.drop(columns=["id"])
                        try:
                            df_filtered_2[param_8] = df_filtered_2[param_8].map(mapping)
                            df_filtered_2.to_sql(param_7, conn, if_exists="append", index=False)
                            logger.info(f"{len(df_filtered_2)} {param_6}(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting {param_6.upper()} for course {new_course_id}: {e}")
                logger.info(f"{len(df_filtered)} {instance_name}(s) inserted for course {new_course_id}.")
            except Exception as e:
                logger.error(f"Error inserting {instance_name.upper()} for course {new_course_id}: {e}")
        else:
            logger.warning(f"No {instance_name.upper()} entries found for course {loop_id}.")


def create_feedback_instance_df(new_course_id, course_shortname):
    english = course_shortname.upper().startswith("EN") or course_shortname.upper().endswith("EN")
    df = pd.DataFrame([{
        "course": new_course_id,
        "name": "Pesquisa de Satisfação" if not english else "Satisfaction Feedback",
        "introformat": 1,
        "anonymous": 2,
        "email_notification": 0,
        "autonumbering": 0,
        "page_after_submitformat": 1,
        "timemodified": timestamp,
        "completionsubmit": 1
    }])
    return df


def create_feedback_items_df(feedback_id, items_df):
    items_df["feedback"] = feedback_id
    items_df["template"] = items_df["template"].iloc[0]
    items_df = items_df.drop(columns=["id"])
    return items_df


def create_page_ex_resource_df(new_course_id, element_name):
    df = pd.DataFrame([{
        "course": new_course_id,
        "name": f"EX-ARQUIVO(resource: {element_name}) - PRECISA EDITAR",
        "introformat": 1,
        "content": """
                    <div
                    style="position: relative; width: 100%; height: 0; padding-top: 50.0000%; padding-bottom: 0; box-shadow: 0 2px 8px 0 rgba(63,69,81,0.16); margin-top: 1.6em; margin-bottom: 0.9em; overflow: hidden; border-radius: 8px; will-change: transform;">
                    <iframe
                        style="position: absolute; width: 100%; height: 100%; top: 0; left: 0; border: none; padding: 0; margin: 0;"
                        src="LINK DO CANVA (exemplo) -> https://www.canva.com/design/DAGeDjwbY1s/hf9o_XUQejYP03e1A12RLw/view?embed"
                        allow="fullscreen" allowfullscreen="allowfullscreen" loading="lazy">
                    </iframe></div>
                   """,
        "contentformat": 1,
        "display": 5,
        "displayoptions": 'a:2:{s:10:"printintro";s:1:"0";s:17:"printlastmodified";s:1:"1";}',
        "revision": 0,
        "timemodified": timestamp
    }])
    return df


def create_course_customfield_data_df(new_course_id, new_course_context_id, customfield_data_df, image_text=None):
    fieldid_mapping_old_to_new = {8: 7, 1: 16, 2: 17, 3: 18, 4: 19, 5: 20, 6: 21, 7: 22}
    valid_old_field_ids = set(fieldid_mapping_old_to_new.keys())
    filtered_df = customfield_data_df[customfield_data_df["fieldid"].isin(valid_old_field_ids)].copy()

    if filtered_df.empty:
        logger.warning(f"No customfield_data found for course {new_course_id}.")
        filtered_df = pd.DataFrame(columns=['fieldid', 'instanceid', 'intvalue', 'decvalue', 'shortcharvalue', 'charvalue',
                                            'value', 'valueformat', 'valuetrust', 'timecreated', 'timemodified', 'contextid'])
    else:
        filtered_df["fieldid"] = filtered_df["fieldid"].map(fieldid_mapping_old_to_new)
        filtered_df["instanceid"] = new_course_id
        filtered_df["contextid"] = new_course_context_id
        filtered_df["timecreated"] = timestamp
        filtered_df["timemodified"] = timestamp
        filtered_df["valuetrust"] = filtered_df["fieldid"].apply(lambda fid: 0 if fid in [7, 22] else 1)

    if image_text:
        new_row = {
            'fieldid': 14,
            'instanceid': new_course_id,
            'intvalue': None,
            'decvalue': None,
            'shortcharvalue': None,
            'charvalue': None,
            'value': image_text,
            'valueformat': 0,
            'valuetrust': 1,
            'timecreated': timestamp,
            'timemodified': timestamp,
            'contextid': new_course_context_id
        }
        new_row_df = pd.DataFrame([new_row])
        if filtered_df.empty:
            filtered_df = pd.DataFrame(columns=new_row_df.columns).astype(new_row_df.dtypes.to_dict())
        new_row_df = new_row_df.astype(filtered_df.dtypes.to_dict())
        filtered_df = pd.concat([filtered_df, new_row_df], ignore_index=True)

    final_columns = ['fieldid', 'instanceid', 'intvalue', 'decvalue', 'shortcharvalue', 'charvalue',
                     'value', 'valueformat', 'valuetrust', 'timecreated', 'timemodified', 'contextid']

    return filtered_df[final_columns]


def download_from_ftp(contenthash, course_shortname, course_original_id):
    local_dir = "src/customcert_images"
    os.makedirs(local_dir, exist_ok=True)
    folder1 = contenthash[:2]
    folder2 = contenthash[2:4]
    ftp = FTP()
    ftp.connect(os.getenv("FTP_HOST"), 21)
    ftp.login(user=os.getenv("FTP_USER"), passwd=os.getenv("FTP_PASSWORD"))
    base_dir = os.getenv("FTP_BASE_DIR")
    ftp.cwd(f"{base_dir}/{folder1}/{folder2}")
    local_filename = f"conteudo_programatico_{course_shortname}.jpeg"
    local_path = os.path.join(local_dir, local_filename)
    temp_path = os.path.join(local_dir, f"{contenthash}_temp")
    image_text = None
    try:
        with open(temp_path, "wb") as temp_file:
            ftp.retrbinary(f"RETR {contenthash}", temp_file.write)
        with Image.open(temp_path) as img:
            if img.mode == "RGBA":
                background = Image.new("RGBA", img.size, (255, 255, 255))
                img = Image.alpha_composite(background, img).convert("RGB")
            else:
                img = img.convert("RGB")
            img.save(local_path, "JPEG", quality=95)
        logger.info(f"File {local_filename} downloaded and converted successfully to {local_dir}.")
        image_text = extract_text_from_image(os.path.abspath(local_path), course_original_id)
    except Exception as e:
        logger.error(f"Error downloading file {local_filename}: {e}")
    finally:
        ftp.quit()
        if os.path.exists(temp_path):
            os.remove(temp_path)
        logger.info(f"File {local_filename} downloaded successfully to {local_dir}.")
    return image_text


def extract_text_from_image(image_path, course_original_id):
    reader = easyocr.Reader(['pt', 'en'], gpu=True)
    left_img, right_img = split_image(image_path)
    left_text, left_conf = text_extract(left_img, reader)
    right_text, right_conf = text_extract(right_img, reader)
    full_text = f"{left_text}\n\n\n{right_text}"
    full_conf = (left_conf + right_conf) / 2
    base = os.path.splitext(os.path.basename(image_path))[0]
    output_dir = os.path.join("src", "customcert_texts", base)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{course_original_id}_{base}.txt")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(full_text)
    for temp_img in (left_img, right_img):
        try:
            if os.path.exists(temp_img):
                os.remove(temp_img)
                logger.info(f"Temporary file deleted: {temp_img}")
        except Exception as e:
            logger.error(f"Error deleting temporary image {temp_img}: {e}")
    logger.info(f"Extracted text (avg confidence {full_conf:.2f}) saved to: {output_path}")
    return full_text


def split_image(path):
    img = cv2.imread(path)
    h, w = img.shape[:2]
    middle = w // 2
    ext = os.path.splitext(path)[1]
    left_path = path.replace(ext, f"_left{ext}")
    right_path = path.replace(ext, f"_right{ext}")
    cv2.imwrite(left_path, img[:, :middle])
    cv2.imwrite(right_path, img[:, middle:])
    return left_path, right_path


def text_extract(path, reader):
    result = reader.readtext(path, paragraph=False)
    texts, confidences = [], []
    for _, text, conf in result:
        text = text.strip()
        if text:
            texts.append(text)
            confidences.append(conf)
    content = "\n".join(texts)
    avg_conf = sum(confidences) / len(confidences) if confidences else 0
    return content, avg_conf


def create_customcert_instance_df(new_course_id, course_shortname):
    default_customcert_df_PTBR = pd.DataFrame([{
        'course': new_course_id,
        'templateid': 0,
        'name': "Certificado de Conclusão",
        'intro': """
                    <p dir="ltr" id="yui_3_17_2_1_1729087267826_1221"><strong>Aumente suas chances no mercado de trabalho adicionando seu certificado no LinkedIn através do botão:</strong><br><a id="yui_3_17_2_1_1729087267826_1238" href="https://www.linkedin.com/profile/add?startTask=CERTIFICATION_NAME&amp;name={coursename}&amp;organizationId=71701836&amp;issueYear={siteyear}&amp;certUrl=https://talisma.seg.br&amp;certId={userid}/{courseidnumber}" target="_blank" rel="noopener"><img id="yui_3_17_2_1_1729087267826_1239" src="https://download.linkedin.com/desktop/add2profile/buttons/pt_BR.png" alt="Botão do LinkedIn para Adicionar ao Perfil"></a></p>
                 """,
        'introformat': 1,
        'requiredtime': 1,
        'verifyany': 1,
        'deliveryoption': 'I',
        'emailstudents': 1,
        'emailteachers': 0,
        'emailothers': 'certificado@talisma.seg.br',
        'protection': 'modify',
        'timecreated': timestamp,
        'timemodified': timestamp
    }])

    default_customcert_df_EN = pd.DataFrame([{
        'course': new_course_id,
        'templateid': 0,
        'name': "Certificate of Completion",
        'intro': """
                    <p dir="ltr" id="yui_3_17_2_1_1729087267826_1221"><strong>Increase your chances in the job market by adding your certificate to LinkedIn through the button:</strong><br><a id="yui_3_17_2_1_1729087267826_1238" href="https://www.linkedin.com/profile/add?startTask=CERTIFICATION_NAME&amp;name={coursename}&amp;organizationId=71701836&amp;issueYear={siteyear}&amp;certUrl=https://talisma.seg.br&amp;certId={userid}/{courseidnumber}" target="_blank" rel="noopener"> <img id="yui_3_17_2_1_1729087267826_1239" src="https://download.linkedin.com/desktop/add2profile/buttons/pt_BR.png" alt="LinkedIn Button to Add to Profile"> </a></p>
                 """,
        'introformat': 1,
        'requiredtime': 1,
        'verifyany': 1,
        'deliveryoption': 'I',
        'emailstudents': 1,
        'emailteachers': 0,
        'emailothers': 'certificado@talisma.seg.br',
        'protection': 'modify',
        'timecreated': timestamp,
        'timemodified': timestamp
    }])

    upper_shortname = course_shortname.upper()
    if upper_shortname.startswith("EN") or upper_shortname.endswith("EN"):
        return default_customcert_df_EN
    return default_customcert_df_PTBR


def create_customcert_template_df(df, contextid, course_shortname):
    df["name"] = f"Template {course_shortname}"
    df["contextid"] = contextid
    df["timecreated"] = timestamp
    df["timemodified"] = timestamp
    df = df.drop(columns=["id"])
    return df


def create_customcert_page_df(df, templateid):
    df["templateid"] = templateid
    df["timecreated"] = timestamp
    df["timemodified"] = timestamp
    df = df.drop(columns=["id"])
    return df


def create_customcert_elements_df(df, pageids):
    data = []
    unique_pageids = df["pageid"].unique()
    mapping = dict(zip(unique_pageids, pageids))
    for unique_pageid, pageid in mapping.items():
        df_copy = df[df["pageid"] == unique_pageid].copy()
        df_copy["pageid"] = pageid
        df_copy["timecreated"] = timestamp
        df_copy["timemodified"] = timestamp
        df_copy = df_copy.drop(columns=["id"])
        data.append(df_copy)
    return pd.concat(data, ignore_index=True)


def if_table_course(conn, table: str, ids: List[int], dataframes: Dict[str, pd.DataFrame], category: int = 1, new_db: str = ''):
    course_table = f"{new_db.prefix}_{table}"
    context_table = f"{new_db.prefix}_context"
    sections_table = f"{new_db.prefix}_course_sections"
    modules_table = f"{new_db.prefix}_modules"
    course_modules_table = f"{new_db.prefix}_course_modules"
    course_format_options_table = f"{new_db.prefix}_course_format_options"
    page_table = f"{new_db.prefix}_page"
    label_table = f"{new_db.prefix}_label"
    url_table = f"{new_db.prefix}_url"
    enrol_table = f"{new_db.prefix}_enrol"
    quiz_table = f"{new_db.prefix}_quiz"
    forum_table = f"{new_db.prefix}_forum"
    reengagement_table = f"{new_db.prefix}_reengagement"
    choice_table = f"{new_db.prefix}_choice"
    choice_options_table = f"{new_db.prefix}_choice_options"
    cc_table = f"{new_db.prefix}_customcert"
    cc_templates_table = f"{new_db.prefix}_customcert_templates"
    cc_elements_table = f"{new_db.prefix}_customcert_elements"
    cc_pages_table = f"{new_db.prefix}_customcert_pages"
    customfield_data_table = f"{new_db.prefix}_customfield_data"
    question_categories_table = f"{new_db.prefix}_question_categories"
    question_table = f"{new_db.prefix}_question"
    question_answers_table = f"{new_db.prefix}_question_answers"
    quiz_slots_table = f"{new_db.prefix}_quiz_slots"
    quiz_sections_table = f"{new_db.prefix}_quiz_sections"
    question_bank_entries_table = f"{new_db.prefix}_question_bank_entries"
    question_versions_table = f"{new_db.prefix}_question_versions"
    question_references_table = f"{new_db.prefix}_question_references"
    qtype_ddimageortext_table = f"{new_db.prefix}_qtype_ddimageortext"
    qtype_ddimageortext_drags_table = f"{new_db.prefix}_qtype_ddimageortext_drags"
    qtype_ddimageortext_drops_table = f"{new_db.prefix}_qtype_ddimageortext_drops"
    qtype_ddmarker_table = f"{new_db.prefix}_qtype_ddmarker"
    qtype_ddmarker_drags_table = f"{new_db.prefix}_qtype_ddmarker_drags"
    qtype_ddmarker_drops_table = f"{new_db.prefix}_qtype_ddmarker_drops"
    qtype_essay_options_table = f"{new_db.prefix}_qtype_essay_options"
    qtype_match_options_table = f"{new_db.prefix}_qtype_match_options"
    qtype_match_subquestions_table = f"{new_db.prefix}_qtype_match_subquestions"
    qtype_multichoice_options_table = f"{new_db.prefix}_qtype_multichoice_options"
    qtype_randomsamatch_options_table = f"{new_db.prefix}_qtype_randomsamatch_options"
    qtype_shortanswer_options_table = f"{new_db.prefix}_qtype_shortanswer_options"
    question_ddwtos_table = f"{new_db.prefix}_question_ddwtos"
    question_gapselect_table = f"{new_db.prefix}_question_gapselect"
    question_truefalse_table = f"{new_db.prefix}_question_truefalse"
    feedback_table = f"{new_db.prefix}_feedback"
    feedback_item_table = f"{new_db.prefix}_feedback_item"
    
    module_instance_mapping = {}

    def insert_question_type(qtype_name_as_string, qtype_df, qtype_table, question_mapping, new_course_id, option_column=None):
        if "questionid" in qtype_df.columns:
            column_placeholder = "questionid"
        elif "question" in qtype_df.columns:
            column_placeholder = "question"
        else:
            logger.warning(f"{qtype_name_as_string} skipped: no 'questionid' or 'question' column found.")
            return
        qtype_filtered = qtype_df[qtype_df[column_placeholder].isin(question_mapping.keys())].copy()
        if not qtype_filtered.empty:
            qtype_filtered[column_placeholder] = qtype_filtered[column_placeholder].map(question_mapping)
            if option_column != None:
                qtype_filtered[option_column] = 0
            qtype_filtered = qtype_filtered.drop(columns=["id"])
            try:
                qtype_filtered.to_sql(qtype_table, conn, if_exists="append", index=False)
                logger.info(f"{len(qtype_filtered)} {qtype_name_as_string}(s) inserted for course {new_course_id}.")
            except Exception as e:
                logger.error(f"Error inserting {qtype_name_as_string.upper()} for course {new_course_id}: {e}")

    for id in ids:
        course_df = dataframes.get("course", pd.DataFrame())
        sections_df = dataframes.get("course_sections", pd.DataFrame())
        course_sections_df = sections_df[sections_df["course"] == id].copy()
        modules_df = dataframes.get("modules", pd.DataFrame())
        course_modules_df = dataframes.get("course_modules", pd.DataFrame())
        old_modules_map = dict(zip(modules_df["id"], modules_df["name"]))
        old_modules_result = conn.execute(text(f"SELECT id, name FROM {modules_table}")).mappings()
        new_modules_map = {row["name"]: row["id"] for row in old_modules_result}
        course_modules_filtered_df = course_modules_df[course_modules_df["course"] == id].copy()
        course_format_options_df = dataframes.get("course_format_options", pd.DataFrame())
        course_format_options_filtered = course_format_options_df[course_format_options_df["courseid"] == id].copy()
        page_df = dataframes.get("page", pd.DataFrame())
        label_df = dataframes.get("label", pd.DataFrame())
        url_df = dataframes.get("url", pd.DataFrame())
        enrol_df = dataframes.get("enrol", pd.DataFrame())
        resource_df = dataframes.get("resource", pd.DataFrame())
        quiz_df = dataframes.get("quiz", pd.DataFrame())
        forum_df = dataframes.get("forum", pd.DataFrame())
        reengagement_df = dataframes.get("reengagement", pd.DataFrame())
        choice_df = dataframes.get("choice", pd.DataFrame())
        choice_options_df = dataframes.get("choice_options", pd.DataFrame())
        feedback_item_ptbr_df = dataframes.get("feedback_item_ptbr", pd.DataFrame())
        feedback_item_en_df = dataframes.get("feedback_item_en", pd.DataFrame())
        cc_templates_br_df = dataframes.get("customcert_templates_ptbr", pd.DataFrame())
        cc_pages_br_df = dataframes.get("customcert_pages_ptbr", pd.DataFrame())
        cc_elements_br_df = dataframes.get("customcert_elements_ptbr", pd.DataFrame())
        cc_templates_en_df = dataframes.get("customcert_templates_en", pd.DataFrame())
        cc_pages_en_df = dataframes.get("customcert_pages_en", pd.DataFrame())
        cc_elements_en_df = dataframes.get("customcert_elements_en", pd.DataFrame())
        customfield_data_old_df = dataframes.get("customfield_data", pd.DataFrame())
        customcert_image_hash_info_df = dataframes.get("customcert_image_hash_info", pd.DataFrame())
        question_categories_df = dataframes.get("question_categories", pd.DataFrame())
        question_df = dataframes.get("question", pd.DataFrame())
        question_answers_df = dataframes.get("question_answers", pd.DataFrame())
        quiz_slots_df = dataframes.get("quiz_slots", pd.DataFrame())
        courses_context_df = dataframes.get("context_course", pd.DataFrame())
        quiz_sections_df = dataframes.get("quiz_sections", pd.DataFrame())
        qtype_ddimageortext_df = dataframes.get("qtype_ddimageortext", pd.DataFrame())
        qtype_ddimageortext_drags_df = dataframes.get("qtype_ddimageortext_drags", pd.DataFrame())
        qtype_ddimageortext_drops_df = dataframes.get("qtype_ddimageortext_drops", pd.DataFrame())
        qtype_ddmarker_df = dataframes.get("qtype_ddmarker", pd.DataFrame())
        qtype_ddmarker_drags_df = dataframes.get("qtype_ddmarker_drags", pd.DataFrame())
        qtype_ddmarker_drops_df = dataframes.get("qtype_ddmarker_drops", pd.DataFrame())
        qtype_essay_options_df = dataframes.get("qtype_essay_options", pd.DataFrame())
        qtype_match_options_df = dataframes.get("qtype_match_options", pd.DataFrame())
        qtype_match_subquestions_df = dataframes.get("qtype_match_subquestions", pd.DataFrame())
        qtype_multichoice_options_df = dataframes.get("qtype_multichoice_options", pd.DataFrame())
        qtype_randomsamatch_options_df = dataframes.get("qtype_randomsamatch_options", pd.DataFrame())
        qtype_shortanswer_options_df = dataframes.get("qtype_shortanswer_options", pd.DataFrame())
        question_ddwtos_df = dataframes.get("question_ddwtos", pd.DataFrame())
        question_gapselect_df = dataframes.get("question_gapselect", pd.DataFrame())
        question_truefalse_df = dataframes.get("question_truefalse", pd.DataFrame())

        course = course_df[course_df["id"] == id]
        if course.empty:
            logger.warning(f"No row(s) found in 'COURSE' with id {id}.")
        else:
            try:
                course_copy = course.copy()
                course_old_context_id = courses_context_df[courses_context_df["instanceid"] == id]["id"].iloc[0]
                cc_image_hash_df = customcert_image_hash_info_df[customcert_image_hash_info_df["course_id"] == id].copy()
                course_shortname = course_copy["shortname"].iloc[0]
                image_text = None
                if not cc_image_hash_df.empty:
                    contenthash = cc_image_hash_df["contenthash"].iloc[0]
                    course_original_id = course_copy["id"].iloc[0]
                    image_text = download_from_ftp(contenthash, course_shortname, course_original_id)
                course_copy["category"] = category
                course_copy = course_copy.drop(columns=["id", "originalcourseid"])

                # course
                course_copy.to_sql(course_table, conn, if_exists="append", index=False)
                new_course = conn.execute(text(f"SELECT id FROM {course_table} ORDER BY id DESC LIMIT 1"))
                new_course_id = new_course.scalar()
                logger.info(f"NEW COURSE inserted successfully! OLD COURSE ID: {id} | NEW COURSE ID: {new_course_id}")

                # category
                category_id = course_copy["category"].iloc[0]
                result = conn.execute(text(
                    f"""
                    SELECT id FROM {context_table}
                    WHERE contextlevel = 40 AND instanceid = {category_id}
                    LIMIT 1
                    """
                ))
                context_category_id = result.scalar()

                # create course context (contextlevel 50)
                conn.execute(text(f"INSERT INTO {context_table} (contextlevel, instanceid, depth, path) VALUES (50, {new_course_id}, 3, NULL)"))
                result = conn.execute(text(f"SELECT id FROM {context_table} WHERE contextlevel = 50 AND instanceid = {new_course_id} ORDER BY id DESC LIMIT 1"))
                new_course_context_id = result.scalar()
                logger.info(f"NEW COURSE CONTEXT ID inserted successfully! NEW COURSE CONTEXT ID: {new_course_context_id}")
                cf_data_df = customfield_data_old_df[customfield_data_old_df["instanceid"] == id].copy()
                cf_data_df = cf_data_df.drop(columns=["id"])
                customfield_data_df = create_course_customfield_data_df(new_course_id, new_course_context_id, cf_data_df, image_text)
                if not customfield_data_df.empty:
                    try:
                        customfield_data_df.to_sql(customfield_data_table, conn, if_exists="append", index=False)
                        logger.info(f"{len(customfield_data_df)} customfield_data(s) inserted for course {new_course_id}.")
                    except Exception as e:
                        logger.error(f"Error inserting CUSTOMFIELD_DATA for course {new_course_id}: {e}")
                path = f"/1/{context_category_id}/{new_course_context_id}"
                conn.execute(text(f"UPDATE {context_table} SET path = '{path}' WHERE id = {new_course_context_id}"))

                # QUESTION CATEGORY
                question_category_mapping = {}
                insert_and_mapping(conn, id, new_course_id, "question_category", question_category_mapping, question_categories_df, question_categories_table,
                                   param_1=course_old_context_id, param_2=new_course_context_id, param_3="contextid")

                # CHOICE            
                choice_instance_mapping = {}
                insert_and_mapping(conn, id, new_course_id, "choice", choice_instance_mapping, choice_df, choice_table,
                                   param_1=id, param_2=new_course_id, param_3="course",
                                   param_6="choice_options", param_7=choice_options_table, param_8="choiceid", param_9=choice_options_df)
        
                # PAGE
                page_instance_mapping = {}
                insert_and_mapping(conn, id, new_course_id, "page", page_instance_mapping, page_df, page_table,
                                   param_1=id, param_2=new_course_id, param_3="course", param_10="content_link")
                                
                # LABEL
                label_instance_mapping = {}
                insert_and_mapping(conn, id, new_course_id, "label", label_instance_mapping, label_df, label_table,
                                   param_1=id, param_2=new_course_id, param_3="course")
                
                # URL
                url_instance_mapping = {}
                insert_and_mapping(conn, id, new_course_id, "url", url_instance_mapping, url_df, url_table,
                                   param_1=id, param_2=new_course_id, param_3="course")
                

                # ENROL
                enrol_instance_mapping = {}
                insert_and_mapping(conn, id, new_course_id, "enrol", enrol_instance_mapping, enrol_df, enrol_table,
                                   param_1=id, param_2=new_course_id, param_3="courseid")
                
                # FEEDBACK
                new_feedback_id = None
                if course_shortname.startswith("EN") or course_shortname.endswith("EN"):
                    if not feedback_item_en_df.empty:
                        fb_item_df = feedback_item_en_df.copy()
                        fb_df = create_feedback_instance_df(new_course_id, course_shortname)
                        fb_df.to_sql(feedback_table, conn, if_exists="append", index=False)
                        result = conn.execute(text(f"SELECT id FROM {feedback_table} WHERE course = :course_id ORDER BY id DESC LIMIT 1"), {"course_id": new_course_id}).scalar()
                        feedback_item_df = create_feedback_items_df(result, fb_item_df)
                        feedback_item_df.to_sql(feedback_item_table, conn, if_exists="append", index=False)
                        new_feedback_id = result
                else:
                    if not feedback_item_ptbr_df.empty:
                        fb_item_df = feedback_item_ptbr_df.copy()
                        fb_df = create_feedback_instance_df(new_course_id, course_shortname)
                        fb_df.to_sql(feedback_table, conn, if_exists="append", index=False)
                        result = conn.execute(text(f"SELECT id FROM {feedback_table} WHERE course = :course_id ORDER BY id DESC LIMIT 1"), {"course_id": new_course_id}).scalar()
                        feedback_item_df = create_feedback_items_df(result, fb_item_df)
                        feedback_item_df.to_sql(feedback_item_table, conn, if_exists="append", index=False)
                        new_feedback_id = result
                    logger.info(f"{len(feedback_item_df)} feedback item(s) inserted for course {new_course_id}.")

                # RESOURCE
                resource_to_page_instance_mapping = {}
                if not resource_df.empty:
                    resource_filtered = resource_df[resource_df["course"] == id].copy()
                    if not resource_filtered.empty:
                        for _, row in resource_filtered.iterrows():
                            old_id = row["id"]
                            resource_name = row["name"]
                            new_page_ex_resource = create_page_ex_resource_df(new_course_id, resource_name)
                            new_page_ex_resource.to_sql(page_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {page_table} WHERE course = :course_id ORDER BY id DESC LIMIT 1"), {"course_id": new_course_id}).scalar()
                            resource_to_page_instance_mapping[old_id] = result
                        logger.info(f"{len(resource_filtered)} NEW PAGE element(s) to represent OLD RESOURCE(s) inserted successfully! OLD COURSE ID: {id} | NEW COURSE ID: {new_course_id}")
                
                # QUIZ
                quiz_instance_mapping = {}
                if not quiz_df.empty:
                    quiz_filtered = quiz_df[quiz_df["course"] == id].copy()
                    if not quiz_filtered.empty:
                        old_quiz_ids = quiz_filtered["id"].tolist()
                        if course_shortname.startswith("EN") or course_shortname.endswith("EN"):
                            quiz_filtered["name"] = "Final Assessment"
                        else:
                            quiz_filtered["name"] = "Avaliação Final"
                        quiz_filtered["course"] = new_course_id
                        quiz_filtered = quiz_filtered.drop(columns=["id", "completionpass"])
                        try:
                            quiz_filtered.to_sql(quiz_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {quiz_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_quiz_ids = [row[0] for row in result]
                            quiz_instance_mapping.update(dict(zip(old_quiz_ids, new_quiz_ids)))
                            logger.info(f"{len(quiz_filtered)} quiz(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting QUIZ for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No QUIZ entries found for course {id}.")
                
                # QUIZ SECTION
                quiz_sections_mapping = {}
                if not quiz_sections_df.empty:
                    quiz_sections_filtered = quiz_sections_df[quiz_sections_df["quizid"].isin(quiz_instance_mapping.keys())].copy()
                    if not quiz_sections_filtered.empty:
                        old_quiz_section_ids = quiz_sections_filtered["id"].tolist()
                        quiz_sections_filtered["quizid"] = quiz_sections_filtered["quizid"].map(quiz_instance_mapping)
                        quiz_sections_filtered = quiz_sections_filtered.drop(columns=["id"])
                        try:
                            quiz_sections_filtered.to_sql(quiz_sections_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {quiz_sections_table} WHERE quizid IN :q_ids ORDER BY id"), {"q_ids": tuple(quiz_instance_mapping.values())}).fetchall()
                            new_quiz_section_ids = [row[0] for row in result]
                            quiz_sections_mapping.update(dict(zip(old_quiz_section_ids, new_quiz_section_ids)))
                            logger.info(f"{len(quiz_sections_filtered)} quiz section(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting QUIZ_SECTIONS for course {new_course_id}: {e}")

                # QUESTION
                question_instance_mapping = {}
                if not question_df.empty:
                    questions_filtered = question_df[(question_df["category"].isin(question_category_mapping.keys()))].copy()
                    if not questions_filtered.empty:
                        old_question_ids = questions_filtered["id"].tolist()
                        questions_filtered["unique_key"] = questions_filtered["name"] + "_tag_" + questions_filtered["stamp"]
                        unique_keys = questions_filtered["unique_key"].tolist()
                        questions_filtered = questions_filtered.drop(columns=["id", "category", "version", "hidden", "idnumber", "unique_key"])
                        try:
                            questions_filtered.to_sql(question_table, conn, if_exists="append", index=False)
                            stmt = text(f"SELECT id, CONCAT(name, '_tag_', stamp) AS unique_key FROM {question_table} WHERE CONCAT(name, '_tag_', stamp) IN :keys")
                            stmt = stmt.bindparams(bindparam("keys", expanding=True))
                            result = conn.execute(stmt, {"keys": unique_keys}).fetchall()
                            db_key_to_id = {row[1]: row[0] for row in result}  # 1: unique_key, 0: id
                            question_instance_mapping = {old_id: db_key_to_id.get(key) for old_id, key in zip(old_question_ids, unique_keys) if key in db_key_to_id}
                            logger.info(f"{len(questions_filtered)} question(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting QUESTION for course {new_course_id}: {e}")

                # QUESTION BANK ENTRY                
                question_bank_entry_mapping = {}
                if question_instance_mapping:
                    bank_entries = []
                    for old_question_id, new_question_id in question_instance_mapping.items():
                        original_question_id = question_df.loc[question_df["id"] == old_question_id].iloc[0]
                        old_category_id = original_question_id["category"]
                        new_category_id = question_category_mapping.get(old_category_id)
                        ownerid = 2
                        bank_entries.append({"questioncategoryid": new_category_id, "ownerid": ownerid})
                    try:
                        df_bank = pd.DataFrame(bank_entries)    
                        df_bank.to_sql(question_bank_entries_table, conn, if_exists="append", index=False)
                        result = conn.execute(text(f"SELECT id FROM {question_bank_entries_table} WHERE questioncategoryid IN :cat_ids ORDER BY id"), {"cat_ids": tuple(question_category_mapping.values())}).fetchall()
                        new_bank_entry_ids = [row[0] for row in result]
                        question_bank_entry_mapping = dict(zip(question_instance_mapping.values(), new_bank_entry_ids))
                        logger.info(f"{len(df_bank)} question_bank_entries inserted for course {new_course_id}.")
                    except Exception as e:
                        logger.error(f"Error inserting QUESTION_BANK_ENTRIES for course {new_course_id}: {e}")
                    version_entries = []
                    for new_question_id in question_instance_mapping.values():
                        question_entry_id = question_bank_entry_mapping.get(new_question_id)
                        version_entries.append({ "questionbankentryid": question_entry_id, "version": 1, "questionid": new_question_id, "status": "ready"})
                    try:
                        df_versions = pd.DataFrame(version_entries)
                        df_versions.to_sql(question_versions_table, conn, if_exists="append", index=False)
                        logger.info(f"{len(df_versions)} question_versions inserted for course {new_course_id}.")
                    except Exception as e:
                        logger.error(f"Error inserting QUESTION_VERSIONS for course {new_course_id}: {e}")
                    insert_question_type("qtype_ddimageortext", qtype_ddimageortext_df, qtype_ddimageortext_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_ddimageortext_drags", qtype_ddimageortext_drags_df, qtype_ddimageortext_drags_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_ddimageortext_drops", qtype_ddimageortext_drops_df, qtype_ddimageortext_drops_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_ddmarker", qtype_ddmarker_df, qtype_ddmarker_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_ddmarker_drags", qtype_ddmarker_drags_df, qtype_ddmarker_drags_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_ddmarker_drops", qtype_ddmarker_drops_df, qtype_ddmarker_drops_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_essay_options", qtype_essay_options_df, qtype_essay_options_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_match_options", qtype_match_options_df, qtype_match_options_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_match_subquestions", qtype_match_subquestions_df, qtype_match_subquestions_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_multichoice_options", qtype_multichoice_options_df, qtype_multichoice_options_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_randomsamatch_options", qtype_randomsamatch_options_df, qtype_randomsamatch_options_table, question_instance_mapping, new_course_id)
                    insert_question_type("qtype_shortanswer_options", qtype_shortanswer_options_df, qtype_shortanswer_options_table, question_instance_mapping, new_course_id)
                    insert_question_type("question_ddwtos", question_ddwtos_df, question_ddwtos_table, question_instance_mapping, new_course_id)
                    insert_question_type("question_gapselect", question_gapselect_df, question_gapselect_table, question_instance_mapping, new_course_id)
                    insert_question_type("question_truefalse", question_truefalse_df, question_truefalse_table, question_instance_mapping, new_course_id, "showstandardinstruction")

                # QUESTION ANSWERS
                question_answers_mapping = {}
                if not question_answers_df.empty:
                    question_answers_filtered = question_answers_df[question_answers_df["question"].isin(question_instance_mapping.keys())].copy()
                    if not question_answers_filtered.empty:
                        old_question_answer_ids = question_answers_filtered["id"].tolist()
                        question_answers_filtered["question"] = question_answers_filtered["question"].map(question_instance_mapping)
                        question_answers_filtered = question_answers_filtered.drop(columns=["id"])
                        try:
                            question_answers_filtered.to_sql(question_answers_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {question_answers_table} WHERE question IN :q_ids ORDER BY id"), {"q_ids": tuple(question_instance_mapping.values())}).fetchall()
                            new_question_answer_ids = [row[0] for row in result]
                            question_answers_mapping.update(dict(zip(old_question_answer_ids, new_question_answer_ids)))
                            logger.info(f"{len(question_answers_filtered)} question answer(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting QUESTION_ANSWERS for course {new_course_id}: {e}")
                
                # QUIZ SLOTS
                quiz_slots_mapping = {}
                if not quiz_slots_df.empty:
                    quiz_slots_filtered = quiz_slots_df[quiz_slots_df["quizid"].isin(quiz_instance_mapping.keys())].copy()
                    if not quiz_slots_filtered.empty:
                        old_quiz_slot_ids = quiz_slots_filtered["id"].tolist()
                        quiz_slots_filtered["quizid"] = quiz_slots_filtered["quizid"].map(quiz_instance_mapping)
                        quiz_slots_filtered["questionid"] = quiz_slots_filtered["questionid"].map(question_instance_mapping)
                        quiz_slots_filtered = quiz_slots_filtered.drop(columns=["id", "questionid", "questioncategoryid", "includingsubcategories"])
                        try:
                            quiz_slots_filtered.to_sql(quiz_slots_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {quiz_slots_table} WHERE quizid IN :q_ids ORDER BY id"), {"q_ids": tuple(quiz_instance_mapping.values())}).fetchall()
                            new_quiz_slot_ids = [row[0] for row in result]
                            quiz_slots_mapping.update(dict(zip(old_quiz_slot_ids, new_quiz_slot_ids)))
                            logger.info(f"{len(quiz_slots_filtered)} quiz slot(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting QUIZ_SLOTS for course {new_course_id}: {e}")

                # FORUM
                forum_instance_mapping = {}
                insert_and_mapping(conn, id, new_course_id, "forum", forum_instance_mapping, forum_df, forum_table,
                                   param_1=id, param_2=new_course_id, param_3="course")
                
                # REENGAGEMENT
                reengagement_instance_mapping = {}
                insert_and_mapping(conn, id, new_course_id, "reengagement", reengagement_instance_mapping, reengagement_df, reengagement_table,
                                   param_1=id, param_2=new_course_id, param_3="course")
                
                # CUSTOMCERT
                customcert_df = create_customcert_instance_df(new_course_id, course_shortname)
                customcert_df.to_sql(f"{cc_table}", conn, if_exists="append", index=False)
                customcert_instance_id = conn.execute(text(f"SELECT id FROM {new_db.prefix}_customcert WHERE course = :course_id ORDER BY id DESC LIMIT 1"), {"course_id": new_course_id}).scalar()
                logger.info(f"NEW CUSTOMCERT inserted successfully! OLD COURSE ID: {id} | NEW CUSTOMCERT ID: {customcert_instance_id}")

                # COURSE MODULES                
                if not course_modules_filtered_df.empty:
                    course_modules_filtered_df["course"] = new_course_id
                    # changing the module ids
                    course_modules_filtered_df["module"] = course_modules_filtered_df["module"].map(
                        lambda func: new_modules_map.get(old_modules_map.get(func))
                    )
                    hvp_module_ids = set(course_modules_filtered_df[course_modules_filtered_df["module"] == 27]["id"].tolist())
                    # changing the quiz instances ids
                    quiz_module_id = new_modules_map.get("quiz")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == quiz_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == quiz_module_id, "instance"
                    ].map(lambda inst: quiz_instance_mapping.get(inst, inst))

                    # changing the label instances ids
                    label_module_id = new_modules_map.get("label")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == label_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == label_module_id, "instance"
                    ].map(lambda inst: label_instance_mapping.get(inst, inst))

                    # changing the url instances ids
                    url_module_id = new_modules_map.get("url")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == url_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == url_module_id, "instance"
                    ].map(lambda inst: url_instance_mapping.get(inst, inst))

                    # changing the page (and old resources) instances ids
                    all_page_instance_mapping = {**page_instance_mapping, **resource_to_page_instance_mapping}

                    page_module_id = new_modules_map.get("page")
                    resource_module_id = new_modules_map.get("resource")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"].isin([page_module_id, resource_module_id]), "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"].isin([page_module_id, resource_module_id]), "instance"
                    ].map(lambda inst: all_page_instance_mapping.get(inst, inst))

                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == resource_module_id, "module"
                    ] = page_module_id

                    # changing the forum instances ids
                    forum_module_id = new_modules_map.get("forum")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == forum_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == forum_module_id, "instance"
                    ].map(lambda inst: forum_instance_mapping.get(inst, inst))

                    # changing the reengagement instances ids
                    reengagement_module_id = new_modules_map.get("reengagement")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == reengagement_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == reengagement_module_id, "instance"
                    ].map(lambda inst: reengagement_instance_mapping.get(inst, inst))
                    if reengagement_module_id:
                        course_modules_filtered_df.loc[
                            course_modules_filtered_df["module"] == reengagement_module_id, 
                            ["visible", "visibleold", "availability"]
                        ] = [0, 0, None]
                    
                    # changing the choice instances ids
                    choice_module_id = new_modules_map.get("choice")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == choice_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == choice_module_id, "instance"
                    ].map(lambda inst: choice_instance_mapping.get(inst, inst))

                    # changing the enrol instances ids
                    enrol_module_id = new_modules_map.get("enrol")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == enrol_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == enrol_module_id, "instance"
                    ].map(lambda inst: enrol_instance_mapping.get(inst, inst))

                    # changing the feedback instances ids
                    feedback_module_id = new_modules_map.get("feedback")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == feedback_module_id, "instance"
                    ] = new_feedback_id

                    # changing the customcert instances ids
                    customcert_module_id = new_modules_map.get("customcert")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == customcert_module_id, "instance"
                    ] = customcert_instance_id

                    # storing old ids
                    course_modules_filtered_df["old_id"] = course_modules_filtered_df["id"]
                    # droping old ids
                    course_modules_filtered_df = course_modules_filtered_df.drop(columns=["id"])

                    # inserting and maping new modules ids
                    for _, row in course_modules_filtered_df.iterrows():
                        sql = text(f"""
                                    INSERT INTO {course_modules_table} 
                                    (course, module, instance, section, added, score, indent, visible, visibleold, groupmode, groupingid, completion, completiongradeitemnumber, completionview, completionexpected, availability, showdescription)
                                    VALUES
                                    (:course, :module, :instance, :section, :added, :score, :indent, :visible, :visibleold, :groupmode, :groupingid, :completion, :completiongradeitemnumber, :completionview, :completionexpected, :availability, :showdescription)
                                    """
                        )
                        contional_modules = [7, 8, 16, 17, 18, 21, 27, 29]  # modules to be inserted with specific RESTRICTIONS
                        """
                        7 = feedback
                        8 = folder
                        16 = page
                        17 = quiz
                        18 = resource
                        21 = url
                        27 = hvp
                        29 = customcert
                        """
                        if row["module"] in contional_modules:
                            row["availability"] = '{"op":"&","c":[{"type":"completion","cm":-1,"e":1}],"showc":[true]}'
                        row_cleaned = row.replace({pd.NA: None, '': None}).where(pd.notnull(row), None)
                        row_dict = row_cleaned.to_dict()
                        conn.execute(sql, row_dict)

                        # Map the old cm_id to the new cm_id
                        old_cm_id = row["old_id"]
                        new_cm_id = conn.execute(text(f"SELECT id FROM {course_modules_table} ORDER BY id DESC LIMIT 1")).scalar()
                        module_instance_mapping[old_cm_id] = new_cm_id

                        # create course_module context (contextlevel 70)
                        conn.execute(text(f"INSERT INTO {context_table} (contextlevel, instanceid, depth, path) VALUES (70, :instanceid, 4, NULL)"), {"instanceid": new_cm_id})
                        # retrieve new context.id
                        result = conn.execute(text(f"SELECT id FROM {context_table} WHERE contextlevel = 70 AND instanceid = :instanceid ORDER BY id DESC LIMIT 1"), {"instanceid": new_cm_id})
                        new_cm_context_id = result.scalar()
                        # update context path
                        path_cm = f"/1/{context_category_id}/{new_course_context_id}/{new_cm_context_id}"
                        conn.execute(text(f"UPDATE {context_table} SET path = :path WHERE id = :id"), {"path": path_cm, "id": new_cm_context_id})
                    logger.info(f"{len(course_modules_filtered_df)} course_modules inserted.")

                # Create question references
                question_references_data = []
                for old_slot_id, new_slot_id in quiz_slots_mapping.items():
                    # get old_question_id from original slot
                    slot_row = quiz_slots_df[quiz_slots_df["id"] == old_slot_id]
                    if slot_row.empty:
                        logger.warning(f"Slot ID {old_slot_id} not found in original quiz_slots_df.")
                        continue

                    old_question_id = slot_row["questionid"].values[0]
                    new_question_id = question_instance_mapping.get(old_question_id)
                    if not new_question_id:
                        logger.warning(f"No new question ID found for old question ID {old_question_id}. Skipping slot {new_slot_id}.")
                        continue
                    
                    questionbankentryid = question_bank_entry_mapping.get(new_question_id)
                    if not questionbankentryid:
                        logger.warning(f"No question bank entry found for question ID {new_question_id}. Skipping slot {new_slot_id}.")
                        continue
                    
                    # find the quiz course_module's contexdid
                    quiz_id = slot_row["quizid"].values[0]
                    new_quiz_id = quiz_instance_mapping.get(quiz_id)
                    if not new_quiz_id:
                        logger.warning(f"No new quiz ID found for old quiz ID {quiz_id}. Skipping.")
                        continue
                    
                    cm_id = conn.execute(text(f"SELECT id FROM {course_modules_table} WHERE course = :course_id AND module = 17 AND instance = :instance ORDER BY id DESC LIMIT 1"),
                                         {"course_id": new_course_id, "instance": new_quiz_id}).scalar()
                    if not cm_id:
                        logger.warning(f"No CM ID found for quiz {new_quiz_id}. Skipping.")
                        continue
                    
                    cm_context_id = conn.execute(text(f"SELECT id FROM {context_table} WHERE contextlevel = 70 AND instanceid = :cm_id ORDER BY id DESC LIMIT 1"), {"cm_id": cm_id}).scalar()
                    if not cm_context_id:
                        logger.warning(f"No context ID found for cm_id {cm_id}. Skipping.")
                        continue
                    
                    question_references_data.append({"usingcontextid": cm_context_id, "component": "mod_quiz", "questionarea": "slot", "itemid": new_slot_id, "questionbankentryid": questionbankentryid})

                # inserting question references
                if question_references_data:
                    question_references_df = pd.DataFrame(question_references_data)
                    try:
                        question_references_df.to_sql(question_references_table, conn, if_exists="append", index=False)
                        logger.info(f"{len(question_references_df)} question references inserted for course {new_course_id}.")
                    except Exception as e:
                        logger.error(f"Error inserting QUESTION_REFERENCES for course {new_course_id}: {e}")

                # CUSTOMCERT
                if not customcert_df.empty:
                    customcert_cm_id = conn.execute(text(f"SELECT id FROM {course_modules_table} WHERE course = :course_id AND module = 29 AND instance = :customcert_instance_id ORDER BY id DESC LIMIT 1"),
                                                    {"course_id": new_course_id, "customcert_instance_id": customcert_instance_id}).scalar()
                    logger.info(f"NEW CUSTOMCERT CM ID inserted successfully! OLD COURSE ID: {id} | NEW CUSTOMCERT CM ID: {customcert_cm_id}")

                    customcert_cm_context_id = conn.execute(text(f"SELECT id FROM {context_table} WHERE instanceid = :instanceid AND contextlevel = 70 ORDER BY id DESC LIMIT 1"), {"instanceid": customcert_cm_id}).scalar()
                    logger.info(f"NEW CUSTOMCERT CM CONTEXT ID inserted successfully! OLD COURSE ID: {id} | NEW CUSTOMCERT CM CONTEXT ID: {customcert_cm_context_id}")

                    if course_shortname.startswith("EN") or course_shortname.endswith("EN"):
                        customcert_template_df = create_customcert_template_df(cc_templates_en_df.copy(), customcert_cm_context_id, course_shortname)
                    else:
                        customcert_template_df = create_customcert_template_df(cc_templates_br_df.copy(), customcert_cm_context_id, course_shortname)
                    customcert_template_df.to_sql(f"{cc_templates_table}", conn, if_exists="append", index=False)
                    customcert_template_id = conn.execute(text(f"SELECT id FROM {cc_templates_table} WHERE contextid = :context_id ORDER BY id DESC LIMIT 1"), {"context_id": customcert_cm_context_id}).scalar()
                    
                    customcert_id = conn.execute(text(f"SELECT id FROM {cc_table} WHERE course = :course_id ORDER BY id DESC LIMIT 1"), {"course_id": new_course_id}).scalar()
                    conn.execute(
                        text(f"UPDATE {cc_table} SET templateid = :templateid WHERE id = :customcert_id and course = :course_id"),
                        {"templateid": customcert_template_id, "customcert_id": customcert_id, "course_id": new_course_id}
                    )
                    logger.info(f"NEW CUSTOMCERT TEMPLATE ID inserted successfully! OLD COURSE ID: {id} | NEW CUSTOMCERT TEMPLATE ID: {customcert_template_id}")

                    if course_shortname.startswith("EN") or course_shortname.endswith("EN"):
                        customcert_pages_df = create_customcert_page_df(cc_pages_en_df.copy(), customcert_template_id)
                    else:
                        customcert_pages_df = create_customcert_page_df(cc_pages_br_df.copy(), customcert_template_id)
                    customcert_pages_df.to_sql(f"{cc_pages_table}", conn, if_exists="append", index=False)
                    customcert_pages_ids = conn.execute(text(f"SELECT id FROM {cc_pages_table} WHERE templateid = :templateid ORDER BY id, sequence ASC"),
                                                        {"templateid": customcert_template_id}).fetchall()
                    cc_pages_ids = [row[0] for row in customcert_pages_ids]
                    logger.info(f"NEW CUSTOMCERT PAGES IDS inserted successfully! OLD COURSE ID: {id} | NEW CUSTOMCERT PAGES IDS: {cc_pages_ids}")
                    
                    if course_shortname.startswith("EN") or course_shortname.endswith("EN"):
                        customcert_elements_df = create_customcert_elements_df(cc_elements_en_df.copy(), cc_pages_ids)
                    else:
                        customcert_elements_df = create_customcert_elements_df(cc_elements_br_df.copy(), cc_pages_ids)
                    customcert_elements_df.to_sql(f"{cc_elements_table}", conn, if_exists="append", index=False)
                    customcert_elements_ids = conn.execute(text(f"SELECT id FROM {cc_elements_table} WHERE pageid IN :pageids ORDER BY id, sequence ASC"), {"pageids": tuple(cc_pages_ids)}).fetchall()
                    cc_elements_ids = [row[0] for row in customcert_elements_ids]
                    logger.info(f"NEW CUSTOMCERT ELEMENTS IDS inserted successfully! OLD COURSE ID: {id} | NEW CUSTOMCERT ELEMENTS IDS: {cc_elements_ids}")

                # COURSE SECTIONS
                if not course_sections_df.empty:
                    section_sequence_map = dict(zip(course_sections_df["section"], course_sections_df["sequence"]))
                    course_sections_df["course"] = new_course_id
                    old_section_ids = course_sections_df["id"].tolist()
                    course_sections_df = course_sections_df.drop(columns=["id"])
                    course_sections_df.loc[course_sections_df["name"].str.strip().str.lower().isin(["conteúdo", "content"]),
                                           "availability"] = '{"op":"|","c":[],"show":false}'
                    mask1 = course_sections_df["name"].str.strip().str.lower().isin(["avaliações finais", "certificado", "final assessments", "certificate"])
                    course_sections_df.loc[mask1, "availability"] = '{"op":"&","c":[{"type":"completion","cm":-1,"e":1}],"showc":[false]}'
                    mask2 = course_sections_df["name"].str.strip().str.lower().isin(["avaliações finais", "final assessments"])
                    if course_shortname.upper().startswith("EN") or course_shortname.upper().endswith("EN"):
                        summary_text = """
                                        <div id="yui_3_17_2_1_1729086180475_803" align="right">
                                        <table>
                                            <tbody>
                                            <tr>
                                                <td>{courseprogressbar}</td>
                                            </tr>
                                            <tr>
                                                <td>{courseprogress}</td>
                                            </tr>
                                            </tbody>
                                        </table>
                                        </div>
                                        <p>In our distance learning methodology, the course includes two fundamental
                                        assessments:</p>
                                        <p>1 - <strong>Final Assessment</strong>: covers the theoretical part of the
                                        course;</p>
                                        <p>2 - <strong>Satisfaction Feedback:</strong> focuses on your feedback regarding
                                        the course.</p>
                                        <p>All the assessments above are conducted here on the platform.</p>
                                        <p dir="ltr"></p>
                                        <p dir="ltr"><span class=""><strong>Attention:</strong> The certificate is
                                            pending completion of both assessments above.</span></p>
                                        """
                        summary_text2 = """
                                        <p>The course completion certificate is a professional and personal achievement
                                        that indicates the student has reached a satisfactory level of training,
                                        equipping and qualifying them to work more safely.</p>
                                        <p>Talismã's certificate is valid throughout the national territory and meets
                                        all the requirements of Brazilian legislation, including references to
                                        Regulatory Standards, the Brazilian Navy, and market standards.</p>
                                        <p><strong>Attention:</strong> The certificate is issued when class attendance
                                        exceeds 75%, and upon passing both the Final and Practical Assessments, if
                                        applicable. Don't forget to complete the Satisfaction Feedback—we value your
                                        feedback.</p>
                                        """
                        summary_text3 = """
                                        <p>The <strong>Final Assessment</strong> aims to evaluate the
                                        progression of the learning process within the course, summarizing key
                                        learnings based on general criteria.</p>
                                        <p>Here are some important criteria:</p>
                                        <p>1 - The assessment is only available after completing all intermediate
                                        assessments;</p>
                                        <p>2 - The duration for completing the assessment is <strong>1 hour (60
                                            minutes)</strong>;</p>
                                        <p>3 - The <strong>minimum passing score is 70%</strong>. If a <strong>Practical Assessment</strong> is applicable, the final score is the
                                        arithmetic average of the Final and Practical Assessments;
                                        </p>
                                        <p>4 - Up to <strong>two attempts</strong> are allowed;</p>
                                        <p>5 - In case of failure in both attempts, please contact us through one of our
                                        support channels.</p>
                                        """
                    else:
                        summary_text = """
                                        <div id="yui_3_17_2_1_1729086180475_803" align="right">
                                        <table>
                                            <tbody>
                                            <tr>
                                                <td>{courseprogressbar}</td>
                                            </tr>
                                            <tr>
                                                <td>{courseprogress}</td>
                                            </tr>
                                            </tbody>
                                        </table>
                                        </div>
                                        <p>Em nossa metodologia de ensino a distância o curso tem duas avaliações
                                        fundamentais, que são:</p>
                                        <p>1 - <strong>Avaliação Final</strong>: aborda a parte teórica do curso;</p>
                                        <p>2 - <strong>Pesquisa de Satisfação: </strong>aborda a sua receptividade ao
                                        curso.</p>
                                        <p>Todas as avaliações acima são realizadas aqui na plataforma.</p>
                                        <p dir="ltr"></p>
                                        <p dir="ltr"><span class="">Atenção: O certificado está pendente a conclusão das
                                            duas avaliações acima.</span></p>
                                       """
                        summary_text2 = """
                                        <p dir="ltr" id="yui_3_17_2_1_1729087194198_803">O certificado de conclusão de
                                        curso é uma conquista profissional e pessoal que indica que o aluno alcançou
                                        nível de treinamento satisfatório capacitando-o e qualificando-o a trabalhar
                                        de forma mais segura.</p>
                                        <p dir="ltr">O certificado da Talismã é válido em todo território nacional e
                                        atende todos os requisitos da legislação brasileira como referências as Normas
                                        Regulamentadoras, Marinha do Brasil e padrões do mercado.</p>
                                        <p dir="ltr" id="yui_3_17_2_1_1729087194198_788"><span
                                            id="yui_3_17_2_1_1729087194198_787" class=""><strong>Atenção:</strong>  O
                                            certificado é emitido na frequência das aulas acima de 75%, na aprovação na
                                            Avaliação Final e Avaliação Prática caso aplicável. Não esqueça de realizar
                                            a Pesquisa de Satisfação, contamos com sua opinião.</span></p>
                                       """
                        summary_text3 = """
                                        <p>A <strong>Avaliação Final</strong> tem como objetivo fazer um balanço da sequência de trabalho de
                                            formação do conteúdo previsto no curso sintetizando as aprendizagens tendo por
                                            bases critérios gerais.</p>
                                        <p>Segue alguns critérios importantes:</p>
                                        <p>1 - A avaliação está disponível somente após a realização de todas as avaliações intermediárias;&nbsp;&nbsp;</p>
                                        <p>2 - Duração para realização da avaliação é de 01 hora (60 minutos);</p>
                                        <p>3 - Nota mínima para aprovação é
                                            de 70%, caso aplicável a avaliação prática, a nota final é a
                                            média aritmética da avaliação final e avaliação prática;</p>
                                        <p>4 - São permitidas até duas tentativas;</p>
                                        <p>5 - Caso de reprovação nas duas tentativas, entrar em contato em um dos nossos canais de atendimentos.</p>                                       
                                        """
                    course_sections_df.loc[mask2, "summary"] = summary_text
                    mask3 = course_sections_df["name"].str.strip().str.lower().isin(["certificado", "certificate"])
                    course_sections_df.loc[mask3, "summary"] = summary_text2
                    mask4 = course_sections_df["name"].str.strip().str.lower().isin(["avaliação das atividades teórica", "theoretical activities assessment"])
                    course_sections_df.loc[mask4, "summary"] = summary_text3
                    course_sections_df["name"] = course_sections_df["name"].replace({
                        "Avaliações Finais": "Avaliação",
                        "Final Assessments": "Assessment",
                        "Avaliação das Atividades Práticas": "Avaliação",
                        "Avaliação das Atividades Prática": "Avaliação",
                        "Avaliação das Atividades Teórica": "Avaliação",
                        "Practical Activities Assessment": "Assessment",
                        "Theoretical Activities Assessment": "Assessment",
                        "Evaluation of Practical Activities": "Assessment"
                    })
                    mask5 = course_sections_df["name"].str.strip().str.lower().isin(["sobre o curso", "about the course"])
                    course_sections_df.loc[mask5, "summary"] = course_sections_df.loc[mask5, "summary"].str.replace(
                        "{course_field_carga_horaria}",
                        "{course_field_ch}",
                        regex=False
                    )

                    course_sections_df["sequence"] = course_sections_df["sequence"].apply(lambda seq: transform_sequence(seq, module_instance_mapping, hvp_module_ids))
                    course_sections_df.to_sql(sections_table, conn, if_exists="append", index=False)
                    logger.info(f"{len(course_sections_df)} section(s) inserted for course {new_course_id}.")

                    # get new sections ids
                    result = conn.execute(text(f"SELECT id, section FROM {sections_table} WHERE course = :course_id"), {"course_id": new_course_id}).mappings().fetchall()
                    new_section_ids = [row["id"] for row in result]
                    section_id_mapping = {0: 0}
                    section_id_mapping.update(dict(zip(old_section_ids, new_section_ids)))
                    old_to_new_section_ids = {row["section"]: row["id"] for row in result}

                    # mapping new sectionids directly from old id
                    course_format_options_filtered["sectionid"] = course_format_options_filtered["sectionid"].fillna(-1).astype(int)
                    course_format_options_filtered["sectionid"] = course_format_options_filtered["sectionid"].map(lambda sid: section_id_mapping.get(sid) if sid > 0 else 0)
                    course_format_options_filtered["courseid"] = new_course_id
                    course_modules_filtered_df["section"] = course_modules_filtered_df["section"].map(old_to_new_section_ids)

                    # SECTION's SEQUENCE
                    section_to_sequence_mapping = {}
                    # Iterate over the old_to_new_section_ids and match it with the sequence values
                    for old_section_index, new_section_id in old_to_new_section_ids.items():
                        sequence_value = section_sequence_map.get(old_section_index, '')
                        section_to_sequence_mapping[new_section_id] = sequence_value
                    
                    # manipulating a specific course_module to adjust the availability and restrictions
                    content_section = conn.execute(text(f"SELECT section FROM {sections_table} WHERE course = :course_id AND name IN ('Conteúdo', 'Content') ORDER BY section ASC LIMIT 1"),
                                                   {"course_id": new_course_id}).scalar()
                    daughter_content_section = conn.execute(text(f"SELECT sequence FROM {sections_table} WHERE course = :course_id AND section > :content_section ORDER BY section ASC LIMIT 1"),
                                                            {"course_id": new_course_id, "content_section": content_section}).scalar()
                    
                    if daughter_content_section:
                        sequence_ids = [int(x) for x in daughter_content_section.split(",") if x.strip().isdigit()]
                        hvp_ids_result = conn.execute(text(f"SELECT id FROM {course_modules_table} WHERE course = :course_id AND module = 27"), {"course_id": new_course_id}).fetchall()
                        hvp_ids = {row[0] for row in hvp_ids_result}
                        filtered_sequence_ids = [cm_id for cm_id in sequence_ids if cm_id not in hvp_ids]                
                        if filtered_sequence_ids:
                            availability_placeholder = '{"op":"|","c":[],"show":true}'
                            conn.execute(text(f"UPDATE {course_modules_table} SET availability = :availability WHERE course = :course_id AND id = :id"),
                                         {"course_id": new_course_id, "id": filtered_sequence_ids[0], "availability": availability_placeholder})
                    logger.info("All course_sections updated with correct sequences.")

                # COURSE FORMAT OPTIONS
                if not course_format_options_filtered.empty:
                    logger.debug(f"Inserting {len(course_format_options_filtered)} course_format_options for course {new_course_id}.")
                    course_format_options_filtered["sectionid"] = course_format_options_filtered["sectionid"].astype("Int64")  # Pandas nullable int
                    course_format_options_filtered = course_format_options_filtered.drop(columns=["id"])
                    logger.info(f"Final format_options to insert: {len(course_format_options_filtered)}")
                    course_format_options_filtered.to_sql(course_format_options_table, conn, if_exists="append", index=False)
                    logger.info(f"{len(course_format_options_filtered)} course_format_options inserted.")
                else:
                    logger.warning(f"No course_format_options found for course ID {id}.")
                
            except Exception as e:
                logger.error(f"Error inserting copied COURSE based on ID {id}: {e}")


def get_unique_filename(output_dir: str) -> str:
    date_str = datetime.now().strftime("%m_%d_%Y")
    version = 1
    while True:
        filename = f"loaded_data_{date_str}_v{version}.xlsx"
        output_path = os.path.join(output_dir, filename)
        if not os.path.exists(output_path):
            return output_path
        version += 1


def load(dataframes: Dict[str, pd.DataFrame], conn, new_db):
    logger.debug(f"-------------------- Starting the loading process... --------------------")

    output_dir = "src/loaded"
    os.makedirs(output_dir, exist_ok=True)
    output_path = get_unique_filename(output_dir)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        try:
            for table, df in dataframes.items():
                logger.debug(f"Processing table '{table.upper()}' for Excel export...")
                if df.empty:
                    logger.warning(f"{table.upper()} is empty.")
                    continue

                logger.info(f"{table.upper()} extracted successfully with {len(df)} rows.")

                if table == "course":
                    if_table_course(conn, table, ids=[53, 399], dataframes=dataframes, category=1, new_db=new_db)

                logger.info(f"{table.upper()} has {len(df.columns)} columns: {df.columns.tolist()}.")

                df.to_excel(writer, sheet_name=table, index=False)
            logger.info(f"File successfully saved: {os.path.basename(output_path)}.")
        except Exception as e:
            logger.error(f"Error creating the xlsx file: {e}.")
    logger.info(f"-------------------- End of loading process. --------------------")
