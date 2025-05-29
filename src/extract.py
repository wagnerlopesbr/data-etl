import pandas as pd
from src.logging import start


logger = start()


def extract_old_course_ids_from_csv(old_id_category, csv):
    df = pd.read_csv(csv, sep=';', encoding='latin1')
    filtered_df = df[df['id_category'] == old_id_category]
    ids_list = filtered_df['id_course'].tolist()
    return ids_list


def extract(conn, db_prefix, origin: str):
    logger.debug(f"-------------------- Starting the extraction process... --------------------")
    dataframes = {}

    specific_joins = ["page", "feedback", "quiz", "url", "forum", "reengagement"]
    
    if origin == "old":
        queries = {
            "context_course": f"SELECT * FROM {db_prefix}_context WHERE contextlevel = 50",
            "context_category": f"SELECT * FROM {db_prefix}_context WHERE contextlevel = 40",
            "course_categories": f"SELECT * FROM {db_prefix}_course_categories",
            "course": f"SELECT * FROM {db_prefix}_course WHERE format <> 'site'",
            "customfield_field_old": f"SELECT * FROM {db_prefix}_customfield_field WHERE categoryid = 2 ORDER BY FIELD(id, 8, 1, 2, 3, 4, 5, 6, 7)",
            "customfield_data": f"SELECT * FROM {db_prefix}_customfield_data ORDER BY FIELD(fieldid, 8, 1, 2, 3, 4, 5, 6, 7)",
            "course_sections": f"SELECT * FROM {db_prefix}_course_sections ORDER BY section ASC",
            "course_modules": f"SELECT * FROM {db_prefix}_course_modules",
            "modules": f"SELECT * FROM {db_prefix}_modules",
            "page": f"SELECT * FROM {db_prefix}_page",
            "choice": f"SELECT * FROM {db_prefix}_choice ORDER BY id ASC",
            "choice_options": f"SELECT * FROM {db_prefix}_choice_options ORDER BY choiceid ASC",
            "quiz": f"SELECT * FROM {db_prefix}_quiz",
            "quiz_slots": f"SELECT * FROM {db_prefix}_quiz_slots",
            "quiz_sections": f"SELECT * FROM {db_prefix}_quiz_sections ORDER BY quizid ASC",
            "question_categories": f"SELECT * FROM {db_prefix}_question_categories",
            "question": f"SELECT * FROM {db_prefix}_question",
            "question_answers": f"SELECT * FROM {db_prefix}_question_answers",
            "qtype_ddimageortext": f"SELECT * FROM {db_prefix}_qtype_ddimageortext",
            "qtype_ddimageortext_drags": f"SELECT * FROM {db_prefix}_qtype_ddimageortext_drags",
            "qtype_ddimageortext_drops": f"SELECT * FROM {db_prefix}_qtype_ddimageortext_drops",
            "qtype_ddmarker": f"SELECT * FROM {db_prefix}_qtype_ddmarker",
            "qtype_ddmarker_drags": f"SELECT * FROM {db_prefix}_qtype_ddmarker_drags",
            "qtype_ddmarker_drops": f"SELECT * FROM {db_prefix}_qtype_ddmarker_drops",
            "qtype_essay_options": f"SELECT * FROM {db_prefix}_qtype_essay_options",
            "qtype_match_options": f"SELECT * FROM {db_prefix}_qtype_match_options",
            "qtype_match_subquestions": f"SELECT * FROM {db_prefix}_qtype_match_subquestions",
            "qtype_multichoice_options": f"SELECT * FROM {db_prefix}_qtype_multichoice_options",
            "qtype_randomsamatch_options": f"SELECT * FROM {db_prefix}_qtype_randomsamatch_options",
            "qtype_shortanswer_options": f"SELECT * FROM {db_prefix}_qtype_shortanswer_options",
            "question_ddwtos": f"SELECT * FROM {db_prefix}_question_ddwtos",
            "question_gapselect": f"SELECT * FROM {db_prefix}_question_gapselect",
            "question_truefalse": f"SELECT * FROM {db_prefix}_question_truefalse",
            "url": f"SELECT * FROM {db_prefix}_url",
            "enrol": f"SELECT * FROM {db_prefix}_enrol ORDER BY courseid ASC",
            "forum": f"SELECT * FROM {db_prefix}_forum",
            "label": f"SELECT * FROM {db_prefix}_label",
            "folder": f"SELECT * FROM {db_prefix}_folder",
            "resource": f"SELECT * FROM {db_prefix}_resource",
            "hvp_contents_libraries": f"SELECT * FROM {db_prefix}_hvp_contents_libraries WHERE library_id IN (11, 12, 44, 79, 80, 81, 82, 83, 100, 104, 107, 113, 114, 124, 133, 134, 137, 141, 142, 145, 146, 150, 151, 153, 154, 156)",
            "hvp": f"SELECT * FROM {db_prefix}_hvp WHERE main_library_id IN (84, 140, 143)",
            "hvp_games": f"SELECT * FROM {db_prefix}_hvp WHERE main_library_id IN (11, 12, 44, 79, 80, 81, 82, 83, 100, 104, 107, 113, 114, 124, 133, 134, 137, 141, 142, 145, 146, 150, 151, 153, 154, 156)",
            "reengagement": f"SELECT * FROM {db_prefix}_reengagement",
            "customcert_image_hash_info": f"""
                                                SELECT ctx.id AS context_id,
                                                    f.id as file_id,
                                                    e.id AS element_id,
                                                    f.filename,
                                                    f.contenthash,
                                                    p.id as page_id,
                                                    f.mimetype,
                                                    cc.course as course_id,
                                                    c.fullname
                                                FROM {db_prefix}_customcert_elements e
                                                INNER JOIN {db_prefix}_customcert_pages p ON p.id = e.pageid
                                                INNER JOIN {db_prefix}_customcert_templates t ON t.id = p.templateid
                                                INNER JOIN {db_prefix}_customcert cc ON cc.templateid = t.id
                                                INNER JOIN {db_prefix}_context ctx ON ctx.id = t.contextid
                                                INNER JOIN {db_prefix}_context cctx ON cctx.instanceid = cc.course
                                                INNER JOIN {db_prefix}_course c ON c.id = cc.course
                                                INNER JOIN {db_prefix}_files f ON f.component = 'mod_customcert' AND f.filearea = 'image' and f.mimetype like '%%image%%' and f.contextid = cctx.id
                                                where e.name = 'conteúdo programático'
                                                AND JSON_UNQUOTE(JSON_EXTRACT(e.data, '$.filename')) = f.filename
                                                ORDER BY c.fullname ASC
                                        """,
            "resource_content_hash_info": f"""
                                                SELECT ctx.id AS resource_context_id,
                                                    f.contenthash,
                                                    f.filename,
                                                    r.id as resource_id,
                                                    r.name as resource_name,
                                                    c.fullname as course,
                                                    c.id as course_id
                                                FROM {db_prefix}_resource r
                                                INNER JOIN {db_prefix}_course_modules cm ON cm.instance = r.id AND cm.module = 18
                                                INNER JOIN {db_prefix}_context ctx ON ctx.instanceid = cm.id
                                                INNER JOIN {db_prefix}_course c ON c.id = r.course
                                                INNER JOIN {db_prefix}_files f ON f.component = 'mod_resource' AND f.contextid = ctx.id
                                                WHERE f.filename <> '.'
                                                ORDER BY c.fullname ASC
                                        """,
            "hvp_content_hash_info": f"""
                                                SELECT ctx.id AS resource_context_id,
                                                    f.contenthash,
                                                    f.filename,
                                                    h.id as hvp_id,
                                                    h.name as hvp_name,
                                                    c.fullname as course,
                                                    c.id as course_id
                                                FROM {db_prefix}_hvp h
                                                INNER JOIN {db_prefix}_course_modules cm ON cm.instance = h.id AND cm.module = 30
                                                INNER JOIN {db_prefix}_context ctx ON ctx.instanceid = cm.id
                                                INNER JOIN {db_prefix}_course c ON c.id = h.course
                                                INNER JOIN {db_prefix}_files f ON f.component = 'mod_hvp' AND f.contextid = ctx.id
                                                WHERE f.filename <> '.'
                                                ORDER BY c.fullname ASC
                                    """,
            "course_format_options": f"SELECT * FROM {db_prefix}_course_format_options ORDER BY sectionid ASC",
            "course_modules_sections": f"""
                                        SELECT
                                        c.id AS course_id,
                                        c.fullname AS course_name,
                                        cs.id AS course_section_id,
                                        cs.name AS course_section_name,
                                        cs.sequence AS course_section_sequence,
                                        cm.id AS course_module_id,
                                        cm.module AS course_module_type_id,
                                        m.name AS course_module_type,
                                        cm.instance AS course_module_instance_id
                                        FROM {db_prefix}_course_modules cm
                                        JOIN {db_prefix}_course c ON c.id = cm.course
                                        JOIN {db_prefix}_course_sections cs ON cs.id = cm.section
                                        JOIN {db_prefix}_modules m ON m.id = cm.module
                                        """
        }

        for type in specific_joins:
            queries[f"course_{type}_instances"] = f"""
                SELECT
                    c.id AS course_id,
                    c.fullname AS course_name,
                    cm.id AS course_module_id,
                    cm.module AS course_module_type_id,
                    m.name AS course_module_type,
                    cm.instance AS course_module_instance_id,
                    t.*
                FROM {db_prefix}_course_modules cm
                JOIN {db_prefix}_course c ON c.id = cm.course
                JOIN {db_prefix}_course_sections cs ON cs.id = cm.section
                JOIN {db_prefix}_modules m ON m.id = cm.module
                JOIN {db_prefix}_{type} t ON t.id = cm.instance AND m.name = '{type}'
            """
    else:
        queries = {
            "customcert_templates_ptbr": f"SELECT * FROM {db_prefix}_customcert_templates WHERE id = 2 LIMIT 1",
            "customcert_pages_ptbr": f"SELECT * FROM {db_prefix}_customcert_pages WHERE templateid = 2 ORDER BY id, sequence ASC",
            "customcert_elements_ptbr": f"SELECT * FROM {db_prefix}_customcert_elements WHERE pageid IN (3, 10) ORDER BY pageid, sequence ASC",
            "customcert_templates_en": f"SELECT * FROM {db_prefix}_customcert_templates WHERE id = 15 LIMIT 1",
            "customcert_pages_en": f"SELECT * FROM {db_prefix}_customcert_pages WHERE templateid = 15 ORDER BY id, sequence ASC",
            "customcert_elements_en": f"SELECT * FROM {db_prefix}_customcert_elements WHERE pageid IN (222, 223) ORDER BY pageid, sequence ASC",
            "customfield_field_new": f"SELECT * FROM {db_prefix}_customfield_field WHERE categoryid = 7 ORDER BY id ASC",
            "feedback_item_ptbr": f"SELECT * FROM {db_prefix}_feedback_item WHERE template = 2 AND feedback = 0",
            "feedback_item_en": f"SELECT * FROM {db_prefix}_feedback_item WHERE template = 4 AND feedback = 0"
        }

    for table, query in queries.items():
        try:
            logger.debug(f"Running query for '{table.upper()}'...")
            df = pd.read_sql(query, conn)
            logger.info(f"Extracted {len(df)} rows from {table.upper()} ({origin.upper()}).")
            dataframes[table] = df
        except Exception as e:
            logger.error(f"Query for {table.upper()} failed: {query}.")
            logger.error(f"Error extracting {table.upper()} from {origin.upper()} DB: {e}.")
            
    logger.info(f"-------------------- End of extraction process. --------------------")
    return dataframes
