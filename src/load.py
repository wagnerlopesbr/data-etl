from typing import Dict, List
import pandas as pd
import os
import datetime
from src.logging import start
from sqlalchemy import text
from src.transform import transform_sequence


logger = start()

"""
def if_table_choice(table: str, df: pd.DataFrame):
    matching = df[df["match_name_filtering"]]
    not_matching = df[~df["match_name_filtering"]]
    logger.info(f"{table.upper()} has {len(matching)} rows matching 'Política de Assinatura' or 'Signature Policy'.")
    if len(not_matching) != 0:
        logger.info(f"There {'is' if len(not_matching) == 1 else 'are'} {len(not_matching)} row{'s' if len(not_matching) != 1 else ''} not matching.")
"""

def if_table_course(conn, table: str, ids: List[int], dataframes: Dict[str, pd.DataFrame], category: int = 1, new_db: str = ''):
    prefixed_table = f"{new_db.prefix}_{table}"
    context_table = f"{new_db.prefix}_context"
    sections_table = f"{new_db.prefix}_course_sections"
    modules_table = f"{new_db.prefix}_modules"
    course_modules_table = f"{new_db.prefix}_course_modules"
    course_format_options_table = f"{new_db.prefix}_course_format_options"
    page_table = f"{new_db.prefix}_page"
    label_table = f"{new_db.prefix}_label"
    url_table = f"{new_db.prefix}_url"
    enrol_table = f"{new_db.prefix}_enrol"
    resource_table = f"{new_db.prefix}_resource"
    quiz_table = f"{new_db.prefix}_quiz"
    forum_table = f"{new_db.prefix}_forum"
    reengagement_table = f"{new_db.prefix}_reengagement"
    choice_table = f"{new_db.prefix}_choice"
    choice_options_table = f"{new_db.prefix}_choice_options"
    #hvp_table = f"{new_db.prefix}_hvp"

    module_instance_mapping = {}

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
        #hvp_df = dataframes.get("hvp", pd.DataFrame())

        course = course_df[course_df["id"] == id]
        if course.empty:
            logger.warning(f"No row(s) found in 'COURSE' with id {id}.")
        else:
            try:
                course_copy = course.copy()
                course_copy["category"] = category
                course_copy = course_copy.drop(columns=["id", "originalcourseid"])

                # course
                course_copy.to_sql(prefixed_table, conn, if_exists="append", index=False)
                new_course = conn.execute(text(f"SELECT id FROM {prefixed_table} ORDER BY id DESC LIMIT 1"))
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
                conn.execute(text(
                    f"""
                    INSERT INTO {context_table} (contextlevel, instanceid, depth, path)
                    VALUES (50, {new_course_id}, 3, NULL)
                    """
                ))
                result = conn.execute(text(
                    f"""
                    SELECT id FROM {context_table}
                    WHERE contextlevel = 50 AND instanceid = {new_course_id}
                    ORDER BY id DESC LIMIT 1
                    """
                ))
                new_course_context_id = result.scalar()
                logger.info(f"NEW COURSE CONTEXT ID inserted successfully! NEW COURSE CONTEXT ID: {new_course_context_id}")
                path = f"/1/{context_category_id}/{new_course_context_id}"
                conn.execute(text(
                    f"""
                    UPDATE {context_table}
                    SET path = '{path}'
                    WHERE id = {new_course_context_id}
                    """
                ))

                choice_instance_mapping = {}

                if not choice_df.empty:
                    choice_filtered = choice_df[choice_df["course"] == id].copy()
                    if not choice_filtered.empty:
                        old_choice_ids = choice_filtered["id"].tolist()
                        choice_filtered["course"] = new_course_id
                        choice_filtered = choice_filtered.drop(columns=["id"])
                        try:
                            choice_filtered.to_sql(choice_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {choice_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_choice_ids = [row[0] for row in result]
                            choice_instance_mapping.update(dict(zip(old_choice_ids, new_choice_ids)))
                            if not choice_options_df.empty:
                                choice_options_filtered = choice_options_df[choice_options_df["choiceid"].isin(old_choice_ids)].copy()
                                if not choice_options_filtered.empty:
                                    choice_options_filtered = choice_options_filtered.drop(columns=["id"])
                                    try:
                                        choice_options_filtered["choiceid"] = choice_options_filtered["choiceid"].map(choice_instance_mapping)
                                        choice_options_filtered.to_sql(choice_options_table, conn, if_exists="append", index=False)
                                        logger.info(f"{len(choice_options_filtered)} choice_option(s) inserted for course {new_course_id}.")
                                    except Exception as e:
                                        logger.error(f"Error inserting CHOICE_OPTIONS for course {new_course_id}: {e}")
                            logger.info(f"{len(choice_filtered)} choice(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting CHOICE for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No CHOICE entries found for course {id}.")

                page_instance_mapping = {}

                if not page_df.empty:
                    page_filtered = page_df[page_df["course"] == id].copy()
                    if not page_filtered.empty:
                        old_page_ids = page_filtered["id"].tolist()
                        page_filtered["course"] = new_course_id
                        page_filtered = page_filtered.drop(columns=["id"])
                        if "content_link" in page_filtered.columns:
                            page_filtered = page_filtered.drop(columns=["content_link"])
                        try:
                            page_filtered.to_sql(page_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {page_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_page_ids = [row[0] for row in result]
                            page_instance_mapping.update(dict(zip(old_page_ids, new_page_ids)))
                            logger.info(f"{len(page_filtered)} page(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting PAGE for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No PAGE entries found for course {id}.")
                
                label_instance_mapping = {}

                if not label_df.empty:
                    label_filtered = label_df[label_df["course"] == id].copy()
                    if not label_filtered.empty:
                        old_label_ids = label_filtered["id"].tolist()
                        label_filtered["course"] = new_course_id
                        label_filtered = label_filtered.drop(columns=["id"])
                        try:
                            label_filtered.to_sql(label_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {label_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_label_ids = [row[0] for row in result]
                            label_instance_mapping.update(dict(zip(old_label_ids, new_label_ids)))
                            logger.info(f"{len(label_filtered)} label(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting LABEL for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No LABEL entries found for course {id}.")
                
                url_instance_mapping = {}

                if not url_df.empty:
                    url_filtered = url_df[url_df["course"] == id].copy()
                    if not url_filtered.empty:
                        old_url_ids = url_filtered["id"].tolist()
                        url_filtered["course"] = new_course_id
                        url_filtered = url_filtered.drop(columns=["id"])
                        try:
                            url_filtered.to_sql(url_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {url_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_url_ids = [row[0] for row in result]
                            url_instance_mapping.update(dict(zip(old_url_ids, new_url_ids)))
                            logger.info(f"{len(url_filtered)} url(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting URL for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No URL entries found for course {id}.")
                
                enrol_instance_mapping = {}
                
                if not enrol_df.empty:
                    enrol_filtered = enrol_df[enrol_df["courseid"] == id].copy()
                    if not enrol_filtered.empty:
                        old_enrol_ids = enrol_filtered["id"].tolist()
                        enrol_filtered["courseid"] = new_course_id
                        enrol_filtered = enrol_filtered.drop(columns=["id"])
                        try:
                            enrol_filtered.to_sql(enrol_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {enrol_table} WHERE courseid = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_enrol_ids = [row[0] for row in result]
                            enrol_instance_mapping.update(dict(zip(old_enrol_ids, new_enrol_ids)))
                            logger.info(f"{len(enrol_filtered)} enrol(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting ENROL for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No ENROL entries found for course {id}.")
                
                resource_instance_mapping = {}

                if not resource_df.empty:
                    resource_filtered = resource_df[resource_df["course"] == id].copy()
                    if not resource_filtered.empty:
                        old_resource_ids = resource_filtered["id"].tolist()
                        resource_filtered["course"] = new_course_id
                        resource_filtered = resource_filtered.drop(columns=["id"])
                        try:
                            resource_filtered.to_sql(resource_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {resource_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_resource_ids = [row[0] for row in result]
                            resource_instance_mapping.update(dict(zip(old_resource_ids, new_resource_ids)))
                            logger.info(f"{len(resource_filtered)} resource(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting RESOURCE for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No RESOURCE entries found for course {id}.")
                
                quiz_instance_mapping = {}

                if not quiz_df.empty:
                    quiz_filtered = quiz_df[quiz_df["course"] == id].copy()
                    if not quiz_filtered.empty:
                        old_quiz_ids = quiz_filtered["id"].tolist()
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
                
                forum_instance_mapping = {}

                if not forum_df.empty:
                    forum_filtered = forum_df[forum_df["course"] == id].copy()
                    if not forum_filtered.empty:
                        old_forum_ids = forum_filtered["id"].tolist()
                        forum_filtered["course"] = new_course_id
                        forum_filtered = forum_filtered.drop(columns=["id"])
                        try:
                            forum_filtered.to_sql(forum_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {forum_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_forum_ids = [row[0] for row in result]
                            forum_instance_mapping.update(dict(zip(old_forum_ids, new_forum_ids)))
                            logger.info(f"{len(forum_filtered)} forum(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting FORUM for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No FORUM entries found for course {id}.")
                
                reengagement_instance_mapping = {}

                if not reengagement_df.empty:
                    reengagement_filtered = reengagement_df[reengagement_df["course"] == id].copy()
                    if not reengagement_filtered.empty:
                        old_reengagement_ids = reengagement_filtered["id"].tolist()
                        reengagement_filtered["course"] = new_course_id
                        reengagement_filtered = reengagement_filtered.drop(columns=["id"])
                        try:
                            reengagement_filtered.to_sql(reengagement_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {reengagement_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_reengagement_ids = [row[0] for row in result]
                            reengagement_instance_mapping.update(dict(zip(old_reengagement_ids, new_reengagement_ids)))
                            logger.info(f"{len(reengagement_filtered)} reengagement(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting REENGAGEMENT for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No REENGAGEMENT entries found for course {id}.")
                
                """
                hvp_instance_mapping = {}

                if not hvp_df.empty:
                    hvp_filtered = hvp_df[hvp_df["course"] == id].copy()
                    if not hvp_filtered.empty:
                        old_hvp_ids = hvp_filtered["id"].tolist()
                        hvp_filtered["course"] = new_course_id
                        hvp_filtered = hvp_filtered.drop(columns=["id"])
                        try:
                            hvp_filtered.to_sql(hvp_table, conn, if_exists="append", index=False)
                            result = conn.execute(text(f"SELECT id FROM {hvp_table} WHERE course = :course_id ORDER BY id"), {"course_id": new_course_id}).fetchall()
                            new_hvp_ids = [row[0] for row in result]
                            hvp_instance_mapping.update(dict(zip(old_hvp_ids, new_hvp_ids)))
                            logger.info(f"{len(hvp_filtered)} hvp(s) inserted for course {new_course_id}.")
                        except Exception as e:
                            logger.error(f"Error inserting HVP for course {new_course_id}: {e}")
                    else:
                        logger.warning(f"No HVP entries found for course {id}.")
                """

                if not course_modules_filtered_df.empty:
                    course_modules_filtered_df["course"] = new_course_id
                    # changing the module ids
                    course_modules_filtered_df["module"] = course_modules_filtered_df["module"].map(
                        lambda func: new_modules_map.get(old_modules_map.get(func))
                    )
                    hvp_module_ids = set(course_modules_filtered_df[course_modules_filtered_df["module"] == 27]["id"].tolist())
                    # changing the quizzes instances ids
                    quiz_module_id = new_modules_map.get("quiz")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == quiz_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == quiz_module_id, "instance"
                    ].map(lambda inst: quiz_instance_mapping.get(inst, inst))

                    # changing the pages instances ids
                    page_module_id = new_modules_map.get("page")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == page_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == page_module_id, "instance"
                    ].map(lambda inst: page_instance_mapping.get(inst, inst))

                    # changing the labels instances ids
                    label_module_id = new_modules_map.get("label")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == label_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == label_module_id, "instance"
                    ].map(lambda inst: label_instance_mapping.get(inst, inst))

                    # changing the urls instances ids
                    url_module_id = new_modules_map.get("url")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == url_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == url_module_id, "instance"
                    ].map(lambda inst: url_instance_mapping.get(inst, inst))

                    # changing the resources instances ids
                    resource_module_id = new_modules_map.get("resource")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == resource_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == resource_module_id, "instance"
                    ].map(lambda inst: resource_instance_mapping.get(inst, inst))

                    # changing the forums instances ids
                    forum_module_id = new_modules_map.get("forum")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == forum_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == forum_module_id, "instance"
                    ].map(lambda inst: forum_instance_mapping.get(inst, inst))

                    # changing the reengagements instances ids
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
                    
                    # changing the choices instances ids
                    choice_module_id = new_modules_map.get("choice")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == choice_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == choice_module_id, "instance"
                    ].map(lambda inst: choice_instance_mapping.get(inst, inst))

                    # changing the enrols instances ids
                    enrol_module_id = new_modules_map.get("enrol")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == enrol_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == enrol_module_id, "instance"
                    ].map(lambda inst: enrol_instance_mapping.get(inst, inst))


                    """
                    # changing the hvps instances ids
                    hvp_module_id = new_modules_map.get("hvp")
                    course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == hvp_module_id, "instance"
                    ] = course_modules_filtered_df.loc[
                        course_modules_filtered_df["module"] == hvp_module_id, "instance"
                    ].map(lambda inst: hvp_instance_mapping.get(inst, inst))
                    """

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
                        contional_modules = [7, 17, 18, 21, 27]  # modules to be inserted with specific RESTRICTIONS
                        """
                        7 = feedback
                        17 = quiz
                        18 = resource
                        21 = url
                        27 = hvp
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
                        conn.execute(text(
                            f"""
                            INSERT INTO {context_table} (contextlevel, instanceid, depth, path)
                            VALUES (70, :instanceid, 4, NULL)
                            """),
                            {"instanceid": new_cm_id})

                        # retrieve new context.id
                        result = conn.execute(text(
                            f"""
                            SELECT id FROM {context_table}
                            WHERE contextlevel = 70 AND instanceid = :instanceid
                            ORDER BY id DESC LIMIT 1
                            """),
                            {"instanceid": new_cm_id})
                        
                        new_cm_context_id = result.scalar()

                        # update context path
                        path_cm = f"/1/{context_category_id}/{new_course_context_id}/{new_cm_context_id}"
                        conn.execute(text(
                            f"""
                            UPDATE {context_table}
                            SET path = :path
                            WHERE id = :id
                            """),
                            {"path": path_cm, "id": new_cm_context_id})
                    logger.info(f"{len(course_modules_filtered_df)} course_modules inserted.")

                if not course_sections_df.empty:
                    #logger.debug(f"Course {new_course_id} has {len(course_sections_df)} sections.")
                                        
                    section_sequence_map = dict(zip(course_sections_df["section"], course_sections_df["sequence"]))
                    #logger.info(f"section_sequence_map: {section_sequence_map}")
                    course_sections_df["course"] = new_course_id
                    old_section_ids = course_sections_df["id"].tolist()
                    course_sections_df = course_sections_df.drop(columns=["id"])
                    course_sections_df.loc[
                        course_sections_df["name"].str.strip().str.lower().isin(["conteúdo", "content"]),
                        "availability"
                    ] = '{"op":"|","c":[],"show":false}'
                    course_sections_df["sequence"] = course_sections_df["sequence"].apply(
                        lambda seq: transform_sequence(seq, module_instance_mapping, hvp_module_ids)
                    )
                    #logger.warning(f"HERE -@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ {hvp_module_ids}")
                    #logger.warning(f"HERE ->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> {module_instance_mapping}")

                    #logger.debug(f"Updated sequences: {course_sections_df['sequence'].tolist()}")

                    course_sections_df.to_sql(sections_table, conn, if_exists="append", index=False)
                    logger.info(f"{len(course_sections_df)} section(s) inserted for course {new_course_id}.")

                    # get new sections ids
                    result = conn.execute(text(
                        f"""
                        SELECT id, section FROM {sections_table}
                        WHERE course = :course_id
                        """
                    ), {"course_id": new_course_id}).mappings().fetchall()

                    new_section_ids = [row["id"] for row in result]
                    section_id_mapping = {0: 0}
                    section_id_mapping.update(dict(zip(old_section_ids, new_section_ids)))
                    #logger.debug(f"Section ID mapping: {section_id_mapping}")

                    old_to_new_section_ids = {row["section"]: row["id"] for row in result}

                    # mapping new sectionids directly from old id
                    #logger.debug(f"course_format_options_filtered before .map: {course_format_options_filtered}")
                    course_format_options_filtered["sectionid"] = course_format_options_filtered["sectionid"].fillna(-1).astype(int)
                    course_format_options_filtered["sectionid"] = course_format_options_filtered["sectionid"].map(
                        lambda sid: section_id_mapping.get(sid) if sid > 0 else 0
                    )
                    course_format_options_filtered["courseid"] = new_course_id
                    #logger.debug(f"course_format_options_filtered after .map: {course_format_options_filtered}")

                    course_modules_filtered_df["section"] = course_modules_filtered_df["section"].map(old_to_new_section_ids)

                    section_to_sequence_mapping = {}

                    # Iterate over the old_to_new_section_ids and match it with the sequence values
                    for old_section_index, new_section_id in old_to_new_section_ids.items():
                        sequence_value = section_sequence_map.get(old_section_index, '')
                        section_to_sequence_mapping[new_section_id] = sequence_value
                    
                    # manipulating a specific course_module to adjust the availability and restrictions
                    content_section = conn.execute(text(f"SELECT section FROM {sections_table} WHERE course = :course_id AND name IN ('Conteúdo', 'Content') ORDER BY section ASC LIMIT 1"), {"course_id": new_course_id}).scalar()
                    daughter_content_section = conn.execute(text(f"SELECT sequence FROM {sections_table} WHERE course = :course_id AND section > :content_section ORDER BY section ASC LIMIT 1"), {"course_id": new_course_id, "content_section": content_section}).scalar()
                    
                    if daughter_content_section:
                        sequence_ids = [int(x) for x in daughter_content_section.split(",") if x.strip().isdigit()]
                        hvp_ids_result = conn.execute(text(f"SELECT id FROM {course_modules_table} WHERE course = :course_id AND module = 27"), {"course_id": new_course_id}).fetchall()
                        hvp_ids = {row[0] for row in hvp_ids_result}
                        filtered_sequence_ids = [cm_id for cm_id in sequence_ids if cm_id not in hvp_ids]                
                        
                        if filtered_sequence_ids:
                            availability_placeholder = '{"op":"|","c":[],"show":true}'
                            conn.execute(text(f"UPDATE {course_modules_table} SET availability = :availability WHERE course = :course_id AND id = :id"), {"course_id": new_course_id, "id": filtered_sequence_ids[0], "availability": availability_placeholder})                 

                    #logger.debug(f"Section ID to Sequence Mapping: {section_to_sequence_mapping}")

                    logger.info("All course_sections updated with correct sequences.")

                if not course_format_options_filtered.empty:
                    logger.debug(f"Inserting {len(course_format_options_filtered)} course_format_options for course {new_course_id}.")

                    #logger.debug(f"section_id_mapping keys: {list(section_id_mapping.keys())}")
                    #logger.debug(f"Unique sectionid values before mapping: {course_format_options_filtered['sectionid'].unique().tolist()}")

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
    date_str = datetime.datetime.now().strftime("%m_%d_%Y")
    version = 1
    while True:
        filename = f"loaded_data_{date_str}_v{version}.xlsx"
        output_path = os.path.join(output_dir, filename)
        if not os.path.exists(output_path):
            return output_path
        version += 1


def load(dataframes: Dict[str, pd.DataFrame], conn, new_db):
    logger.debug(f"Starting the loading process...")

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
                    if_table_course(conn, table, ids=[53], dataframes=dataframes, category=1, new_db=new_db)

                """
                if table == "choice":
                    if_table_choice(table, df)
                """
                
                logger.info(f"{table.upper()} has {len(df.columns)} columns: {df.columns.tolist()}.")

                df.to_excel(writer, sheet_name=table, index=False)
            logger.info(f"File successfully saved: {os.path.basename(output_path)}.")
        except Exception as e:
            logger.error(f"Error creating the xlsx file: {e}.")
    logger.info(f"End of loading process.")
