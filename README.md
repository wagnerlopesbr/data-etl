# data-etl

## Note
Requires Python's 3.11 version.

## Setting the project up
If it's your first time running the project on your computer, run:
```bash
./requirements.sh
```

If it's not, just run to activate your Virtual Environment:

- Windows:
```bash
source .venv/Scripts/activate
```
- Unix/Linux:
source .venv/bin/activate
```bash
source .venv/bin/activate
```

### reminder (to Wagner; to be deleted)
- create 'customcert_templates' basically to each different 'course_category' (not able to use the same template for all courses)
- adapt 'reengagement' logic to one specific instance target specific 'course_module' (the first one of the 'content'/'conteúdo' section)
- adapt 'load.py' logic to be called more than one time (each call for each category)
- adapt 'if_table_course' logic to be able to be called with optional parameteres related to course 'customcert_template'