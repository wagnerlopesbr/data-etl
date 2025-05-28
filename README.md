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
- adapt 'load.py' logic to be called more then one time (each call for each category)
- adapt 'if_table_course' logic to be able to be called with optional parameteres related to course language
- try to migrate some of the 'hvp' elements (only games) as actually 'hvp' too