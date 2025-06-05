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
- adapt 'customcert' insertion logic to manipulate and insert as many instances as i want to
- adapt 'customcert_templates' to fit the 'agroupment' needed to be able to get that customcert instance
- adapt 'load()' call to use a list of 'customcert_templates' ids (same as course_ids) instead of a string
- adapt 'reengagement' logic to one specific instance target specific 'course_module' (the first one of the 'content'/'conteúdo' section)