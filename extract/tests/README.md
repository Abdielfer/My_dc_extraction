# How to run tests?     

```
REM open a command prompt 

REM  Activate conda 
conda activate datacube-extract

REM setting certificat
SET REQUESTS_CA_BUNDLE=.... /cacert.pem

REM set adresse du dépot 
set path_dc_extract=.... /dc_extract/extract

REM  Pour tous les tests
pytest -q --disable-pytest-warnings %path_dc_extract%/tests/

REM  Pour unit test
pytest -q --disable-pytest-warnings %path_dc_extract%/tests/test_unit.py

REM  Pour unit integration
pytest -q --disable-pytest-warnings %path_dc_extract%/tests/test_integration.py

REM  Pour unit functional
pytest -q --disable-pytest-warnings %path_dc_extract%/tests/test_functional.py

REM  Pour une fonction ou class
pytest -q --disable-pytest-warnings -k test_add_overviews %path_dc_extract%/tests/test_integration.py
```

