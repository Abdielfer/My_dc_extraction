# How to run tests?     

1. **Open a conda prompt**

2. **Activate conda**\
`conda activate datacube-extract`

3. **Setting certificat**\
`SET REQUESTS_CA_BUNDLE={local_path_to_SLL_certificate}/cacert.pem`

4. **Set adresse du dépot of test**\
`SET path_dc_extract={local_path_to_git_folder}/dc_extract/extract_cog/tests`

5. **Run tests**
    - To run all tests\
    `pytest -q --disable-pytest-warnings %path_dc_extract%`

    - To run only unit tests\
    `pytest -q --disable-pytest-warnings %path_dc_extract%/test_unit.py`

    - To run only integration tests\
    `pytest -q --disable-pytest-warnings %path_dc_extract%/test_integration.py`

    - To run only functional tests\
    `pytest -q --disable-pytest-warnings %path_dc_extract%/test_functional.py`

    - To run a specific function or class inside one of the test file\
    `pytest -q --disable-pytest-warnings -k test_add_overviews %path_dc_extract%/test_integration.py`
