rm dist/*.whl
python setup.py install
python setup.py bdist_wheel
python -m twine upload --repository-url https://upload.pypi.org/legacy/ dist/*.whl
