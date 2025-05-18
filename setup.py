from pathlib import Path
from setuptools import setup, find_packages


NAME = 'mds.main'
DESCRIPTION = 'Package for running the data pipeline for Modern Data Stack with Azure and Databricks'
URL = ''
AUTHOR = 'Godwin AMEGAH'
EMAIL = 'komlan.godwin.amegah@gmail.com'
REQUIRES_PYTHON = '>=3.9'

HERE = Path(__file__).parent
REQUIRES_FILE = HERE / 'requirements.txt'
REQUIRED = [i.strip() for i in open(REQUIRES_FILE) if not i.startswith('#')]

for line in open('mds/__init__.py'):
    line = line.strip()
    if '__version__' in line:
        context = {}
        exec(line, context)
        VERSION = context['__version__']
try:
    with open(HERE / "README.md", encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION


setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author_email=EMAIL,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    url=URL,
    python_requires=REQUIRES_PYTHON,
    install_requires=REQUIRED,
    packages=[p for p in find_packages(exclude=("*tests.*", "*tests")) if p.startswith('mds')],
    include_package_data=True,
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    entry_points={
        'console_scripts': [
            'mds = mds.main:main'
        ]
    }
)
