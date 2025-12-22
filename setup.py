from pathlib import Path
from setuptools import setup, find_packages


NAME = 'daie'
DESCRIPTION = 'daie package'

URL = 'https://github.com/goamegah/daie'
AUTHOR = 'Godwin AMEGAH'
EMAIL = 'komlan.godwin.amegah@gmail.com'
REQUIRES_PYTHON = '>=3.11'

import re
version_file = open('daie/__init__.py').read()
version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
if version_match:
    VERSION = version_match.group(1)
else:
    raise RuntimeError("Unable to find version string.")

HERE = Path(__file__).parent

try:
    with open(HERE / "README.md", encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

REQUIRES_FILE = HERE / 'requirements.txt'
REQUIRED = [i.strip() for i in open(REQUIRES_FILE) if not i.startswith('#')]


setup(
    name = NAME,
    version = VERSION,
    description = DESCRIPTION,
    author_email = EMAIL,
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    author = AUTHOR,
    url = URL,
    install_requires = REQUIRED,
    packages = find_packages(exclude = ('*tests.*', '*tests*')),
    classifiers = [
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Programming Language :: Python :: 3.11',
        'Operating System :: OS Independent'
    ],
    entry_points = {
        'console_scripts': [
            'daie = daie.main:main',
        ],
    },
)