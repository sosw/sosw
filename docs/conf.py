# Configuration file for the Sphinx documentation builder of `sosw`.
#
# The docs are built strictly with warnings treated as errors:
#     python -m sphinx -W -a -b html docs sosw-rtd
#
# Full list of options: https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

from importlib.metadata import PackageNotFoundError, version as package_version


# Repository root on sys.path: autodoc imports `sosw` (and `docs.ext` / `docs.hello`) from sources.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# -- Project information ---------------------------------------------------

project = 'sosw'
copyright = '2025, sosw core contributors'
author = 'Nikolay Grishchenko'

try:
    release = package_version('sosw')
except PackageNotFoundError:
    # Building from a source checkout without the package installed.
    release = '3.0.0'

# The short X.Y version.
version = '.'.join(release.split('.')[:2])


# -- General configuration -------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'docs.ext.hidden_code_block',
    'sphinx_sitemap',
]

templates_path = ['_templates']
source_suffix = {'.rst': 'restructuredtext'}
root_doc = 'index'
language = 'en'

exclude_patterns = ['_build']


# -- Options for HTML output -----------------------------------------------

html_theme = 'furo'
html_title = 'sosw — a framework for bootstrapping AWS Lambda functions'
html_short_title = 'sosw'

html_theme_options = {
    'light_logo':        'images/logo/full_sosw_logo_black.png',
    'dark_logo':         'images/logo/full_sosw_logo_white.png',
    'sidebar_hide_name': True,
    # The links are root-relative so that the banner works from every page of the deployed site.
    'announcement':      'sosw is a framework for bootstrapping AWS Lambda functions. '
                         'Upgrading from 0.7.x? See the '
                         '<a href="/migration_3_0.html">migration guide</a> and the '
                         '<a href="/previous/0.7.51/index.html">previous documentation</a>.',
}

html_favicon = '_static/images/favicon/favicon.ico'
html_static_path = ['_static']

# Files copied verbatim to the root of the built docs (served from https://docs.sosw.app/).
# `../previous_versions` carries the compiled documentation archives of the previous major
# releases (e.g. previous/0.7.51/), published at the site root by the same S3 sync.
html_extra_path = [
    'robots.txt',
    'manifest.json',
    'browserconfig.xml',
    'favicon.ico',
    'favicon-96x96.png',
    '../previous_versions',
]

html_baseurl = 'https://docs.sosw.app/'

# sphinx-sitemap: flat URLs matching the deployed docs (no language / version prefixes).
sitemap_url_scheme = '{link}'
sitemap_excludes = [
    'search.html',
    'genindex.html',
]


# -- Linkcheck ---------------------------------------------------------------

# Relative links that exist only in the deployed output (copied via `html_extra_path`),
# so the linkcheck builder cannot resolve them from the sources.
linkcheck_ignore = [
    r'^index\.html$',
    r'^previous/',
    # The AWS signup portal answers 401 to non-browser clients.
    r'^https://portal\.aws\.amazon\.com/billing/signup',
]

# Anchors starting with '/' are client-side SPA routes (AWS consoles), invisible to linkcheck.
linkcheck_anchors_ignore = ['^!', '^/']


# -- Autodoc ---------------------------------------------------------------

autodoc_member_order = 'bysource'
