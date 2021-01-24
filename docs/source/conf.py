from recommonmark.transform import AutoStructify

import os
import sys
import sphinx_rtd_theme

sys.path.insert(0, os.path.abspath('..'))


# -- Project information -----------------------------------------------------

project = 'redisent'
copyright = '2021, Jon'
author = 'Jon'

# The full version, including alpha/beta/rc tags
release = '0.0.1'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    'recommonmark',
    'IPython.sphinxext.ipython_console_highlighting',
    'IPython.sphinxext.ipython_directive'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

autoclass_content = 'both'
autodoc_default_options = {
        'members': True,
        'member-order': 'bysource',
        'special-members': '__init__',
        'undoc-members': True,
        'exclude-members': '__weakref__'
}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
pygments_style = 'monokai'

nitpick_classes = ['redis.ConnectionPool', 'redis.connection.ConnectionPool', 'aioredis.pool.ConnectionsPool', 'asyncio.events.AbstractEventLoop', 'datetime']

nitpicky = True
nitpick_ignore = []

for cls_name in nitpick_classes:
    nitpick_ignore.append(('py:class', cls_name))
autodoc_mock_imports = ['redis', 'aioredis', 'asyncio', 'datetime']

# -- Extension configuration -------------------------------------------------
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

source_suffix = ['.rst', '.md']


def setup(app):
    recom_cfg = {'auto_toc_tree_section': 'Contents',
                 'enable_eval_rst': True}
    app.add_config_value('recommonmark_config', recom_cfg, True)
    app.add_transform(AutoStructify)
    app.add_css_file('custom.css')


# -- Options for intersphinx extension ---------------------------------------

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'aioredis': ('https://aioredis.readthedocs.io/en/latest/', None),
    'redis': ('https://redis-py.readthedocs.io/en/stable/', None),
}
