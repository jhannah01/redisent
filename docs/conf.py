from recommonmark.transform import AutoStructify

import os
import builtins
import sys
import sphinx_rtd_theme

sys.path.insert(0, os.path.abspath('../src'))

from redisent import __version__ as redisent_version

# -- Project information -----------------------------------------------------

project = 'redisent'
author = 'Jon Hannah'
copyright = f'2021, {author}'

release = redisent_version

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    'sphinx_markdown_tables',
    'recommonmark',
    'IPython.sphinxext.ipython_console_highlighting',
    'IPython.sphinxext.ipython_directive'
]

templates_path = ['_templates']
exclude_patterns = []

autoclass_content = 'both'
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# autodoc_type_aliases = {
#     'RedisType': 'redisent.common.RedisType',
#     'RedisPoolType': 'redisent.common.RedisPoolType'
# }

html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
html_static_path = ['_static']
pygments_style = 'monokai'

nitpick_classes = ['redis.Redis', 'redis.ConnectionPool', 'redis.connection.ConnectionPool', 'datetime', 'redis.client.Redis', 'redislite.client.Redis', 'redislite.client.StrictRedis']

nitpicky = True
nitpick_ignore = []

for name in dir(builtins):
    nitpick_ignore = [('py:class', name)]

for cls_name in nitpick_classes:
    nitpick_ignore.append(('py:class', cls_name))

source_parsers = {
    '.md': 'recommonmark.parser.CommonMarkParser',
}

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'redis': ('https://redis-py.readthedocs.io/en/stable/', None),
    'redislite': ('https://redislite.readthedocs.io/en/latest/', None)
}


def setup(app):
    recom_cfg = {'auto_toc_tree_section': 'Contents',
                 'enable_eval_rst': True}
    app.add_config_value('recommonmark_config', recom_cfg, True)
    app.add_transform(AutoStructify)
    app.add_css_file('custom.css')

