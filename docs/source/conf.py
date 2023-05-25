# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# the following to lines make sure that the pipelines folder is the root
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join('..', '..', 'pipelines')))

project = 'pipelines'
copyright = '2023, dlthub'
author = 'dlthub'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'furo'
html_static_path = ['_static']
extensions = ['sphinx.ext.autodoc']
extensions = ['sphinx.ext.napoleon']
