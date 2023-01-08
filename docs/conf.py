"""Sphinx configuration."""
project = "Dbt Core Interface"
author = "Alex Butler"
copyright = "2023, Alex Butler"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
