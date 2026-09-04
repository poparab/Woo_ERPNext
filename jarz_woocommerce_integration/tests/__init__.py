"""Test package marker.

Every other subpackage in this app carries an ``__init__.py``; this directory did
not, which makes it a PEP 420 namespace package.  ``unittest discover`` dropped
namespace-package support in Python 3.11, so ``python -m unittest discover`` walked
straight past this directory and reported zero tests — a green run that had
executed nothing.  ``bench run-tests`` imports by dotted module name and was
unaffected, which is why the gap stayed invisible.

Keep this file.
"""
