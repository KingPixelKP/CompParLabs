"""spark-submit entry point: spark-submit proj1/submit.py <proj1 arguments>.

The package's __main__.py uses a relative import, so it cannot be submitted as a file.
Only the driver runs proj1 code (no UDFs), so executors do not need the package.
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "src"))

from proj1 import main

main()
