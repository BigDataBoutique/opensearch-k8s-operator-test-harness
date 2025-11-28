#!/usr/bin/env python3
"""
Main entry point for running oko-test-harness as a module.
This allows running the tool without installing it as a package:
    python -m oko_test_harness [command] [args]
"""

from .cli import main

if __name__ == "__main__":
    main()
