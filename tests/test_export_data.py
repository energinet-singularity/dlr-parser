#!/usr/bin/env python3
# import sys
# import os
# import pytest

# Import modules that should be included for testing
import app.dlr_limit_consumer

# Dummytest which will always succeed - must be replaced by real tests

def test_dummy():
    assert True

def test_add():
    assert app.dlr_limit_consumer.add(4, 2) == 6