#!/usr/bin/env python3
import sys
import os
import path
# import pytest

# Dummytest which will always succeed - must be replaced by real tests

def test_dummy():
    assert True

from dlr_limit_consumer import add

def test_add():
    assert add(4, 2) == 6