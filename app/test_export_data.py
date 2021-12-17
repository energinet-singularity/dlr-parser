#!/usr/bin/env python3
import sys
import os
import path
# import pytest

# Dummytest which will always succeed - must be replaced by real tests

def test_dummy():
    assert True

import dlr_limit_consumer

def test_add():
    assert dlr_limit_consumer.add(4, 2) == 6