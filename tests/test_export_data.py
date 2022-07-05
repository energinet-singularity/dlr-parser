#!/usr/bin/env python3
import os

# Import modules that should be included for testing
import app.dlr_limit_consumer

# Test format of the file created by the funtion export_to_file
def test_file(tmpdir):
    file_path = tmpdir.mkdir("data").strpath
    file_name = os.path.join(file_path, "limits.csv")
    shape_data = True
    data = [
        {
            "mrid": "test-mrid",
            "steady_state_rating": 10.10,
            "emergency_rating_15min": 20.20,
            "load_shedding": 20.20,
            "calculation_time": "2021-09-13T13:23:15Z",
        },
        {
            "mrid": "test-mrid-2",
            "steady_state_rating": 30.30,
            "emergency_rating_15min": 40.40,
            "load_shedding": 40.40,
            "calculation_time": "2021-09-13T13:23:15Z",
        },
    ]
    app.dlr_limit_consumer.export_to_file(file_name, data, shape_data)
    test = open(file_name, "r").readlines()
    assert test[0] == "I,SEGLIM,LINESEG_MRID,LIMIT1,LIMIT2,LIMIT3\n"
    assert test[1] == "D,SEGLIM,testmrid,10.1,20.2,20.2\n"
    assert test[2] == "D,SEGLIM,testmrid2,30.3,40.4,40.4\n"
