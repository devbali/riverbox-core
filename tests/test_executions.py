
import sys
sys.path.append(".")

from core.run import main
import json
from uuid import uuid4


with open("tests/code_exec.rbx", "r") as f:
  riverbox = json.load(f)

def test_plain_python ():
    cube_of_interest = "8f39f94f-8df6-45d6-a307-10d1af518e93"
    expected_output = "2\n"
    received_output = ""

    riverbox["language"] = "python"
    output_num = 0
    def callback(m):
        nonlocal output_num, received_output
        print(m)
        if m["type"] == "SUCCESSFUL_CUBE_EXECUTION" and output_num == 1:
           received_output = m["console-output"]
        elif m["type"] == "SUCCESSFUL_CUBE_EXECUTION":
           output_num += 1

    main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "UPTO", cube_of_interest)
    assert expected_output == received_output

# def test_ipynb ():
#     cube_of_interest = "8f39f94f-8df6-45d6-a307-10d1af518e93"
#     expected_output = "2\n"
#     received_output = ""

#     riverbox["metadata"]["language"] = "ipynb"
#     output_num = 0
#     def callback(m):
#         nonlocal output_num, received_output
#         print(m)
#         if m["type"] == "SUCCESSFUL_CUBE_EXECUTION" and output_num == 1:
#            received_output = m["console-output"]
#         elif m["type"] == "SUCCESSFUL_CUBE_EXECUTION":
#            output_num += 1

#     main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "UPTO", cube_of_interest)
#     assert expected_output == received_output
