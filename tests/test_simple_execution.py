from core.run import main
from . import multi_language

import json
from uuid import uuid4
import time

with open("tests/test_riverbox.rbx", "r") as f:
  test_riverbox = json.load(f)

with open("tests/object_map.rbx", "r") as f:
  simple_riverbox = json.load(f)

@multi_language("python")
def test_object_map (simple_riverbox=simple_riverbox):  
  def callback (m):
    print(m)
    if m["type"] == "EXECUTION_DONE":
      assert m["results"] == {"main-result": [1,2]}

  main(simple_riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "FULL")

@multi_language("python")
def test_run (test_riverbox=test_riverbox):
  main_execution = uuid4()
  def callback (m):
    assert m["type"] != "CUBE_EXECUTION_ERROR", m["error-message"]
    sub_results = [{'straight-result': ['bears'] * 10 }, {'straight-result': ['dogs'] * 10 }]
    if m["type"] == "EXECUTION_DONE" and m["execution-id"] == main_execution:
      print(m["results"])
      assert m["results"] == {'result_str': 'ChatGPT', 'nest-result': [['bears'] * 10, ['dogs']*10]}
    elif m["type"] == "EXECUTION_DONE":
      assert m["results"] in sub_results
      sub_results.remove(m["results"])

  start = time.time()
  main(test_riverbox, {"execution-id": main_execution, "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "FULL")
  end = time.time()
  # should be marginally above 0.4 seconds
  assert test_riverbox["metadata"]["language"] != "python" or end-start < 0.5

  # should be worse, but not by 10x
  assert test_riverbox["metadata"]["language"] != "ipynb" or end-start < 2

@multi_language("python")
def test_solo (test_riverbox=test_riverbox):
  execution = uuid4()
  successful_executions = 0
  def callback(m):
    nonlocal successful_executions
    assert m["type"] != "CUBE_EXECUTION_ERROR"
    if m["type"] == "SUCCESSFUL_CUBE_EXECUTION":
      assert m["return-value"] == "Claude"
      successful_executions += 1
  main(test_riverbox, {"execution-id": execution, "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "ONLY", "29706140-cc03-4141-a5bb-4df4763bc0cc")
  assert successful_executions == 1

@multi_language("python")
def test_upto (test_riverbox=test_riverbox):
  execution = uuid4()
  rvs = ['Write a joke about bears',
          "Write a joke about dogs", 
          "ChatGPT", "ChatGPT",
          "Bears are funny (Claude)",
          "Bears are funny (GPT)",
          "None", "1", "[['bears'], ['dogs']]"]
  successful_executions = 0
  def callback(m):
    nonlocal successful_executions
    print(m)
    assert m["type"] != "CUBE_EXECUTION_ERROR"
    if m["type"] == "SUCCESSFUL_CUBE_EXECUTION":
      print(m)
      assert m["return-value"] in rvs
      successful_executions += 1

  main(test_riverbox, {"execution-id": execution, "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "UPTO", "29706140-cc03-4141-a5bb-4df4763bc0cc")
  assert successful_executions == len(rvs)
