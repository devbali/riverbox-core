from core.run import main
import json
from uuid import uuid4
from . import multi_language

with open("tests/debug.rbx", "r") as f:
  riverbox = json.load(f)

@multi_language("python")
def test_debug_solo (riverbox=riverbox, offset = 10, num = 5):
  got_execution_indexes = []
  got_results = []

  def callback (m):
    print(m)
    if m["type"] == "START_CUBE_EXECUTION":
      got_execution_indexes.append(m["global-execution-count"])
    
    if m["type"] == "SUCCESSFUL_CUBE_EXECUTION":
      got_results.append(m["return-value"])
  
  s = main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_ONLY", cube_id="9b46ee41-ab07-43b8-809b-43650dca745e", args={"initial_value":offset})
  s = main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_ONLY", cube_id="29706140-cc03-4141-a5bb-4df4763bc0cc", debug_state=s)

  got_execution_indexes = []
  got_results = []

  for _ in range(num):
    s = main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_ONLY", cube_id="8f39f94f-8df6-45d6-a307-10d1af518e93", debug_state=s)
  
  assert got_execution_indexes == list(range(3,num+3))
  assert got_results == [str(offset + i) for i in range(1, num + 1)]

@multi_language("python")
def test_debug_solo_repropagation (riverbox=riverbox):
  # first remove edge
  riverbox_bad = {**riverbox}
  riverbox_bad["flow"] = {**riverbox["flow"]}
  riverbox_bad["flow"]["cubes"] = [{**c} for c in riverbox["flow"]["cubes"]]
  riverbox_bad["flow"]["cubes"][2]["start-edges"] = []
  
  start_cube_counter = 0
  successful_cube_counter = 0
  cube_error_counter = 0
  
  def callback (m):
    nonlocal start_cube_counter, successful_cube_counter, cube_error_counter
    print(m)
    if m["type"] == "START_CUBE_EXECUTION":
      start_cube_counter += 1
    
    if m["type"] == "SUCCESSFUL_CUBE_EXECUTION":
      successful_cube_counter += 1
    
    if m["type"] == "CUBE_EXECUTION_ERROR":
      cube_error_counter += 1
  
  e_meta = {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}
  s = main(riverbox_bad, e_meta, callback, "DEBUG_ONLY", cube_id="9b46ee41-ab07-43b8-809b-43650dca745e", args={"initial_value":2})
  s = main(riverbox_bad, e_meta, callback, "DEBUG_ONLY", cube_id="29706140-cc03-4141-a5bb-4df4763bc0cc", debug_state=s)
  s = main(riverbox_bad, e_meta, callback, "DEBUG_ONLY", cube_id="8f39f94f-8df6-45d6-a307-10d1af518e93", debug_state=s)

  assert start_cube_counter == 3
  assert successful_cube_counter == 3

  s = main(riverbox_bad, e_meta, callback, "DEBUG_NEXT", debug_state=s)
  s = main(riverbox_bad, e_meta, callback, "DEBUG_ONLY", cube_id="4339f94f-8df6-45d6-a307-10d1af518e93", debug_state=s)
  
  assert start_cube_counter == 5
  assert cube_error_counter == 2
  
  # edge has been "added" now
  print("rbx bad", riverbox["flow"]["cubes"][2])

  s = main(riverbox, e_meta, callback, "DEBUG_ONLY", cube_id="4339f94f-8df6-45d6-a307-10d1af518e93", debug_state=s)
  
  assert start_cube_counter == 6
  assert cube_error_counter == 2
  assert successful_cube_counter == 4

@multi_language("python")
def test_debug_upto (riverbox=riverbox, offset=10, num=5):
  results = []
  execution_counts = []
  def callback (m):
    print(m)
    if m["type"] == "START_CUBE_EXECUTION":
      execution_counts.append(m["global-execution-count"])

    if m["type"] == "SUCCESSFUL_CUBE_EXECUTION":
      if m["return-value"] != 'None':
        results.append(int(m["return-value"]))

  s = main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_UPTO", cube_id="8f39f94f-8df6-45d6-a307-10d1af518e93", args={"initial_value":offset})
  assert results == [offset, offset + 1]
  assert execution_counts == list(range(1,4))

  for i in range(num):
    results = []
    execution_counts = []
    s = main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_UPTO", cube_id="8f39f94f-8df6-45d6-a307-10d1af518e93", args={"initial_value":offset*2}, debug_state=s)
    assert results == [offset*2, offset*2 + 1]
    assert execution_counts == list(range(4 + 3 * i, 7 + 3 * i))
  
  for i in range(num):
    results = []
    execution_counts = []
    s = main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_ONLY", cube_id="8f39f94f-8df6-45d6-a307-10d1af518e93", args={"initial_value":offset*2}, debug_state=s)
    assert results == [offset*2 + i + 2]
    assert execution_counts == [4 + 3 * num + i]

@multi_language("python")
def test_debug_next (riverbox=riverbox, offset=10):
  results = set()
  execution_counts = []
  def callback (m):
    print(m)
    if m["type"] == "START_CUBE_EXECUTION":
      execution_counts.append(m["global-execution-count"])

    if m["type"] == "SUCCESSFUL_CUBE_EXECUTION":
      if m["return-value"] != 'None':
        results.add(m["return-value"])

  s = main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_UPTO", 
           cube_id="8f39f94f-8df6-45d6-a307-10d1af518e93", args={"initial_value":offset})
  s = main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_ONLY", 
           cube_id="4339f94f-8df6-45d6-a307-10d1af518e93", debug_state=s)

  assert results == {str(i) for i in [offset, offset + 1]}.union({str(list(range(offset+1)))})
  assert execution_counts == list(range(1,5))

  results = set()
  execution_counts = []
  main(riverbox, {"execution-id": uuid4(), "flow-id": uuid4(), "invocation-id": uuid4()}, callback, "DEBUG_NEXT", debug_state=s)
  assert results == {str(i * 1000) for i in range(offset+1)}.union({str(offset)})
