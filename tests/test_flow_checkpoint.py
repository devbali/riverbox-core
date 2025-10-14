import sys
import os
import tempfile

import json
from uuid import uuid4
from core.FlowExecution import FlowExecution

def test_get_flow_from_checkpoint():
    # Create a test flow structure
    riverbox_flow = {
        "metadata": {
            "language": "python",
            "version": "3.11"
        },
        "flow": {
            "cubes": [
                {
                    "id": "cube1",
                    "kind": "REGULAR",
                    "name": "test_cube",
                    "code": "x = 1\nprint(x)",
                    "start-edges": []
                }
            ]
        },
        "env": {},
        "tags": [],
        "tag-stack": [[]]
    }

    # Create a temporary directory for checkpoints
    with tempfile.TemporaryDirectory() as temp_dir:
        # Initialize FlowExecution with checkpoint directory
        flow_exec = FlowExecution(
            riverbox_flow,
            {
                "execution-id": str(uuid4()),
                "flow-id": str(uuid4()),
                "invocation-id": str(uuid4())
            },
            lambda x: None,  # dummy callback
            "FULL",
            None,
            None,
            temp_dir
        )

        # Mock a state dump
        flow_exec.global_vars = {"test_var": 42}
        flow_exec.global_execution_count = 1
        flow_exec._dump_state(0)

        # Retrieve flow from checkpoint
        restored_flow = flow_exec.get_flow_from_checkpoint(1)

        # Test assertions
        assert restored_flow is not None
        assert restored_flow.global_vars == {"test_var": 42}
        assert restored_flow.global_execution_count == 1
        assert restored_flow.riverbox_metadata == riverbox_flow["metadata"]
        assert len(restored_flow.cubes) == 1
        assert restored_flow.cubes[0].id == "cube1"
        assert restored_flow.dump_state_folder == temp_dir

def test_get_flow_from_checkpoint_no_dump_folder():
    # Test when no dump folder is specified
    riverbox_flow = {
        "metadata": {"language": "python", "version": "3.11"},
        "flow": {"cubes": []},
        "env": {},
        "tags": [],
        "tag-stack": [[]]
    }

    flow_exec = FlowExecution(
        riverbox_flow,
        {
            "execution-id": str(uuid4()),
            "flow-id": str(uuid4()),
            "invocation-id": str(uuid4())
        },
        lambda x: None,
        "FULL",
        None,
        None,
        None  # No dump folder specified
    )

    restored_flow = flow_exec.get_flow_from_checkpoint(1)
    assert restored_flow is None

def test_get_flow_from_checkpoint_no_previous_dump():
    # Test when dump folder exists but no previous checkpoint exists
    with tempfile.TemporaryDirectory() as temp_dir:
        riverbox_flow = {
            "metadata": {"language": "python", "version": "3.11"},
            "flow": {"cubes": []},
            "env": {},
            "tags": [],
            "tag-stack": [[]]
        }

        flow_exec = FlowExecution(
            riverbox_flow,
            {
                "execution-id": str(uuid4()),
                "flow-id": str(uuid4()),
                "invocation-id": str(uuid4())
            },
            lambda x: None,
            "FULL",
            None,
            None,
            temp_dir
        )

        # Set execution count but don't create dump
        flow_exec.global_execution_count = 1
        
        restored_flow = flow_exec.get_flow_from_checkpoint(1)
        assert restored_flow is None

def test_debug_execution_with_checkpoint():
    # Create a test flow with multiple cells that modify state
    riverbox_flow = {
        "metadata": {
            "language": "python",
            "version": "3.11"
        },
        "flow": {
            "cubes": [
                {
                    "id": "cube1",
                    "kind": "REGULAR",
                    "name": "first_cube",
                    "code": "x = 1\ny = 2\nresult = x + y",
                    "start-edges": []
                },
                {
                    "id": "cube2",
                    "kind": "REGULAR",
                    "name": "second_cube",
                    "code": "x = 10\nresult = result * x",  # Should make result = 30
                    "start-edges": [{
                        "id": "edge1",
                        "end": "cube1",
                        "end-arg-key": "result",
                        "kind": "STRAIGHT",
                        "start-arg-key": None
                    }]
                },
                {
                    "id": "cube3",
                    "kind": "REGULAR",
                    "name": "third_cube",
                    "code": "result = result * 2",  # Should make result = 60
                    "start-edges": [{
                        "id": "edge2",
                        "end": "cube2",
                        "end-arg-key": "result",
                        "kind": "STRAIGHT",
                        "start-arg-key": None
                    }]
                }
            ]
        },
        "env": {},
        "tags": [],
        "tag-stack": [[]]
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        execution_results = []
        def callback(message):
            if message["type"] == "SUCCESSFUL_CUBE_EXECUTION":
                execution_results.append(message)
        
                # Initialize flow execution in DEBUG_START mode
            flow_exec = FlowExecution(
                riverbox_flow,
                {
                    "execution-id": str(uuid4()),
                    "flow-id": str(uuid4()),
                    "invocation-id": str(uuid4())
                },
                callback,
                "DEBUG_START",
                None,  # Initially no cube_id as we're just starting
                None,
                temp_dir
            )
            # Start execution with empty args to initialize
            flow_exec.execute({})

            # Create new execution for first cube
            cube1_exec = FlowExecution(
                riverbox_flow,
                {
                    "execution-id": str(uuid4()),
                    "flow-id": str(uuid4()),
                    "invocation-id": str(uuid4())
                },
                callback,
                "DEBUG_NEXT",
                "cube1",  # Run cube1
                flow_exec,  # Pass the previous execution state
                temp_dir
            )
            cube1_exec.execute({}, True)
            # At this point global_vars should have x=1, y=2, result=3
            assert cube1_exec.global_vars["x"] == 1
            assert cube1_exec.global_vars["y"] == 2
            assert cube1_exec.global_vars["result"] == 3
            assert cube1_exec.global_execution_count == 1

            # Create new execution for second cube
            cube2_exec = FlowExecution(
                riverbox_flow,
                {
                    "execution-id": str(uuid4()),
                    "flow-id": str(uuid4()),
                    "invocation-id": str(uuid4())
                },
                callback,
                "DEBUG_NEXT",
                "cube2",  # Run cube2
                cube1_exec,  # Pass the previous execution state
                temp_dir
            )
            cube2_exec.execute({}, True)
            # At this point global_vars should have x=10, y=2, result=30
            assert cube2_exec.global_vars["result"] == 30
            assert cube2_exec.global_vars["x"] == 10
            assert cube2_exec.global_vars["y"] == 2
            assert cube2_exec.global_execution_count == 2

            # Checkpoint at execution count 2
            cube2_exec._dump_state(2)

            # Create new execution for third cube
            cube3_exec = FlowExecution(
                riverbox_flow,
                {
                    "execution-id": str(uuid4()),
                    "flow-id": str(uuid4()),
                    "invocation-id": str(uuid4())
                },
                callback,
                "DEBUG_NEXT",
                "cube3",  # Run cube3
                cube2_exec,  # Pass the previous execution state
                temp_dir
            )
            cube3_exec.execute({}, True)
            assert cube3_exec.global_vars["result"] == 60
            assert cube3_exec.global_execution_count == 3

            # Now restore from checkpoint at execution count 2
            restored_flow = cube3_exec.get_flow_from_checkpoint(2)
            assert restored_flow is not None
            assert restored_flow.global_vars["result"] == 30
            assert restored_flow.global_vars["x"] == 10
            assert restored_flow.global_vars["y"] == 2
            assert restored_flow.global_execution_count == 2

            # Verify that the cubes are properly restored
            assert len(restored_flow.cubes) == 3
            assert restored_flow.cubes[2].code == "result = result * 2"
