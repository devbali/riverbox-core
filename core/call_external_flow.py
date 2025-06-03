import uuid

uuids = set()

def get_uuid ():
    uuid_str = None
    while uuid_str is None or uuid_str in uuids:
        uuid_str = str(uuid.uuid4())

    return uuid_str

def call_external_flow (body_flow, parent_metadata: dict, callback, args, global_execution_count, layer, env):
    from .FlowExecution import FlowExecution

    if body_flow["run-on-same"]:
        parent_metadata_for_child = parent_metadata.copy()
        parent_metadata_for_child["execution-id"] = get_uuid()
        parent_metadata_for_child["flow-id"] = body_flow["sub-flow-id"]
        body = {}
        body["flow"] = body_flow
        body["metadata"] = body_flow["metadata"]
        body["env"] = body_flow["env"]
        sub_flow = FlowExecution(body, parent_metadata_for_child, callback, "FULL", None, None)
        return sub_flow.execute(args, worker_assigned=True)

    # Send it out to cluster manager
    # For now, wait till execution is complete
    return callback({
        "type": "TRACK_EXTERNAL_SUBFLOW",
        "cube-id": body_flow["id"],
        "global-execution-count": global_execution_count,
        "layer": layer,
        "args": args,
        "parent-env": env,
        "invocation-id": parent_metadata["invocation-id"]
    })
