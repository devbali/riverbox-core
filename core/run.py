from .FlowExecution import FlowExecution

def main(riverbox_flow: dict, current_execution_metadata: dict, callback_function, execution_type, cube_id=None, args={}, debug_state: FlowExecution= None, return_results=False, dump_state_folder=None, flow_registry=None):
  flow = FlowExecution(riverbox_flow, current_execution_metadata, callback_function, execution_type, cube_id, debug_state, dump_state_folder=dump_state_folder, flow_registry=flow_registry if flow_registry is not None else {})
  print("Starting execution of flow with metadata:", current_execution_metadata)
  results = flow.execute(args, parent_cubeexecution_id=current_execution_metadata.get("parent-cube-execution-id", None))
  
  if return_results:
    return results
  else:
    return flow
