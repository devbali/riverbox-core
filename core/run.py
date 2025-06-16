from .FlowExecution import FlowExecution

def main(riverbox_flow: dict, flow_metadata: dict, callback_function, execution_type, cube_id=None, args={}, debug_state: FlowExecution= None):
  flow = FlowExecution(riverbox_flow, flow_metadata, callback_function, execution_type, cube_id, debug_state)
  flow.execute(args)
  return flow
