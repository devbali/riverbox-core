from .FlowExecution import FlowExecution

def main(riverbox_flow: dict, flow_metadata: dict, callback_function, execution_type, cube_id=None, args={}, debug_state: FlowExecution= None):
  print("main happening, riverbox tag-stack", riverbox_flow["tag-stack"])
  flow = FlowExecution(riverbox_flow, flow_metadata, callback_function, execution_type, cube_id, debug_state)
  flow.execute(args)
  return flow
