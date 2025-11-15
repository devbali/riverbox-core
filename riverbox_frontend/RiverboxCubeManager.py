from core.call_external_flow import call_external_flow

class RiverboxCubeManager ():
  def __init__(self, inputs: dict, box_metadata: dict=None, riverbox_metadata: dict=None, flow_registry: dict=None, main_callback=None):
    self.input = inputs
    self.output = None
    self.next_function = None
    self.handler = None
    self.handler_split = False
    self.finished = False
    self.box_metadata = box_metadata
    self.riverbox_metadata = riverbox_metadata
    self.flow_registry = flow_registry if flow_registry is not None else {}
    self.main_callback = main_callback

  def get_next (self):
    return (self.output, self.next_function)
  
  def set_output (self, output):
    self.output = output
  
  def main_handler (self, handler_func):
    self.handler = handler_func
    return handler_func
  
  def finish (self):
    if self.handler:
      for arg in self.input:
        if "-" in arg:
          del self.input[arg]

      out = self.handler(**self.input)
      if self.handler_split:
        self.output, self.next_function = out
      else:
        self.output = out
    self.finished = True

  def call (self, riverbox_name, args):
    if riverbox_name not in self.flow_registry:
      return None

    rbx_flow = self.flow_registry[riverbox_name]
    body = rbx_flow.copy()  # Create a COPY to avoid modifying flow_registry
    body["run-on-same"] = True
    body["sub-flow-version-id"] = "latest"
    body["cubes"] = rbx_flow["flow"]["cubes"]
    del body["flow"]
    callback = self.main_callback if self.main_callback is not None else lambda x: x

    # Pass flow_registry to enable speculation/caching in nested calls
    result = call_external_flow(body, {**self.riverbox_metadata, "invocation-id": None}, callback, args, flow_registry=self.flow_registry)
    return result

