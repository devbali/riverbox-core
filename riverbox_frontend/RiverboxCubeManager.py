class RiverboxCubeManager ():
  def __init__(self, inputs: dict, box_metadata: dict=None, riverbox_metadata: dict=None):
    self.input = inputs
    self.output = None
    self.next_function = None
    self.handler = None
    self.handler_split = False
    self.finished = False
    self.box_metadata = box_metadata
    self.riverbox_metadata = riverbox_metadata

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
