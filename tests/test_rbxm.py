import sys
sys.path.append(".")

import riverbox_frontend as rbx


def test_rbxm():
  rbxm = rbx.RiverboxCubeManager({})
  a = 2
  @rbxm.main_handler
  def add_to_a ():
    nonlocal a
    a += 1
    return 4
  
  rbxm.finish()
  assert rbxm.output == 4
  assert a == 3
  assert rbxm.finished
  
