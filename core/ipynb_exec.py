from IPython.core.interactiveshell import InteractiveShell
import traceback

class RiverboxIPythonShell (InteractiveShell):
    def __init__ (self, *args, **kwargs):
        self.error_message = ""
        super().__init__(colors="NoColor", *args, **kwargs)
        self.enable_matplotlib("inline")
    
    def enable_gui(self, gui=...):
        pass

    def _showtraceback(self, etype, evalue, stb: str):
        val = self.InteractiveTB.stb2text(stb)
        if self.error_message != "": self.error_message += "\n"
        try:
            self.error_message += val
        except UnicodeEncodeError:
            self.error_message += val.encode("utf-8", "backslashreplace").decode()

def print_override(console_output_file):
    def print_function(*args, **kwargs):
        if "file" in kwargs:
            return print(*args, **kwargs)
        else:
            return print(*args, file=console_output_file, **kwargs)
    return print_function

def execute_code (cube_execution_layer, global_vars, flow):
    import riverbox_frontend

    flow.global_vars_lock.acquire()
    try:
        local_global_vars = {**global_vars, **cube_execution_layer.args}
    except:
        local_global_vars = {**global_vars}
    flow.global_vars_lock.release()

    rbxm = riverbox_frontend.RiverboxCubeManager(cube_execution_layer.args)
    local_global_vars["rbx"] = riverbox_frontend
    local_global_vars["print"] = print_override(cube_execution_layer.console_output)
    local_global_vars["rbxm"] = rbxm

    shell = RiverboxIPythonShell()
    old_keys = list(shell.user_ns.keys())
    shell.user_ns.update(local_global_vars)
    try:
        res = shell.run_cell(cube_execution_layer.cube.code)
        rbxm = local_global_vars["rbxm"]
        rbxm.finish()
    except:
        cube_execution_layer.errored = True
        contents = cube_execution_layer.console_output.getvalue()
        cube_execution_layer.console_output.close()
        cube_execution_layer.error(str(cube_execution_layer.execution_id), flow, "\n\nERROR:\n" + traceback.format_exc() + "\n", contents)

    flow.global_vars_lock.acquire()
    flow.global_vars = {**flow.global_vars, **{
        k: shell.user_ns[k] for k in shell.user_ns if k not in old_keys
    }}
    flow.global_vars_lock.release()

    if res.error_before_exec or res.error_in_exec:
        cube_execution_layer.errored = True
        contents = cube_execution_layer.console_output.getvalue()

        cube_execution_layer.console_output.close()
        cube_execution_layer.error(str(cube_execution_layer.execution_id), flow, shell.error_message, contents)
    
    print(res)
    if res.result is not None:
        print(res.result, file=cube_execution_layer.console_output)

    cube_execution_layer.return_value = rbxm.output

if __name__ == "__main__":
    shell = RiverboxIPythonShell()
    old_keys = list(shell.user_ns.keys())
    res = shell.run_cell("""

import pandas as pd

df = pd.DataFrame({"series1": [1,2,3,4], "series2": [5,6,7,8]})

df

""")
    print(type(res.result))
