import ast
import asyncio
import uuid
import time
import json
import traceback
import threading
from .plain_python_exec import execute_code as plain_python_execute
from .ipynb_exec import execute_code as ipynb_execute
from .call_external_flow import call_external_flow

class Edge:
    def __init__(self, edge_body):
        self.id = edge_body["id"]
        self.end = edge_body["end"]
        self.end_arg_key: str | int = edge_body["end-arg-key"]
        self.kind = edge_body["kind"]
        self.start_arg_key: str = edge_body["start-arg-key"]

    def get_client_edge_metadata (self):
        return {
            "end": self.end,
            "end-arg-key": self.end_arg_key,
            "kind": self.kind,
            "start-arg-key": self.start_arg_key
        }

class Arg:
    def __init__ (self, map, value, keyword):
        self.map = map
        self.value = value
        self.keyword = keyword

import io

class CubeExecutionLayer:
    def __init__(self, cube, args, layer):
        self.cube = cube
        self.args: Arg | dict[str, Arg] = args
        self.layer = layer
        self.errored = False
        self.console_output = io.StringIO()
        self.execution_id = uuid.uuid4()
        self.return_value = None
        self.next_function = None

    def get_client_box_metadata (self):
        return {
            "box": self.cube.get_client_box_metadata_for_flow(),
            "layer": self.layer
        }

    def print_override(self):
        def print_function(*args, **kwargs):
            if "file" in kwargs:
                return print(*args, **kwargs)
            else:
                return print(*args, file=self.console_output, **kwargs)
        return print_function

    def error(self, execution_id, flow, message, console_output):
        self.errored = True
        flow.update_manager({
            "type": "CUBE_EXECUTION_ERROR",
            "cube-execution-id": execution_id,
            "error-message": message,
            "console-output": console_output,
            "time": time.time()
        })
        print("Error",message)

    def execute_code (self, global_vars, flow):
        if flow.riverbox_metadata["language"] == "python":
            plain_python_execute(self, global_vars, flow)
        elif flow.riverbox_metadata["language"] == "ipython":
            plain_python_execute(self, global_vars, flow)

    def execute(self, global_vars, flow):
        from .FlowExecution import FlowExecution
        # No local variables here
        # print("in cube execute, cube execution id and flow", self.execution_id, flow)
        flow.update_manager({
            "type": "START_CUBE_EXECUTION",
            "cube-id": self.cube.id,
            "flow-execution-id": flow.execution_id,
            "cube-execution-id": str(self.execution_id),
            "arguments": self.args,
            "layer": self.layer,
            "global-execution-count": self.cube.global_execution_count,
            "time": time.time()
        })
        
        if self.cube.pre_execution_error:
            self.error(str(self.execution_id), flow, self.cube.pre_execution_error, "")
            return None
        
        if self.cube.kind == "FLOW":
            self.return_value = call_external_flow(*self.cube.sub_flow_args, self.args, self.cube.global_execution_count, self.layer, flow.env)
        
        elif self.cube.kind == "PARAM":
            if self.cube.arg_key and self.cube.arg_key in flow.args:
                self.return_value = flow.args[self.cube.arg_key]
            elif not self.cube.arg_key and self.cube.name in flow.args:
                self.return_value = flow.args[self.cube.name]
            else:
                self.return_value = self.cube.default_value

        else:
            self.execute_code(global_vars, flow)

        if not self.errored:
            contents = self.console_output.getvalue()
            self.console_output.close()
            flow.update_manager({
                "type": "SUCCESSFUL_CUBE_EXECUTION",
                "cube-execution-id": str(self.execution_id),
                "console-output": contents,
                "return-value": str(self.return_value),
                "time": time.time()
            })
        
        return self.return_value

def has_same_underlying_cube (e1, e2):
    for k in ["id", "kind", "name", "arg-key", "code", "execution-id","default-value", "start-edges"]:
        if k in e1 and k not in e2:
            return False
        if k in e2 and k not in e1:
            return False
        if k in e1 and k in e2 and e1[k] != e2[k]:
            return False
    return True

class CubeExecution:
    #TODO: make separate classes for types of Cubes
    class NO_ARG_TYPE():
        pass
    NO_ARG = NO_ARG_TYPE()
    
    def __init__(self, body, flow, global_execution_count = 0):
        self.body = body
        self.flow = flow

        self.id = body["id"]
        self.kind = body["kind"]
        self.name = body["name"]
        self.global_execution_count = global_execution_count
        
        if self.kind in ["PARAM", "RESULT"]:
            self.arg_key = body["arg-key"]
        
        if self.kind == "REGULAR":
            self.code = body["code"]
        
        if self.kind == "FLOW":
            self.sub_flow_args = [body, flow.flow_metadata, flow.callback_function]

        if self.kind == "PARAM":
            try:
                self.default_value = json.loads(body["default-value"])
            except:
                self.default_value = body["default-value"]
        
        if self.kind != "RESULT":
            self.start_edges = [Edge(c) for c in body["start-edges"]]
        else:
            self.start_edges = []

        # (Some of) these need to be set to a previous execution in case of debug mode
        # Look at `clone_for_new_execution` when changing
        self.dont_run = False
        self.started = False
        self.done = False
        self.undone = False
        self.pre_execution_error = ""
        
        self.num_layers = 0
        self.return_values = []
        self.args = Arg(False, None, None)
        
        # Maximum 100 layers at the same time
        self.layer_semaphore = threading.Semaphore(100)

        try:
            if self.kind == "REGULAR": self.ast_analyze(self.code)
        except:
            self.pre_execution_error = "\n\nERROR:\n" + traceback.format_exc() + "\n"

    def get_client_box_metadata_for_flow (self):
        return {
            "id": self.id,
            "kind": self.kind,
            "edges-to": [e.get_client_edge_metadata() for e in self.start_edges]
        }

    def clone_for_new_debug_execution (self, new_body):
        if has_same_underlying_cube(self.body, new_body):
            return self
        c = CubeExecution(new_body, self.flow, self.global_execution_count)

        c.dont_run = self.dont_run
        c.started = self.started
        c.done = self.done
        c.pre_execution_error = self.pre_execution_error

        c.num_layers = self.num_layers
        c.return_values = self.return_values
        c.args = self.args

        return c

    def __str__(self):
        return f"Cube {self.name} Execution"

    def ast_analyze(self, code):
        print("----<>self.flow", self.flow.riverbox_metadata)
        if self.flow.riverbox_metadata["language"] == "python":
            module = ast.parse(code)
            self.function_name = None
            for expr in module.body:
                if type(expr) == ast.FunctionDef:
                    self.function_name = expr.name
                    #self.args = [Arg(False, CubeExecution.NO_ARG, a.arg) for a in expr.args.args]
                    break

    async def thread_execute_layer(self, execution: CubeExecutionLayer, *args, **kwargs):
        def execute_with_sema(execution, *args, **kwargs):
            self.layer_semaphore.acquire()
            #print("Started execution on layer", execution.layer)
            rv = execution.execute(*args, **kwargs)
            self.layer_semaphore.release()
            return rv

        return await asyncio.to_thread(execute_with_sema, execution, *args, **kwargs)

    async def execute_multilayer(self, global_vars, layer_args, flow):
        tasks = []
        layer_num = 1
        for layer in layer_args:
            if layer_num > 1000:
                # Maximum number of layers is 1000
                break
            execution = CubeExecutionLayer(self, layer, layer_num)
            tasks.append(asyncio.create_task(
                self.thread_execute_layer(execution, global_vars, flow)))
            layer_num += 1
    
        return [(await task) for task in tasks]

    def post_successful_execution (self, flow, return_value=None, map_type=False):
        self.undone = False

        if self.kind in ["REGULAR", "PARAM", "FLOW"]: # None of this needed for result
            for edge in self.start_edges:
                next_function = flow.latest_cubes_lookup[edge.end]
                next_function.undone = True
                
                # Based on return value of current Cube and the edge to the next cube, create
                #   the value to be fed to the arg in the next cube
                if edge.start_arg_key is None:
                    return_value_on_edge = return_value
                elif map_type:
                    return_value_on_edge = []
                    for layer_rv in return_value:
                        if isinstance(layer_rv, dict) and edge.start_arg_key in layer_rv:
                            return_value_on_edge.append(layer_rv[edge.start_arg_key])
                        else:
                            return_value_on_edge.append(layer_rv)
                elif isinstance(return_value, dict) and edge.start_arg_key in return_value:
                    return_value_on_edge = return_value[edge.start_arg_key]
                else:
                    next_function.dont_run = True

                if edge.end_arg_key is not None and not next_function.dont_run:
                    next_arg = Arg(edge.kind == "MAP", return_value_on_edge, edge.end_arg_key)
                    if not isinstance(next_function.args, dict):
                        next_function.args = {}
                    next_function.args[edge.end_arg_key] = next_arg
                elif not next_function.dont_run:
                    next_function.args = Arg(edge.kind == "MAP", return_value_on_edge, edge.end_arg_key)

        self.done = True

    def get_args_layers (self):
        layer_args = [dict()]
        map_type = False
        add_to_each = lambda arr, k,v: [{**kw, k:v} for kw in arr]

        if isinstance(self.args, Arg):
            if self.args.value is None:
                self.args.value = {}
            elif not isinstance(self.args.value, dict):
                # Args passed but not dict
                self.pre_execution_error = f"ERROR in args, expected dict but is {type(self.args.value)}"
                return False, [{}]

            map_type = self.args.map
            layer_args = [self.args.value] if not map_type else self.args.value

        else:
            for arg_key in self.args:
                a = self.args[arg_key]
                if not a.map and a.value is not CubeExecution.NO_ARG:
                    layer_args = add_to_each(layer_args, a.keyword, a.value)
                elif a.value is not CubeExecution.NO_ARG:
                    map_type = True
                    try:
                        if isinstance(a.value, int):
                            iterable = range(a.value)
                        else:
                            iterable = list(a.value)
                    except TypeError:
                        iterable = [a.value]
                    layer_args = sum([add_to_each(layer_args, a.keyword, arg) for arg in iterable], [])

        def iswaiting (s):
            try:
                return int(s) < 0
            except ValueError:
                return False

        remove_numeric_keys = lambda d: {k: d[k] for k in d if not iswaiting(k)}
        layer_args = [remove_numeric_keys(args) for args in layer_args]
        return map_type, layer_args

    def execute(self, flow: 'Flow'):
        self.started = True
        map_type = False

        if self.kind == "RESULT":
            assert isinstance(self.args, Arg), self.args
            layer_args = self.args.value
        else:
            map_type, layer_args = self.get_args_layers()
            self.num_layers = len(layer_args)

        if self.kind == "RESULT":
            flow.add_result(self, layer_args)
            return_value = None
        elif not map_type:
            return_value = CubeExecutionLayer(self, layer_args[0], 1).execute(flow.global_vars, flow)
        else:
            return_value = asyncio.run(self.execute_multilayer(flow.global_vars, layer_args, flow))

        self.post_successful_execution(flow, return_value, map_type)
