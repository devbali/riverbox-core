from .Cube import CubeExecution
import asyncio
import threading
import time
import os
import dill

class FlowExecution:
    def __init__(self, riverbox_flow_full, current_exec_metadata, callback_function, execution_type, cube_id, debug_state: 'FlowExecution', dump_state_folder=None):
        self.current_execution_metadata = current_exec_metadata
        self.riverbox_metadata = riverbox_flow_full["metadata"]
        riverbox_flow = riverbox_flow_full["flow"]
        self.riverbox_flow_full = riverbox_flow_full

        self.tags = riverbox_flow_full.get("tags", [])
        self.tag_stack =  riverbox_flow_full.get("tag-stack", [[]])
        self.execution_id = current_exec_metadata["execution-id"]
        self.flow_id = current_exec_metadata["flow-id"]
        
        self.dump_state_folder = dump_state_folder
        self.num_running_cubes = 0

        self.execution_type = execution_type
        assert self.execution_type in ["FULL", "ONLY", "UPTO", "DEBUG_ONLY", "DEBUG_UPTO", "DEBUG_NEXT", "DEBUG_START"]

        self.callback_function = callback_function

        if debug_state is not None:
            print("Initializing FlowExecution in debug mode with prior execution state at global execution count", debug_state.global_execution_count)
            with debug_state.global_vars_lock:
                self.prior_debug_execution_object = debug_state
                self.global_vars = debug_state.global_vars
                self.global_vars_lock = debug_state.global_vars_lock
            print("Global vars at start of debug:", self.global_vars)

            self.cubes: list[CubeExecution] = []
            for c in riverbox_flow["cubes"]:
                if c["id"] in debug_state.latest_cubes_lookup:
                    self.cubes.append(debug_state.latest_cubes_lookup[c['id']].clone_for_new_debug_execution(c, self))
                else:
                    self.cubes.append(CubeExecution(c, self))

            self.global_execution_count = debug_state.global_execution_count
            self.env = debug_state.env

        else:
            self.env = riverbox_flow_full["env"]
            for k in self.env:
                os.environ[k] = self.env[k]

            self.global_vars = {}
            self.global_vars_lock = threading.Lock()
            self.prior_debug_execution_object = None

            self.cubes = [CubeExecution(c, self) for c in riverbox_flow["cubes"]]
            self.global_execution_count = 0
        
        self.latest_cubes_lookup = {c.id: c for c in self.cubes} # latest_cubes_lookup only has latest executions for quick lookup

        self.execution_main_cube_id = cube_id
        if self.execution_type == "UPTO" or self.execution_type == "DEBUG_UPTO":
            # If an upto is being called, artificially mark all the start nodes
            # in the critical path as not done, so that they can be executed

            self.critical_path_upto = self.calculate_critical_path_upto(cube_id)

            for id, cube in self.critical_path_upto.items():
                cube.done = False
                cube.started = False
                cube.dont_run = False
        
        self.scheduler_lock = threading.Lock()
        self.callback_lock = threading.Lock()
    
    def is_debug (self):
        return self.execution_type in ["DEBUG_START", "DEBUG_ONLY", "DEBUG_UPTO", "DEBUG_NEXT"]

    def calculate_critical_path_upto (self, cube_id):
        all_nodes_ids = set()
        def go_deep(start_cube, target_cube_id, visited=set()):
            nonlocal all_nodes_ids
            """
            Keep going to the next node until you:
              - Hit the target node: return set of nodes on path
              - Come back to the same node: return None
              - Reach a node where there is no next node: return empty set but try again from the previous

            """
            print("in go deep, start", start_cube.id, "target", target_cube_id)
            if start_cube.id in visited:
                return
            visited = visited.union({start_cube.id})
            
            if start_cube.id == target_cube_id:
                all_nodes_ids = all_nodes_ids.union(visited)
                return
            
            if start_cube.start_edges == []:
                return
            
            for edge in start_cube.start_edges:
                go_deep(self.latest_cubes_lookup[edge.end], target_cube_id, visited)
                
        cube_set = set(self.latest_cubes_lookup.keys())
        for cube in self.cubes:
            try:
                for edge in cube.start_edges:
                    cube_set.remove(edge.end)
            except KeyError:
                pass

        for id in cube_set:
            go_deep(self.latest_cubes_lookup[id], cube_id)

        return {id: self.latest_cubes_lookup[id] for id in all_nodes_ids}

    def find_executables(self):
        executables = {}
        with self.scheduler_lock:
            if self.execution_type in ["ONLY", "DEBUG_ONLY"]:
                return {self.execution_main_cube_id: self.latest_cubes_lookup[self.execution_main_cube_id]}

            if self.execution_type in ["FULL", "DEBUG_NEXT"]:
                executables = {**self.latest_cubes_lookup}
            elif self.execution_type in ["UPTO", "DEBUG_UPTO"]:
                executables = {**self.critical_path_upto}
            
            # If debug, try to repropogate as much as possible
            # If a node has an incoming edge from a node that has executed, but the edge was not fresh,
            # then repropogate the return value along that edge

            if self.is_debug():
                for cube in executables.values():
                    if cube.done:
                        for edge in cube.start_edges:
                            if edge.end not in executables:
                                continue
                            dest = executables[edge.end]
                            if not dest.started and not dest.done and not edge.fresh:
                                executables[edge.end].propagate_return_value_along_edge(edge)

            # Find nodes with no incoming edges, start_nodes that are not done, along with 
            # nodes with all fresh incoming edges, fresh_nodes

            start_nodes = {id: cube for id, cube in executables.items() if not cube.done}
            fresh_node_inputs = {} # keep track of input arg keys and whether at least one fresh edge points to it
            
            for cube_id in executables:
                cube = self.latest_cubes_lookup[cube_id]

                for edge in cube.start_edges:
                    if edge.end in start_nodes:
                        del start_nodes[edge.end]
                    
                    if edge.end not in executables:
                        continue

                    if edge.end not in fresh_node_inputs:
                        fresh_node_inputs[edge.end] = {}
                    
                    inputs = fresh_node_inputs[edge.end]

                    if edge.fresh:
                        inputs[edge.end_arg_key] = True
                    elif edge.end_arg_key not in inputs:
                        inputs[edge.end_arg_key] = False

            fresh_nodes = {}
            for id, inputs in fresh_node_inputs.items():
                all_fresh = True
                for k, v in inputs.items():
                    if not v:
                        all_fresh = False
                        break
                if all_fresh:
                    fresh_nodes[id] = self.latest_cubes_lookup[id]

            potential_executables = {**start_nodes, **fresh_nodes}
            #print("Executables step 1", executables, "start_nodes", start_nodes, "fresh_nodes", fresh_nodes, "fresh_node_inputs", fresh_node_inputs)

            for id in list(potential_executables.keys()):
                # Either the cube is to not run due to a None in input
                # Or the cube is to not because it has already started
                if potential_executables[id].dont_run or (potential_executables[id].started and not potential_executables[id].done):
                    del potential_executables[id]

        #print("Returning Executables", potential_executables)
        return potential_executables

    def update_manager(self, message):
        with self.callback_lock:
            self.callback_function(message)

    async def run_cube(self, func_id, single):
        self.num_running_cubes += 1
        cube = self.latest_cubes_lookup[func_id]
        cube.global_execution_count = self.global_execution_count
        await asyncio.to_thread(self.latest_cubes_lookup[func_id].execute, self)
        if not single:
            await self.run_all_possible(False)
        self.num_running_cubes -= 1

    def _dump_file_name (self, counter):
        if self.dump_state_folder is None:
            return None
        if not os.path.exists(self.dump_state_folder):
            os.makedirs(self.dump_state_folder)
        return os.path.join(self.dump_state_folder, f"global_vars_after_{counter}.dill")

    def _dump_state (self, counter):
        with self.global_vars_lock:
            # use dill to dump self.global_vars to file
            with open(self._dump_file_name(counter), "wb") as f:    # write‑binary mode
                dill.dump(self, f)

    def get_flow_from_checkpoint (self, exec_count, allow_closest_lower=True) -> "FlowExecution":
        if self.dump_state_folder is None:
            return None
        print("Getting flow from checkpoint at exec count", exec_count, "in folder", self.dump_state_folder)
        file_name = None
        while file_name is None:
            file_name = self._dump_file_name(exec_count)
            print("file name", file_name, "exists?", os.path.exists(file_name))
            if (exec_count < 0) or (not os.path.exists(file_name) and not allow_closest_lower):
                return None
            
            if not os.path.exists(file_name):
                file_name = None
                exec_count -= 1

        with open(file_name, "rb") as f:    # read‑binary mode
            flow_exec: FlowExecution = dill.load(f)
            flow_exec.global_vars_lock = threading.Lock()
            flow_exec.scheduler_lock = threading.Lock()
            flow_exec.callback_lock = threading.Lock()
            print("Loaded flow execution from file", file_name, "with exec count", flow_exec.global_execution_count, "and cubes", [c.id for c in flow_exec.cubes])
            return flow_exec

    async def run_all_possible(self, single):
        executables = self.find_executables()
        if executables:
            if self.dump_state_folder is not None and self.num_running_cubes == 0:
                self._dump_state(self.global_execution_count)
            self.global_execution_count += 1
        tasks = []
        for starter in executables:
            tasks.append(asyncio.create_task(self.run_cube(starter, single)))
        for task in tasks:
            await task

    def init_results(self):
        self.results = {}
        self.result_label = {}
        count = 1
        for cube in self.cubes:
            if cube.kind == "RESULT":
                if cube.arg_key and cube.arg_key not in self.results:
                    self.results[cube.arg_key] = None
                    self.result_label[cube.id] = cube.arg_key
                elif cube.name and cube.name not in self.results:
                    self.results[cube.name] = None
                    self.result_label[cube.id] = cube.name
                else:
                    self.results[count] = None
                    self.result_label[cube.id] = count
                    count += 1

    def add_result(self, cube, result):
        label = self.result_label[cube.id]
        self.results[label] = result
        self.update_manager({
            "type": "RESULT_UPDATE",
            "flow-execution-id": self.execution_id,
            "cube-id": cube.id,
            "label": label,
            "time": time.time(),
            "results": self.results
        })

    def execute(self, args, worker_assigned=False, parent_cubeexecution_id=None, flow_version_id=None):
        self.update_manager({
            "type": "NEW_EXECUTION",
            "execution-id": self.execution_id,
            "flow-id": self.flow_id,
            "args": args,
            "execution-type": self.execution_type,
            "invocation-id": self.current_execution_metadata["invocation-id"],
            "worker-assigned": worker_assigned,
            "parent-cube-execution-id": parent_cubeexecution_id,
            "flow-version-id": flow_version_id,
            "env": self.env if worker_assigned else None,
            "time": time.time()
        })

        self.init_results()
        self.args = args

        if self.execution_type != "DEBUG_START":
            asyncio.run(self.run_all_possible(self.execution_type in ["ONLY", "DEBUG_ONLY", "DEBUG_NEXT"]))

        if self.execution_type not in ["DEBUG_ONLY", "DEBUG_NEXT", "DEBUG_UPTO", "DEBUG_START"]:
            self.update_manager({
                "type": "EXECUTION_DONE",
                "execution-id": self.execution_id,
                "results": self.results,
                "time": time.time()
            })

        return self.results
    
    def get_client_riverbox_metadata (self):
        riverbox_meta = {"tags": self.tags, "tag-stack": self.tag_stack}
        return riverbox_meta
