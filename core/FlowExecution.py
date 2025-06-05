from .Cube import CubeExecution
import asyncio
import threading
import time
import os

class FlowExecution:
    def __init__(self, riverbox_flow_full, flow_metadata, callback_function, execution_type, cube_id, debug_state: 'FlowExecution'):
        self.flow_metadata = flow_metadata
        self.riverbox_metadata = riverbox_flow_full["metadata"]
        print("init happening, riverbox tagstack", riverbox_flow_full.get("tag-stack", [[]]))
        riverbox_flow = riverbox_flow_full["flow"]

        self.tags = riverbox_flow_full.get("tags", [])
        self.tag_stack =  riverbox_flow_full.get("tag-stack", [[]])
        self.execution_id = flow_metadata["execution-id"]
        self.flow_id = flow_metadata["flow-id"]

        self.execution_type = execution_type
        assert self.execution_type in ["FULL", "ONLY", "UPTO", "DEBUG_ONLY", "DEBUG_UPTO", "DEBUG_NEXT", "DEBUG_START"]

        self.callback_function = callback_function

        if debug_state is not None:
            with debug_state.global_vars_lock:
                self.prior_debug_execution_object = debug_state
                self.global_vars = debug_state.global_vars
                self.global_vars_lock = debug_state.global_vars_lock

            self.cubes: list[CubeExecution] = []
            for c in riverbox_flow["cubes"]:
                if c["id"] in debug_state.latest_cubes_lookup:
                    self.cubes.append(debug_state.latest_cubes_lookup[c['id']].clone_for_new_debug_execution(c))
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
            self.critical_path_upto = self.calculate_critical_path_upto(cube_id)
            if self.execution_type == "DEBUG_UPTO":
                for e in self.critical_path_upto:
                    # mark all undone
                    self.latest_cubes_lookup[e].undone = True
        
        self.scheduler_lock = threading.Lock()
        self.callback_lock = threading.Lock()

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

            #print("Executables step 1", executables)
            for cube_id in list(executables.keys()):
                cube = self.latest_cubes_lookup[cube_id]
                print(executables, cube_id, cube.done, cube.undone)


                # If a cube is done, should not be executed, except if it is also undone (to allow cycles)
                if cube.done and not cube.undone and cube_id in executables:
                    del executables[cube_id]
                    continue

                # If an undone cube points to something it can not be executed
                for edge in cube.start_edges:
                    if edge.end in executables:
                        del executables[edge.end]

            #print("Executables step 2", executables)
            for id in list(executables.keys()):
                # Either the cube is to not run due to a None in input
                # Or the cube is to not because it has already started and 
                #   it's not the case that its a done then undone situation (cycle/redone in Debug more)
                if executables[id].dont_run or (executables[id].started and not (executables[id].done and executables[id].undone)):
                    del executables[id]

        #print("Executables step 3", executables)
        return executables

    def update_manager(self, message):
        with self.callback_lock:
            self.callback_function(message)

    async def run_cube(self, func_id, single):
        cube = self.latest_cubes_lookup[func_id]
        cube.global_execution_count = self.global_execution_count
        await asyncio.to_thread(self.latest_cubes_lookup[func_id].execute, self)
        if not single:
            await self.run_all_possible(False)

    async def run_all_possible(self, single):
        executables = self.find_executables()
        if executables: self.global_execution_count += 1
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

    def execute(self, args, worker_assigned=False):
        self.update_manager({
            "type": "NEW_EXECUTION",
            "execution-id": self.execution_id,
            "flow-id": self.flow_id,
            "args": args,
            "execution-type": self.execution_type,
            "invocation-id": self.flow_metadata["invocation-id"],
            "worker-assigned": worker_assigned,
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
