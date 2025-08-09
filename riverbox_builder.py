# riverbox_builder.py

import sys
import os

# Add the parent directory of this file to the Python path.
# This ensures that `core` and `riverbox` modules can be found.
_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.append(_current_dir)

import uuid
import inspect
import json
import textwrap
from typing import Any, Dict, List, Optional

from core.run import main

# ------------------------------------------------------------
# GLOBAL REGISTRY for decorated functions. 
# When you do @rbx_function({ … }), we store those params here.
# ------------------------------------------------------------
_REGISTERED_RBX_FUNCTIONS: Dict[Any, Dict[str, Any]] = {}


def rbx_function(params: Dict[str, Any] = None):
    """
    Decorator to mark a Python function as a “riverbox cube.” 
    The `params` dict can contain keys like “kind”, “name”, “arg-key”, “default-value”, etc.
    We store it in a global registry so that Flow.add_cube(fn) can look it up.
    """
    if params is None:
        params = {}

    def decorator(fn):
        _REGISTERED_RBX_FUNCTIONS[fn] = params.copy()
        return fn
    return decorator


def _new_id() -> str:
    """Return a fresh UUID string each time."""
    return str(uuid.uuid4())

def get_invocation_metadata() -> Dict[str, Any]:
    return {"execution-id": _new_id(), "flow-id": _new_id(), "invocation-id": _new_id()}

# ------------------------------------------------------------
# C L A S S   C U B E
# ------------------------------------------------------------
class Cube:
    def __init__(
        self,
        *,
        id: Optional[str] = None,
        kind: str,
        name: str,
        code: Optional[str] = None,
        arg_key: Optional[Any] = None,
        default_value: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        # **Note: we no longer store tag_stack here.** 
        # Instead, we compute it at serialization time.
        inner_cubes: Optional[List["Cube"]] = None,
        execution_id: Optional[str] = None,
        run_on_same: Optional[bool] = None,
        sub_flow_version_id: Optional[str] = None,
        env: Optional[Dict[str, Any]] = None,
    ):
        self.id: str = id or _new_id()
        self.kind: str = kind
        self.name: str = name
        self.code: Optional[str] = code
        self.arg_key: Optional[Any] = arg_key
        self.default_value: Optional[Any] = default_value
        self.metadata: Dict[str, Any] = metadata.copy() if metadata else {}
        self.tags: List[str] = list(tags) if tags else []
        # Edges going out of this cube
        self.start_edges: List[Dict[str, Any]] = []

        # If this is a FLOW‐cube, we store its nested cubes here:
        self.inner_cubes: List[Cube] = list(inner_cubes) if inner_cubes else []
        # Extra FLOW‐specific fields:
        self.execution_id: Optional[str] = execution_id
        self.run_on_same: Optional[bool] = run_on_same
        self.sub_flow_version_id: Optional[str] = sub_flow_version_id
        self.env: Dict[str, Any] = env.copy() if env else {}

    def add_edge_to(
        self,
        target: "Cube",
        *,
        end_arg_key: Any = None,
        start_arg_key: Any = None,
        kind: str = "REGULAR",
        edge_id: Optional[str] = None,
    ) -> None:
        """
        Create an edge from self → target.
        - `end_arg_key`: which argument on the target this edge feeds into.
        - `start_arg_key`: which output key on self is being sent (or None).
        - `kind`: e.g. "REGULAR", "MAP", etc.
        """
        eid = edge_id or _new_id()
        edge = {
            "id": eid,
            "end": target.id,
            "end-arg-key": end_arg_key,
            "start-arg-key": start_arg_key,
            "kind": kind,
        }
        self.start_edges.append(edge)
        return eid

    def _compute_this_flow_summary(self) -> Dict[str, Any]:
        """
        Return a summary-dict of the form:
            {
              "main": [ this_flow’s tags ],
              "cubes": {
                "<child-cube-id>": {
                  "kind": "<child.kind>",
                  # if child.kind == "FLOW", include child.tags here
                  "tags": [ ... ],
                  "start-edges": [ ... ]
                },
                ...
              }
            }
        This is used to build the "tag-stack" entry for any FLOW cube.
        """
        cubes_dict: Dict[str, Any] = {}
        for child in self.inner_cubes:
            entry: Dict[str, Any] = {"kind": child.kind, "start-edges": list(child.start_edges)}
            if child.kind == "FLOW":
                # Only FLOW‐cubes have their own tags to include
                entry["tags"] = list(child.tags)
            cubes_dict[child.id] = entry

        return {
            "main": list(self.tags),
            "cubes": cubes_dict
        }

    def to_dict(self, parent_tag_stack: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Emit a dict matching your JSON spec, **including** a computed "tag-stack" if self.kind == "FLOW".
        For non-FLOW cubes, we omit "tag-stack."

        If this cube is a FLOW, we compute:
          new_tag_stack = (parent_tag_stack or []) + [ this_flow_summary ]
        and include "tag-stack": new_tag_stack.  Then we embed all nested cubes by calling
        to_dict(...) on each, passing down new_tag_stack (so that deeper FLOWs build theirs properly).
        """
        base: Dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "name": self.name,
        }

        # PARAM cubes
        if self.kind == "PARAM":
            if self.arg_key is not None:
                base["arg-key"] = self.arg_key
            if self.default_value is not None:
                base["default-value"] = self.default_value

        # REGULAR cubes
        if self.kind == "REGULAR" and self.code is not None:
            base["code"] = self.code

        # RESULT cubes
        if self.kind == "RESULT":
            if self.arg_key is not None:
                base["arg-key"] = self.arg_key

        # FLOW cubes: we must include execution-id, run-on-same, sub-flow-version-id, env, tags, and nested cubes
        if self.kind == "FLOW":
            if self.execution_id is not None:
                base["execution-id"] = self.execution_id
            base["run-on-same"] = self.run_on_same if self.run_on_same is not None else False
            base["sub-flow-version-id"] = self.sub_flow_version_id if self.sub_flow_version_id is not None else ""
            if self.env:
                base["env"] = self.env
            else:
                base["env"] = {}

            if self.tags:
                base["tags"] = list(self.tags)

            # 1) Compute this flow’s summary (main/tags + cubes summary)
            this_summary = self._compute_this_flow_summary()

            # 2) Build the new tag-stack for this FLOW cube
            new_tag_stack: List[Dict[str, Any]] = []
            if parent_tag_stack:
                new_tag_stack.extend(parent_tag_stack)
            new_tag_stack.append(this_summary)
            base["tag-stack"] = new_tag_stack

            # 3) Now serialize inner_cubes, passing down new_tag_stack when needed:
            nested_list: List[Dict[str, Any]] = []
            for child in self.inner_cubes:
                if child.kind == "FLOW":
                    # Pass the new_tag_stack so that deeper FLOWs build on it:
                    nested_list.append(child.to_dict(parent_tag_stack=new_tag_stack))
                else:
                    # Non-FLOW cubes do not carry a tag-stack of their own
                    nested_list.append(child.to_dict())
            base["cubes"] = nested_list

        # If there is per-cube metadata (but only for non-FLOW cubes)
        if self.metadata:
            base["metadata"] = self.metadata

        # Always include start-edges (even if empty)
        base["start-edges"] = list(self.start_edges)
        return base


# ------------------------------------------------------------
# C L A S S   F L O W
# ------------------------------------------------------------
class Flow:
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        execution_id: Optional[str] = None,
        run_on_same: Optional[bool] = None,
        sub_flow_version_id: Optional[str] = None,
        riverbox_version: float = 1.0,
        language: str = "python",
        version: str = "3.11",
        tags: Optional[List[str]] = None,
        env: Optional[Dict[str, Any]] = None,
        args: Optional[Dict[str, Any]] = None,
    ):
        # If this Flow is later embedded as a FLOW cube, these fields are used:
        self.name: Optional[str] = name or ""
        self.execution_id: Optional[str] = execution_id
        self.run_on_same: Optional[bool] = run_on_same
        self.sub_flow_version_id: Optional[str] = sub_flow_version_id

        # Top‐level metadata (for either a top‐level Flow → JSON, or a nested Flow)
        self.metadata: Dict[str, Any] = {
            "riverbox-version": riverbox_version,
            "language": language,
            "version": version,
        }
        self.tags: List[str] = list(tags) if tags else []

        # flow-level env / args
        self.env: Dict[str, Any] = env.copy() if env else {}
        self.args: Dict[str, Any] = args.copy() if args else {}

        # Internally store all cubes (including nested FLOWS as Cube(kind="FLOW"))
        self._cubes: List[Cube] = []
    
    def __getitem__(self, cube_id):
        cubes =  [cube for cube in self._cubes if cube.id == cube_id]
        if cubes:
            return cubes[0]
    
    def nodes (self):
        return self._cubes
    
    def node_ids(self):
        return [cube.id for cube in self._cubes]
    
    def edges (self):
        edges = []
        for cube in self._cubes:
            edges += cube.start_edges
        return edges

    def add_cube(self, spec: Any) -> Cube:
        """
        Three supported cases:
          (A) spec is a decorated Python function  → build a REGULAR (or PARAM/RESULT) cube
          (B) spec is a dict                   → shallow-copy it into a Cube
          (C) spec is another Flow instance   → wrap it into a Cube(kind="FLOW")
        """
        # --- CASE A: a decorated function  ---
        if callable(spec) and spec in _REGISTERED_RBX_FUNCTIONS:
            params = _REGISTERED_RBX_FUNCTIONS[spec]
            kind = params.get("kind", "REGULAR")
            name = params.get("name", spec.__name__)

            # 1) Grab the full source
            try:
                raw_source = inspect.getsource(spec)
            except OSError:
                clean_body = None
            else:
                lines = raw_source.splitlines()

                # 2) Remove all leading decorator lines (those starting with '@')
                i = 0
                while i < len(lines) and not lines[i].strip().startswith("def"):
                    i += 1
                # Now lines[i] should be the 'def …:' line
                # 3) Everything _after_ that line is the function body
                body_lines = lines[i + 1 :]

                # 4) Dedent the body so it's flush at column 0
                clean_body = textwrap.dedent("\n".join(body_lines)).rstrip()
            

            cube = Cube(
                id=params.get("id"),
                kind=kind,
                name=name,
                code=clean_body,
                arg_key=params.get("arg-key"),
                default_value=params.get("default-value"),
                metadata=params.get("metadata"),
                tags=params.get("tags"),
            )
            self._cubes.append(cube)
            return cube

        # CASE B: raw dict describing a cube
        elif isinstance(spec, dict):
            kind = spec["kind"]
            name = spec["name"]
            cube = Cube(
                id=spec.get("id"),
                kind=kind,
                name=name,
                code=spec.get("code"),
                arg_key=spec.get("arg-key"),
                default_value=spec.get("default-value"),
                metadata=spec.get("metadata"),
                tags=spec.get("tags"),
                # If it’s already a FLOW‐dict, pull in nested cubes recursively:
                inner_cubes=[
                    Cube(
                        id=sc.get("id"),
                        kind=sc["kind"],
                        name=sc["name"],
                        code=sc.get("code"),
                        arg_key=sc.get("arg-key"),
                        default_value=sc.get("default-value"),
                        metadata=sc.get("metadata"),
                        tags=sc.get("tags"),
                        execution_id=sc.get("execution-id"),
                        run_on_same=sc.get("run-on-same"),
                        sub_flow_version_id=sc.get("sub-flow-version-id"),
                        env=sc.get("env"),
                        inner_cubes=[
                            # You could recurse deeper if needed. For simplicity, 
                            # we handle only one level at a time here.
                            Cube(
                                id=subsc.get("id"),
                                kind=subsc["kind"],
                                name=subsc["name"],
                                code=subsc.get("code"),
                                arg_key=subsc.get("arg-key"),
                                default_value=subsc.get("default-value"),
                                metadata=subsc.get("metadata"),
                                tags=subsc.get("tags"),
                                execution_id=subsc.get("execution-id"),
                                run_on_same=subsc.get("run-on-same"),
                                sub_flow_version_id=subsc.get("sub-flow-version-id"),
                                env=subsc.get("env", {}),
                                inner_cubes=[],
                            )
                            for subsc in sc.get("cubes", [])
                        ]
                    )
                    for sc in spec.get("cubes", [])
                ] if kind == "FLOW" else [],
                execution_id=spec.get("execution-id"),
                run_on_same=spec.get("run-on-same"),
                sub_flow_version_id=spec.get("sub-flow-version-id"),
                env=spec.get("env"),
            )
            if "start-edges" in spec:
                cube.start_edges = list(spec["start-edges"])
            self._cubes.append(cube)
            return cube

        # CASE C: embed another Flow as a FLOW‐cube
        elif isinstance(spec, Flow):
            # Wrap the entire Flow object into a single Cube(kind="FLOW")
            cube = Cube(
                id=None,
                kind="FLOW",
                name=spec.name or "",
                code=None,
                arg_key=None,
                default_value=None,
                metadata=dict(spec.metadata),
                tags=spec.tags,
                inner_cubes=list(spec._cubes),
                execution_id=spec.execution_id or _new_id(),
                run_on_same=spec.run_on_same if spec.run_on_same is not None else False,
                sub_flow_version_id=spec.sub_flow_version_id or "",
                env=spec.env,
            )
            self._cubes.append(cube)
            return cube

        else:
            raise ValueError(
                "add_cube expects either:\n"
                "  • a function decorated with @rbx_function, or\n"
                "  • a dict describing a cube, or\n"
                "  • a Flow object (to embed as kind='FLOW')."
            )

    def remove_cube(self, cube_id: str):
        """
        Removes a cube by its ID and also removes any edges pointing to it.
        """
        # Remove the cube with the matching ID
        self._cubes = [cube for cube in self._cubes if cube.id != cube_id]

        # Iterate through the remaining cubes to remove edges pointing to the deleted cube
        for cube in self._cubes:
            cube.start_edges = [edge for edge in cube.start_edges if edge.get("end") != cube_id]
    
    def remove_edge_by_id (self, edge_id: str):
        """
        Removes an edge by its ID.
        """
        for cube in self._cubes:
            for edge in cube.start_edges:
                if edge["id"] == edge_id:
                    cube.start_edges.remove(edge)
    
    def to_dict (self):
        return self.to_json(-1)

    def to_json(self, indent: int = 2) -> str:
        """
        Serialize this Flow (top‐level) to a JSON string with exactly:

            {
              "metadata": { … },
              "tags": [ … ],
              "tag-stack": [ <one dictionary for this flow> ],
              "env": { … },
              "args": { … },
              "flow": {
                "cubes": [ <cube1_dict>, <cube2_dict>, … ]
              }
            }

        Where “tag-stack” is computed as an array of exactly one dict for the top‐level flow,
        and nested FLOW‐cubes will append their own entries when you inspect their to_dict().
        """
        # 1) Build the top-level flow summary dict (for tag-stack)
        top_summary: Dict[str, Any] = {
            "main": list(self.tags),
            "cubes": {
                cube.id: (
                    {"kind": cube.kind,
                     **({"tags": cube.tags} if cube.kind == "FLOW" else {}),
                     "start-edges": list(cube.start_edges)
                     }
                )
                for cube in self._cubes
            }
        }

        # 2) At top‐level, tag_stack is a list containing exactly this one dict
        top_tag_stack: List[Dict[str, Any]] = [top_summary]

        # 3) Now serialize each cube: pass parent_tag_stack=top_tag_stack if kind=="FLOW"
        serialized_cubes: List[Dict[str, Any]] = []
        for cube in self._cubes:
            if cube.kind == "FLOW":
                serialized_cubes.append(cube.to_dict(parent_tag_stack=top_tag_stack))
            else:
                serialized_cubes.append(cube.to_dict())

        full: Dict[str, Any] = {
            "metadata": self.metadata,
            "tags": list(self.tags),
            "tag-stack": top_tag_stack,
            "env": self.env,
            "args": self.args,
            "flow": {
                "cubes": serialized_cubes
            },
        }
        
        if indent == -1:
            #print(json.dumps(full, indent=4))
            return full
        return json.dumps(full, indent=indent)
    
    def run_full_with_args (self, callback, args):
        return main(self.to_json(-1), get_invocation_metadata(), callback, "FULL",  args=args)
