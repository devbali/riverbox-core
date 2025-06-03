# riverbox_builder.py

import uuid
import inspect
import json
from typing import Any, Dict, List, Optional

# --------------------------------------------------------------------------------
# GLOBAL REGISTRY for decorated functions.
# --------------------------------------------------------------------------------
_REGISTERED_RBX_FUNCTIONS: Dict[Any, Dict[str, Any]] = {}


def rbx_function(params: Dict[str, Any]):
    """
    Decorator to mark a Python function as a "riverbox cube".
    The `params` dict can contain keys like "kind", "name", "arg-key", "default-value", etc.
    We store it in a global registry so that Flow.add_cube(fn) knows how to build it.
    """
    def decorator(fn):
        _REGISTERED_RBX_FUNCTIONS[fn] = params.copy()
        return fn
    return decorator


def _new_id() -> str:
    """Generate a fresh UUID string each time."""
    return str(uuid.uuid4())


# --------------------------------------------------------------------------------
# C L A S S   C U B E
# --------------------------------------------------------------------------------
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
        tag_stack: Optional[List[List[str]]] = None,
        # --- EXTRA FIELDS FOR A "FLOW" cube ---
        inner_cubes: Optional[List["Cube"]] = None,
        execution_id: Optional[str] = None,
        run_on_same: Optional[bool] = None,
        sub_flow_id: Optional[str] = None,
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
        self.tag_stack: List[List[str]] = [list(ts) for ts in tag_stack] if tag_stack else []
        # Always keep track of edges out of this cube
        self.start_edges: List[Dict[str, Any]] = []

        # Only used if kind == "FLOW":
        self.inner_cubes: List[Cube] = list(inner_cubes) if inner_cubes else []
        self.execution_id: Optional[str] = execution_id
        self.run_on_same: Optional[bool] = run_on_same
        self.sub_flow_id: Optional[str] = sub_flow_id
        self.env: Dict[str, Any] = env.copy() if env else {}

    def add_edge_to(
        self,
        target: "Cube",
        *,
        end_arg_key: Any,
        start_arg_key: Any = None,
        kind: str = "REGULAR",
        edge_id: Optional[str] = None,
    ) -> None:
        """
        Create a directed edge from THIS cube → target cube.
        - end_arg_key: which argument on the target this edge feeds.
        - start_arg_key: which output key on this cube is being sent (or None).
        - kind: e.g. "REGULAR", "MAP", etc.
        """
        eid = edge_id or _new_id()
        edge_dict = {
            "id": eid,
            "end": target.id,
            "end-arg-key": end_arg_key,
            "start-arg-key": start_arg_key,
            "kind": kind,
        }
        self.start_edges.append(edge_dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize this Cube into a dict exactly matching your JSON spec.
        Emits different fields depending on self.kind:
        - PARAM: needs "arg-key" + "default-value".
        - REGULAR: needs "code".
        - RESULT: needs "arg-key".
        - FLOW: needs "execution-id", "run-on-same", "sub-flow-id", "env", "tags", "tag-stack", and nested "cubes".
        Always includes "start-edges" (even if empty).
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

        # FLOW cubes (embed nested cubes, plus extra flow‐specific fields)
        if self.kind == "FLOW":
            if self.execution_id is not None:
                base["execution-id"] = self.execution_id
            # run-on-same must be included (default to False if None)
            base["run-on-same"] = self.run_on_same if self.run_on_same is not None else False
            base["sub-flow-id"] = self.sub_flow_id if self.sub_flow_id is not None else ""
            if self.env:
                base["env"] = self.env
            if self.tags:
                base["tags"] = list(self.tags)
            if self.tag_stack:
                base["tag-stack"] = [list(ts) for ts in self.tag_stack]
            # Now embed the nested cubes
            base["cubes"] = [c.to_dict() for c in self.inner_cubes]

        # If there is per‐cube metadata for non‐FLOW cubes, include it
        if self.metadata and self.kind != "FLOW":
            base["metadata"] = self.metadata

        # Always attach start-edges (even if empty)
        base["start-edges"] = list(self.start_edges)
        return base


# --------------------------------------------------------------------------------
# C L A S S   F L O W
# --------------------------------------------------------------------------------
class Flow:
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        execution_id: Optional[str] = None,
        run_on_same: Optional[bool] = None,
        sub_flow_id: Optional[str] = None,
        riverbox_version: float = 1.0,
        language: str = "python",
        version: str = "3.11",
        tags: Optional[List[str]] = None,
        tag_stack: Optional[List[List[str]]] = None,
        env: Optional[Dict[str, Any]] = None,
        args: Optional[Dict[str, Any]] = None,
    ):
        # If this Flow is later embedded as a Cube(kind="FLOW"), these fields are used.
        self.name: Optional[str] = name or ""
        self.execution_id: Optional[str] = execution_id
        self.run_on_same: Optional[bool] = run_on_same
        self.sub_flow_id: Optional[str] = sub_flow_id

        # Top-level metadata (these go into the top JSON's "metadata")
        self.metadata: Dict[str, Any] = {
            "riverbox-version": riverbox_version,
            "language": language,
            "version": version,
        }
        self.tags: List[str] = list(tags) if tags else []
        self.tag_stack: List[List[str]] = [list(ts) for ts in tag_stack] if tag_stack else []

        # flow-level env/args
        self.env: Dict[str, Any] = env.copy() if env else {}
        self.args: Dict[str, Any] = args.copy() if args else {}

        # Internally store all cubes (including nested flows if any)
        self._cubes: List[Cube] = []

    def add_cube(self, spec: Any) -> Cube:
        """
        Two special cases:
          1. If 'spec' is a Python function previously decorated @rbx_function(…),
             we look it up in _REGISTERED_RBX_FUNCTIONS, grab its params + source code,
             and construct a Cube(kind="REGULAR", code=…).
          2. If 'spec' is a dict, we assume the user already knows the exact fields
             for a single cube (including maybe a "FLOW" cube that already has its
             nested "cubes": […]). We shallow-copy those fields into a new Cube.
          3. If 'spec' is a Flow object, we wrap it into a Cube(kind="FLOW", …),
             pulling in spec.name, spec.execution_id, spec.run_on_same, spec.sub_flow_id,
             spec.env, spec.tags, spec.tag_stack, and for each nested sub-cube in spec._cubes,
             we embed them. 
        """
        # --- CASE A: a decorated function  ---
        if callable(spec) and spec in _REGISTERED_RBX_FUNCTIONS:
            params = _REGISTERED_RBX_FUNCTIONS[spec]
            kind = params.get("kind", "REGULAR")
            name = params.get("name", spec.__name__)
            try:
                source = inspect.getsource(spec)
            except OSError:
                source = None

            cube = Cube(
                id=params.get("id"),
                kind=kind,
                name=name,
                code=source,
                arg_key=params.get("arg-key"),
                default_value=params.get("default-value"),
                metadata=params.get("metadata"),
                tags=params.get("tags"),
                tag_stack=params.get("tag-stack"),
            )
            self._cubes.append(cube)
            return cube

        # --- CASE B: a raw dict describing a single cube  ---
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
                tag_stack=spec.get("tag-stack"),
                # If it's already a FLOW‐dict, pull out nested cubes
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
                        tag_stack=sc.get("tag-stack"),
                        execution_id=sc.get("execution-id"),
                        run_on_same=sc.get("run-on-same"),
                        sub_flow_id=sc.get("sub-flow-id"),
                        env=sc.get("env"),
                        # Recursive: if sc itself has "cubes", build them too
                        inner_cubes=[
                            Cube(**{
                                "id": subsc.get("id"),
                                "kind": subsc["kind"],
                                "name": subsc["name"],
                                "code": subsc.get("code"),
                                "arg_key": subsc.get("arg-key"),
                                "default_value": subsc.get("default-value"),
                                "metadata": subsc.get("metadata"),
                                "tags": subsc.get("tags"),
                                "tag_stack": subsc.get("tag-stack"),
                                "execution_id": subsc.get("execution-id"),
                                "run_on_same": subsc.get("run-on-same"),
                                "sub_flow_id": subsc.get("sub-flow-id"),
                                "env": subsc.get("env", {}),
                                "inner_cubes": [] if not subsc.get("cubes") else None,  # deeper nesting can likewise be handled recursively
                            })
                            for subsc in sc.get("cubes", [])
                        ]
                    )
                    for sc in spec.get("cubes", [])
                ] if kind == "FLOW" else [],
                execution_id=spec.get("execution-id"),
                run_on_same=spec.get("run-on-same"),
                sub_flow_id=spec.get("sub-flow-id"),
                env=spec.get("env"),
            )
            # If the dict already had its own "start-edges", copy them in
            if "start-edges" in spec:
                cube.start_edges = list(spec["start-edges"])
            self._cubes.append(cube)
            return cube

        # --- CASE C: a Flow object → embed it as a "FLOW" cube  ---
        elif isinstance(spec, Flow):
            # Use spec.name, spec.execution_id, spec.run_on_same, spec.sub_flow_id, spec.env, spec.tags, spec.tag_stack
            cube = Cube(
                id=None,
                kind="FLOW",
                name=spec.name or "",
                code=None,
                arg_key=None,
                default_value=None,
                metadata=dict(spec.metadata),
                tags=spec.tags,
                tag_stack=spec.tag_stack,
                inner_cubes=list(spec._cubes),
                execution_id=spec.execution_id or _new_id(),
                run_on_same=spec.run_on_same if spec.run_on_same is not None else False,
                sub_flow_id=spec.sub_flow_id or "",
                env=spec.env,
            )
            self._cubes.append(cube)
            return cube

        else:
            raise ValueError(
                "add_cube expects either: \n"
                "  • a function decorated with @rbx_function, \n"
                "  • a dict describing a single cube, or \n"
                "  • a Flow object (to embed as kind='FLOW')."
            )

    def to_json(self, indent: int = 2) -> str:
        """
        Emit the full JSON representation of this Flow:
        {
          "metadata": {…},
          "tags": […],
          "tag-stack": [ […], … ],
          "env": {…},
          "args": {…},
          "flow": {
            "cubes": [ <each cube.to_dict()> … ]
          }
        }
        """
        full_dict: Dict[str, Any] = {
            "metadata": self.metadata,
            "tags": self.tags,
            "tag-stack": self.tag_stack,
            "env": self.env,
            "args": self.args,
            "flow": {
                "cubes": [cube.to_dict() for cube in self._cubes]
            },
        }
        return json.dumps(full_dict, indent=indent)
