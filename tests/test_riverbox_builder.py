
from riverbox_builder import Flow, rbx_function

# 1) Define two Python functions and mark them as cubes
@rbx_function({
    "kind": "REGULAR",
    "name": "makeJokePrompt",
})
def make_joke_prompt(topics):
    print("RUNNING ON TOPIC", topics)
    return f"Write a joke about {topics[0]}"


@rbx_function({
    "kind": "REGULAR",
    "name": "chatGPT",
})
def chat_gpt(prompt):
    import time
    time.sleep(0.2)
    return "Bears are funny (GPT)"


# 2) Build a sub‐flow “Sleep 0.2” that has:
#      • a PARAM cube (“Topics”),
#      • then a REGULAR cube (“Joke Prompt” → “Chat GPT”),
#      • and finally a RESULT cube (“Straight Result”).
sub_f = Flow(
    name="Sleep 0.2",
    execution_id="52cec591-e2da-4b8c-a7cc-f3ca03e154a4",
    run_on_same=True,
    sub_flow_id="",
    riverbox_version=1.0,
    language="python",
    version="3.12",
    tags=["sub_tag"],
    tag_stack=[["parent_tag"], ["sub_tag"]],
    env={},
    args={},
)

# 2a) PARAM cube inside sub_f:
param_topics = sub_f.add_cube({
    "id": "49929a27-f81b-4d80-96c4-486631a2a101",
    "kind": "PARAM",
    "name": "Topics",
    "arg-key": "prompt",
    "default-value": '[["bears"], ["dogs"]]',
    "start-edges": [],  # we’ll wire these after we add the other cubes
})

# 2b) “Joke Prompt” REGULAR cube inside sub_f (using the decorated function):
joke_prompt = sub_f.add_cube(make_joke_prompt)

# 2c) “Chat GPT” REGULAR cube inside sub_f:
chat_cube = sub_f.add_cube(chat_gpt)

# 2d) RESULT cube (“Straight Result”) inside sub_f:
straight_result = sub_f.add_cube({
    "kind": "RESULT",
    "name": "Straight Result (without wait)",
    "arg-key": "straight-result",
})

# 2e) Wire them in sub_f exactly as in your JSON:
#     param_topics → make_joke_prompt (map “topics” → “topics”)
param_topics.add_edge_to(joke_prompt, end_arg_key="topics", start_arg_key=None, kind="MAP")

#     make_joke_prompt → chatGPT (feed “prompt” → “prompt”)
joke_prompt.add_edge_to(chat_cube, end_arg_key="prompt", start_arg_key=None, kind="REGULAR")

#     chatGPT → straight_result (feed “straight-result”)
chat_cube.add_edge_to(straight_result, end_arg_key="straight-result", start_arg_key=None, kind="REGULAR")


# 3) Build the **parent** Flow and embed sub_f as a single cube of kind="FLOW":
parent_f = Flow(
    name="Parent Flow",
    riverbox_version=1.0,
    language="python",
    version="3.11",
    tags=["parent_tag"],
    tag_stack=[["parent_tag"]],
    env={},
    args={},
)

# 3a) This call will wrap the entire sub_f (with its 4 cubes above) 
#     into one Cube(kind="FLOW", …).  Its ID will be auto‐generated,
#     name="Sleep 0.2", execution-id, run-on-same, etc.  All nested cubes
#     live inside parent_f._cubes[0].inner_cubes.
flow_cube = parent_f.add_cube(sub_f)

# 3b) You can still add more cubes to parent_f if needed.
#     For example, add a top‐level RESULT cube (“Nest Result”):
nest_result = parent_f.add_cube({
    "kind": "RESULT",
    "name": "Nest Result",
    "arg-key": "nest-result",
})

# 3c) Wire the “FLOW” cube → “Nest Result”:
#     Suppose the flow‐cube’s output “straight-result” feeds the parent’s “nest-result”
flow_cube.add_edge_to(nest_result, end_arg_key="nest-result", start_arg_key="straight-result", kind="REGULAR")


# 4) Finally, dump the entire parent_f to JSON:
print(parent_f.to_json())

