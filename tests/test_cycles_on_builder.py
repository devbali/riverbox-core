
from riverbox_builder import Flow, rbx_function
import json

def get_flow ():
    # 1) Define two Python functions and mark them as cubes
    @rbx_function()
    def init_counter(rbxm, counter):
        counter = rbxm.input["counter"]
        rbxm.output = True # TODO: should not be needed
        
    @rbx_function()
    def check_if_counter_is_0(rbxm, counter):
        if counter == 0:
            rbxm.output = {"end": True, "more": None}
        else:
            rbxm.output = {"end": None, "more": True}


    @rbx_function()
    def return_counter (rbxm, counter):
        rbxm.output = counter
    
    @rbx_function()
    def decrement_counter (rbxm, counter):
        rbxm.output = counter - 1

    flow = Flow(
        name="Cyclical",
        sub_flow_id="",
        riverbox_version=1.0,
        language="python",
        version="3.12",
        tags=["sub_tag"],
        env={},
        args={},
    )

    # 2a) PARAM cube inside sub_f:
    param_topics = sub_f.add_cube({
        "kind": "PARAM",
        "name": "Topics",
        "arg-key": "prompt",
        "default-value": '["bears", "dogs"]',
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
    joke_prompt.add_edge_to(chat_cube, end_arg_key="prompt", start_arg_key=None, kind="MAP")

    #     chatGPT → straight_result (feed “straight-result”)
    chat_cube.add_edge_to(straight_result, end_arg_key=None, start_arg_key=None, kind="REGULAR")

    return sub_f
