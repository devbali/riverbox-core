{
  "metadata": {
    "riverbox-version": 1.0,
    "language": "python",
    "version": "3.11"
  },
  "tags": ["parent_tag"],
  "tag-stack": [["parent_tag"]],
  "env": {},
  "args": {},
  "flow": {
    "cubes": [
      {
        "id": "49929a27-f81b-4d80-96c4-486631a2a101",
        "kind": "PARAM",
        "name": "Topics",
        "arg-key": "prompt",
        "default-value": "[[\"bears\"], [\"dogs\"]]",
        "start-edges": [
          {
            "id": "c8a0fd78-ae6d-4ccc-a672-841ba913749b",
            "end": "8f39f94f-8df6-45d6-a307-10d1af518e93",
            "end-arg-key": "topics",
            "start-arg-key": null,
            "kind": "MAP"
          },
          {
            "id": "4f011b8e-c5dc-42ff-b577-5dcf081c91f0",
            "end": "e2b05fd1-651a-49b8-b387-826a9ca0d59c",
            "end-arg-key": "prompt",
            "start-arg-key": null,
            "kind": "MAP"
          }
        ]
      },
      {
        "id": "e2b05fd1-651a-49b8-b387-826a9ca0d59c",
        "kind": "FLOW",
        "name": "Sleep 0.2",
        "execution-id": "52cec591-e2da-4b8c-a7cc-f3ca03e154a4",
        "metadata": {
          "riverbox-version": 1.0,
          "language": "python",
          "version": "3.12",
          "description": ""
        },
        "run-on-same": true,
        "sub-flow-version-id": "",
        "env": {},
        "tags": ["sub_tag"],
        "tag-stack": [
          ["parent_tag"],
          ["sub_tag"]
        ],
        "cubes": [
          {
            "id": "d1057352-5b67-4a86-a42d-0374fbdc8159",
            "kind": "PARAM",
            "name": "Prompt",
            "arg-key": "prompt",
            "default-value": "[\"default prompt in sub\", \"prompt 2\"]",
            "start-edges": [
              {
                "id": "1bdf1c5a-6185-4fd6-812b-9f1146c90e20",
                "end": "2c8dbf7c-4de2-433d-b023-7cc32f595fda",
                "end-arg-key": "arg",
                "start-arg-key": null,
                "kind": "MAP"
              }
            ]
          },
          {
            "id": "c69d1215-fa4d-4722-93f7-3dda9c183a4a",
            "kind": "PARAM",
            "name": "Multiplier",
            "arg-key": "multiplier",
            "default-value": "10",
            "start-edges": [
              {
                "id": "8bdf1b9a-1b24-4320-aab5-47bc075d22d5",
                "end": "2c8dbf7c-4de2-433d-b023-7cc32f595fda",
                "end-arg-key": -1,
                "start-arg-key": null,
                "kind": "MAP"
              }
            ]
          },
          {
            "id": "2c8dbf7c-4de2-433d-b023-7cc32f595fda",
            "kind": "REGULAR",
            "name": "Multiplier Code",
            "code": "import time\n\n@rbxm.main_handler\ndef multiplier(arg):\n  time.sleep(0.3)\n  return arg",
            "start-edges": [
              {
                "id": "e8b8ecc7-27f6-4d7b-821e-1164159f5e72",
                "end": "6d3b2f41-fd4e-432f-97f2-b4279a600c4a",
                "end-arg-key": "arg",
                "start-arg-key": null,
                "kind": "REGULAR"
              }
            ]
          },
          {
            "id": "6d3b2f41-fd4e-432f-97f2-b4279a600c4a",
            "kind": "REGULAR",
            "name": "Aggregator",
            "code": "@rbxm.main_handler\ndef choose_straight (arg):\n  return {\"straight-result\": arg}",
            "start-edges": [
              {
                "id": "5b7d7c9e-c01f-4b93-96eb-433eb0773fb6",
                "end": "d688e071-5e08-4550-8feb-6947b9c04ec5",
                "end-arg-key": null,
                "start-arg-key": "straight-result",
                "kind": "REGULAR"
              },
              {
                "id": "0789faa6-aad4-4481-9186-a3fe1586561c",
                "end": "e7de30d0-0ad3-4ce6-abb6-385957ff3769",
                "end-arg-key": -1,
                "start-arg-key": "wait-result",
                "kind": "REGULAR"
              }
            ]
          },
          {
            "id": "d688e071-5e08-4550-8feb-6947b9c04ec5",
            "kind": "RESULT",
            "name": "Straight Result (without wait)",
            "arg-key": "straight-result"
          },
          {
            "id": "e7de30d0-0ad3-4ce6-abb6-385957ff3769",
            "kind": "REGULAR",
            "name": "Wait Code",
            "start-edges": [],
            "code": "import time\n\n@rbxm.main_handler\ndef multiplier(prompt):\n  time.sleep(1)"
          }
        ],
        "start-edges": [
          {
            "id": "a7bab300-a423-4095-a7f7-d862e0ccfad1",
            "end": "9b46ee41-ab07-43b8-809b-43650dca745e",
            "end-arg-key": null,
            "start-arg-key": "straight-result",
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "9b46ee41-ab07-43b8-809b-43650dca745e",
        "kind": "RESULT",
        "name": "Nest Result",
        "arg-key": "nest-result"
      },
      {
        "id": "29706140-cc03-4141-a5bb-4df4763bc0cc",
        "kind": "REGULAR",
        "code": "@rbxm.main_handler\ndef chooseWinner(gptPick=\"2\", claudePick=\"2\"):\n\n\n  if \"1\" in gptPick and \"1\" in claudePick:\n    return \"ChatGPT\"\n    \n  if \"2\" in gptPick and \"2\" in claudePick:\n    return \"Claude\"\n    \n  elif \"2\" in gptPick and \"1\" in claudePick:\n    return \"Tie, each system likes the other's joke better\"\n    \n  elif \"1\" in gptPick and \"2\" in claudePick:\n    return \"Tie, each system likes their own joke better\"\n    \n  else:\n    return \"No result\"",
        "name": "Choose Winner",
        "start-edges": [
          {
            "id": "00edd319-2165-423a-8eb0-c7a86e3a3e31",
            "end": "c5953e37-f04f-4f92-9829-e638625b714b",
            "end-arg-key": null,
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "c5953e37-f04f-4f92-9829-e638625b714b",
        "kind": "RESULT",
        "name": "Result",
        "arg-key": "result_str"
      },
      {
        "id": "8f39f94f-8df6-45d6-a307-10d1af518e93",
        "kind": "REGULAR",
        "code": "@rbxm.main_handler\ndef makeJokePrompt (topics):\n\n  print(\"RUNNING ON TOPIC \", topics)\n  return f\"Write a joke about {topics[0]}\"\n  ",
        "name": "Joke Prompt",
        "start-edges": [
          {
            "id": "23b69f37-8ee3-4199-872e-e3383470a9f2",
            "end": "026f4cea-befe-44e2-a6ff-31cab1c5cfac",
            "end-arg-key": "prompt",
            "start-arg-key": null,
            "kind": "REGULAR"
          },
          {
            "id": "bb0b22c3-ad8e-4adf-b796-2245c665855b",
            "end": "5fc74cca-dc97-455a-b426-ead826ef6c19",
            "end-arg-key": "prompt",
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "026f4cea-befe-44e2-a6ff-31cab1c5cfac",
        "kind": "REGULAR",
        "code": "import time\n\n@rbxm.main_handler\ndef chatGPT(prompt):\n  time.sleep(0.2)\n  return \"Bears are funny (GPT)\"",
        "name": "Chat GPT",
        "start-edges": [
          {
            "id": "e513611e-d08b-4194-974d-e94c5ecb2675",
            "end": "6e55bb9c-6479-4cd7-ade3-5218620f4b40",
            "end-arg-key": "joke1",
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "5fc74cca-dc97-455a-b426-ead826ef6c19",
        "kind": "REGULAR",
        "code": "import time\n\n@rbxm.main_handler\ndef chatGPT(prompt):\n  time.sleep(0.1)\n  return \"Bears are funny (Claude)\"",
        "name": "Claude",
        "start-edges": [
          {
            "id": "0c644c18-13c8-45e9-8aad-85dd0c254f46",
            "end": "6e55bb9c-6479-4cd7-ade3-5218620f4b40",
            "end-arg-key": "joke2",
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "8fa133cc-017b-4ce2-bb32-f67f074583f1",
        "kind": "REGULAR",
        "code": "import time\n\n@rbxm.main_handler\ndef chatGPT(prompt):\n  time.sleep(0.1)\n  return \"1\"",
        "name": "Claude",
        "start-edges": [
          {
            "id": "3f76058b-eed5-4f39-88a2-441c6656cc55",
            "end": "29706140-cc03-4141-a5bb-4df4763bc0cc",
            "end-arg-key": "claudePick",
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "731e6e4f-c776-46ac-b6a4-8c9bb77d0213",
        "kind": "REGULAR",
        "code": "import time\n\n@rbxm.main_handler\ndef chatGPT(prompt):\n  time.sleep(0.2)\n  return \"1\"",
        "name": "Chat GPT",
        "start-edges": [
          {
            "id": "ffd2f528-88a6-4098-9257-af4e6f1f68ef",
            "end": "29706140-cc03-4141-a5bb-4df4763bc0cc",
            "end-arg-key": "gptPick",
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "6e55bb9c-6479-4cd7-ade3-5218620f4b40",
        "kind": "REGULAR",
        "code": "def pickTheBetterJoke(joke1, joke2):\n  return f\"Which Joke is better? \\n Joke 1:{joke1} \\n Joke 2:{joke2}. Just respond either 1 or 2. Nothing Else\"",
        "name": "Pick the better joke prompt",
        "start-edges": [
          {
            "id": "f450857c-3136-4563-a129-21fe75fc2678",
            "end": "731e6e4f-c776-46ac-b6a4-8c9bb77d0213",
            "end-arg-key": "prompt",
            "start-arg-key": null,
            "kind": "REGULAR"
          },
          {
            "id": "14d4b48e-c33f-439a-8417-9a5ff97a09a2",
            "end": "8fa133cc-017b-4ce2-bb32-f67f074583f1",
            "end-arg-key": "prompt",
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      }
    ]
  }
}
