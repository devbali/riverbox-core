{
  "metadata": {
    "riverbox-version": 1.0,
    "language": "python",
    "version": "3.12",
    "description": ""
  },
  "env": {},
  "args": {},
  "flow": {
    "cubes": [
      {
        "id": "9b46ee41-ab07-43b8-809b-43650dca745e",
        "kind": "PARAM",
        "name": "Initial Value",
        "arg-key": "initial_value",
        "default-value": 2,
        "start-edges": [
            {
                "id": "10edd319-2165-423a-8eb0-c7a86e3a3e31",
                "end": "29706140-cc03-4141-a5bb-4df4763bc0cc",
                "end-arg-key": "initial_value",
                "start-arg-key": null,
                "kind": "REGULAR"
            }
        ]
      },
      {
        "id": "29706140-cc03-4141-a5bb-4df4763bc0cc",
        "kind": "REGULAR",
        "code": "a = rbxm.input[\"initial_value\"]",
        "name": "Set global variable",
        "start-edges": [
          {
            "id": "00edd319-2165-423a-8eb0-c7a86e3a3e31",
            "end": "8f39f94f-8df6-45d6-a307-10d1af518e93",
            "end-arg-key": "-2",
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "8f39f94f-8df6-45d6-a307-10d1af518e93",
        "kind": "REGULAR",
        "code": "a += 1\nprint(\"AAAAAAAAAAAAA\",a)\nrbxm.output = a",
        "name": "Increment",
        "start-edges": [
          {
            "id": "23b69f37-8ee3-4199-872e-e3383470a9f2",
            "end": "4339f94f-8df6-45d6-a307-10d1af518e93",
            "end-arg-key": "n",
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      },
      {
        "id": "4339f94f-8df6-45d6-a307-10d1af518e93",
        "kind": "REGULAR",
        "code": "rbxm.output = list(range(rbxm.input[\"n\"]))",
        "name": "Iterator",
        "start-edges": [
          {
            "id": "23b09f37-8ee3-4199-872e-e3383470a9f2",
            "end": "ee39f94f-8df6-45d6-a307-10d1af518e93",
            "end-arg-key": "n",
            "start-arg-key": null,
            "kind": "REGULAR"
          },
          {
            "id": "56b69f37-8ee3-4199-872e-e3383470a9f2",
            "end": "7739f94f-8df6-45d6-a307-10d1af518e93",
            "end-arg-key": "n",
            "start-arg-key": null,
            "kind": "MAP"
          }
        ]
      },
      {
        "id": "ee39f94f-8df6-45d6-a307-10d1af518e93",
        "kind": "REGULAR",
        "code": "rbxm.output = max(rbxm.input[\"n\"])",
        "name": "Maximum",
        "start-edges": []
      },
      {
        "id": "7739f94f-8df6-45d6-a307-10d1af518e93",
        "kind": "REGULAR",
        "code": "rbxm.output = rbxm.input[\"n\"] * 1000",
        "name": "1000x",
        "start-edges": []
      }
    ]
  }
}
