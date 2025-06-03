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
        "id": "29706140-cc03-4141-a5bb-4df4763bc0cc",
        "kind": "REGULAR",
        "code": "x = 3\ny = 5\nprint(x,y)",
        "name": "Set global variables",
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
        "code": "print(y-x)\nx+y",
        "name": "Access Global Variables",
        "start-edges": [
        ]
      }
    ]
  }
}
