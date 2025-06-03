{
  "metadata": {
    "riverbox-version": 1.0,
    "language": "python",
    "version": "3.11"
  },
  "env": {},
  "args": {},
  "flow": {
    "cubes": [
      {
        "id": "9b46ee41-ab07-43b8-809b-43650dca745e",
        "kind": "RESULT",
        "name": "Main Result",
        "arg-key": "main-result"
      },
      {
        "id": "29706140-cc03-4141-a5bb-4df4763bc0cc",
        "kind": "REGULAR",
        "code": "class t:\n  def __init__ (self, a):\n    self.a = a\n\nobjs = [t(1), t(2)]\n@rbxm.main_handler\ndef returnObjects():\n  return objs",
        "name": "Return Objects",
        "start-edges": [
          {
            "id": "00edd319-2165-423a-8eb0-c7a86e3a3e31",
            "end": "8f39f94f-8df6-45d6-a307-10d1af518e93",
            "end-arg-key": "obj",
            "start-arg-key": null,
            "kind": "MAP"
          }
        ]
      },
      {
        "id": "8f39f94f-8df6-45d6-a307-10d1af518e93",
        "kind": "REGULAR",
        "code": "print(objs, objs[0].a, objs[1].a)\n@rbxm.main_handler\ndef returnA (obj):\n  print('obj received: ', obj.a)\n  return obj.a",
        "name": "Return attribute",
        "start-edges": [
          {
            "id": "23b69f37-8ee3-4199-872e-e3383470a9f2",
            "end": "9b46ee41-ab07-43b8-809b-43650dca745e",
            "end-arg-key": null,
            "start-arg-key": null,
            "kind": "REGULAR"
          }
        ]
      }
    ]
  }
}
