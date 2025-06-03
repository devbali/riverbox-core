import time

from .RiverboxCubeManager import RiverboxCubeManager

# class GPT():
#     """
#     Static class which defines all useful methods using the GPT LLM
#     We use an API key and send requests to this LLM via the openai Python SDK
#     """

#     def get_bullets(answer: str) -> list[str]:
#         """
#         Helper method to take an answer that GPT has given which is supposed to have bullets,
#             and get back a python `list` of strings where each is a bullet point

#         Args:
#             answer (str): The answer which was given by GPT

#         Returns:
#             list[str]: The bullet points as a python `list`
#         """
    
#         lines: list[str] = [s.strip() for s in answer.split("\n") if s.strip()]
#         bullets: list[str] = []
#         for line in lines:
#             if "-" in line:
#                 bullets.append("-".join(line.split("-")[1:]))
#         return bullets


#     def query(q: str, messages: list[dict] = []) -> str:
#         """
#         Helper function to abstract away a GPT query
#         You simply give it a string `q` "Question" or "Prompt" as a Python string, and receive the "Answer"

#         Args:
#             q (str): "Question" or prompt to GPT

#         Returns:
#             str: Answer given back by GPT
#         """
        

#         # While loop: Keep requesting as long as an answer is not received
#         response = None
#         counter = 0
#         while response is None and counter < 11:
#             counter += 1
#             try:
#                 response = openai.ChatCompletion.create(
#                     model="gpt-3.5-turbo",  # Use the latest model available
#                     messages=[
#                     *messages,
#                     # {"role": "system", "content": "You are a helpful assistant."},
#                     {'role': 'user', 'content': q}
#                     ]
#                 )
            
#             except openai.error.RateLimitError:
#                 print(f"Rate limit error in GPT Query")
#                 # Save QA record in Database
#                 time.sleep(1)
            
#             except openai.error.ServiceUnavailableError:
#                 print(f"OpenAI Service Unavailable error")
#                 time.sleep(1)

#         ans: str = response.choices[0].message.content if response else None
#         return ans
    