import json
from jupyter_client import KernelManager
from jupyter_client.provisioning import LocalProvisioner
import os
from jupyter_client.launcher import launch_kernel
from threading import Lock
from _queue import Empty

class KernelObject:
    def __init__(self):
        self.km = KernelManager()
        self.stale = False
        self.started = False
    
    def start (self):
        # Create a new kernel manager
        self.km.start_kernel()

        # Create a client to interact with the kernel
        self.kc = self.km.client()
        self.kc.start_channels()
        self.started = True

    def execute_code (self, code):
        try:
            # Execute the code
            print("executing code \n\n\n\n", code)
            self.kc.execute(code)

            # Wait for and capture the execution result
            shell_msg = self.kc.get_shell_msg (timeout=1)
            print("shell", shell_msg)

            console_value = ""
            error_value = ""
            return_value = None
            empty_count = 0

            while shell_msg["content"]["status"] == "ok":
                if empty_count == 30:
                    break

                try:
                    msg = self.kc.get_iopub_msg(timeout=1)
                    print("iopub", msg)

                except Empty:
                    print("Empty iopub")
                    empty_count += 1
                    continue

                if msg["msg_type"] == "status" and msg["content"]["execution_state"] == "idle":
                    break

                elif msg["msg_type"] == "stream" and msg["content"]["name"] == "stdout":
                    console_output += msg["content"]["text"] 

                elif msg['msg_type'] == 'execute_result':
                    console_output += msg['content']['data']

                elif msg['msg_type'] == 'error':
                    error_output += "ERROR:\n" + msg['content']

            # Return the result as JSON
            print("returning from execute code ipynb",  console_value, error_value, return_value)
            return console_value, error_value, return_value

        finally:
            # Clean up
            self.kc.stop_channels()
            self.km.shutdown_kernel()

    def clone (self):
        # for now just return a new one
        return KernelObject()


class KernelFactory:
    def __init__ (self):
        self.kernels = {}
        self.len_kernels = 0
        self.lock = Lock()
    
    def new_kernel (self, from_key=None) -> KernelObject:
        with self.lock:
            new_key = self.len_kernels
            self.len_kernels += 1
        if from_key is None:
            self.kernels[new_key] = KernelObject()
        else:
            assert from_key in self.kernels
            self.kernels[new_key] = self.kernels[from_key].clone()

        self.kernels[new_key].start()
        return self.kernels[new_key]

# STATIC
kernel_factory = KernelFactory()

def execute_code (cube_execution_layer, global_vars, flow):
    console_value, error_value, return_value = kernel_factory.new_kernel().execute_code(cube_execution_layer.cube.code)
    cube_execution_layer.console_value = console_value
    cube_execution_layer.return_value = return_value
    if error_value: cube_execution_layer.error(error_value)
