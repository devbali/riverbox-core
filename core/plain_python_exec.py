import traceback
import io
from contextlib import redirect_stdout
import threading
import sys


# Create a thread-local object to hold per-thread stdout
_thread_local = threading.local()

class ThreadSafeStdout:
    def write(self, data):
        target = getattr(_thread_local, 'stdout', sys.__stdout__)
        return target.write(data)

    def flush(self):
        target = getattr(_thread_local, 'stdout', sys.__stdout__)
        return target.flush()

# Store the global instance of ThreadSafeStdout and replace sys.stdout
_global_thread_safe_stdout_instance = ThreadSafeStdout()
sys.stdout = _global_thread_safe_stdout_instance

class ThreadStdoutRedirect:
    def __init__(self, file_path_or_obj, mode='w'):
        self._original_sys_stdout = None
        self._original_thread_local_stdout = None # For nesting ThreadStdoutRedirect
        self._we_set_thread_local_stdout = False

        if isinstance(file_path_or_obj, str):
            self._target_stream = open(file_path_or_obj, mode, buffering=1)  # line-buffered
            self._close_on_exit = True
        else:
            self._target_stream = file_path_or_obj
            self._close_on_exit = False

    def __enter__(self):
        self._original_sys_stdout = sys.stdout  # Save current sys.stdout (e.g., pytest's capturer)

        if hasattr(_thread_local, 'stdout'):
            self._original_thread_local_stdout = _thread_local.stdout
        
        _thread_local.stdout = self._target_stream  # Point thread-local to our target
        self._we_set_thread_local_stdout = True
        
        # Ensure sys.stdout is our global ThreadSafeStdout instance
        sys.stdout = _global_thread_safe_stdout_instance 
        
        return self._target_stream

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._original_sys_stdout is not None:
            sys.stdout = self._original_sys_stdout # Restore previous sys.stdout

        if self._we_set_thread_local_stdout:
            if self._original_thread_local_stdout is not None:
                _thread_local.stdout = self._original_thread_local_stdout
            elif hasattr(_thread_local, 'stdout') and getattr(_thread_local, 'stdout', None) is self._target_stream:
                del _thread_local.stdout

        if self._close_on_exit:
            self._target_stream.close()

def execute_code (cube_execution_layer, global_vars, flow):
    try:
        from .. import riverbox_frontend
    except ImportError:
        import riverbox_frontend

    try:
        flow.global_vars_lock.acquire()
        try:
            local_global_vars = {**global_vars, **cube_execution_layer.args}
        except:
            local_global_vars = {**global_vars}
        flow.global_vars_lock.release()

        rbxm = riverbox_frontend.RiverboxCubeManager(cube_execution_layer.args, 
                                                     cube_execution_layer.get_client_box_metadata(),
                                                     flow.get_client_riverbox_metadata())
        local_global_vars["rbx"] = riverbox_frontend
        local_global_vars["rbxm"] = rbxm

        with ThreadStdoutRedirect(cube_execution_layer.console_output):
            exec(cube_execution_layer.cube.code, local_global_vars)

            rbxm = local_global_vars["rbxm"]
            rbxm.finish()

        flow.global_vars_lock.acquire()
        flow.global_vars = {**flow.global_vars, **local_global_vars}
        flow.global_vars_lock.release()

        cube_execution_layer.return_value = rbxm.output

    except Exception:
        cube_execution_layer.errored = True
        contents = cube_execution_layer.console_output.getvalue()
        cube_execution_layer.console_output.close()
        cube_execution_layer.error(str(cube_execution_layer.execution_id), flow, "\n\nERROR:\n" + traceback.format_exc() + "\n", contents)
