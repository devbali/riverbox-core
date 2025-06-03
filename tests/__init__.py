import inspect

def multi_language(*languages):
    def decorator(f):
        def wrapper ():
            riverbox = None
            print(f)
            sig = inspect.signature(f)
        
            # Get default values
            defaults = {
                k: v.default
                for k, v in sig.parameters.items()
                if v.default is not inspect.Parameter.empty
            }
            rbx_kwarg_key = None
            for kwarg_key in defaults:
                kwarg = defaults[kwarg_key]
                if isinstance(kwarg, dict) and "metadata" in kwarg and "riverbox-version" in kwarg["metadata"]:
                    riverbox = kwarg
                    rbx_kwarg_key = kwarg_key

            assert riverbox is not None
            assert rbx_kwarg_key is not None

            for language in languages:
                print(f"LANGUAGE OF RIVERBOX: {language}")
                riverbox["metadata"]["language"] = language
                f(**{rbx_kwarg_key:riverbox})
        return wrapper
    return decorator
