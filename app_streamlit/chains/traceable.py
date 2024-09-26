
from app_streamlit.tracing.langfuse import *


class TraceableChain:
    def __call__(self, *args, **kwargs):
        callbacks = kwargs.get("callbacks", [])
        callbacks.append(trace.getNewHandler())
        kwargs["callbacks"] = callbacks
        return super().__call__(*args, **kwargs)
