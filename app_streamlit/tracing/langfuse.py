import os
from dotenv import load_dotenv
from langfuse.callback import CallbackHandler
from langfuse import Langfuse
#from langfuse.model import CreateTrace

load_dotenv()

# Intialize Langfuse Client
from langfuse import Langfuse
langfuse = Langfuse(
    public_key=os.environ["LANGFUSE_PUBLIC_KEY"],
    secret_key=os.environ["LANGFUSE_SECRET_KEY"],
    host=os.environ["LANGFUSE_HOST"],
    #session_id="conversation_chain"
    #id=chat_args.conversation_id,
    #metadata=chat_args.metadata,
)

from uuid import uuid4
trace_id = str(uuid4())
#trace_ini = langfuse.trace()
trace = langfuse.trace(id=trace_id)

