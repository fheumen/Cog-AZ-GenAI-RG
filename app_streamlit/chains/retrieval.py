from langchain.chains import ConversationalRetrievalChain
from app_streamlit.chains.traceable import TraceableChain
from app_streamlit.chains.streamable import StreamableChain

class StreamingConversationalRetrievalChain(
    TraceableChain, StreamableChain, ConversationalRetrievalChain
):
    pass