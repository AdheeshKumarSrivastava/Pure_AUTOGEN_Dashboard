import os
from typing import Optional,Type
from pydantic import BaseModel
from autogen_ext.models.ollama import OllamaChatCompletionClient

def make_model_client(response_format:Optional[Type[BaseModel]]=None):
    model = os.getenv("OLLAMA_MODEL","qwen2.5:7b")
    if response_format is not None:
        return OllamaChatCompletionClient(model=model,response_format=response_format)
    return OllamaChatCompletionClient(model=model)
