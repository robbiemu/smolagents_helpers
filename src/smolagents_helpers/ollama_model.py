from dataclasses import dataclass
import ollama
import asyncio
from typing import List, Dict, Any, Union

@dataclass
class Message:
    content: str  # Atributo obrigatório para smolagents

class OllamaModel:
    def __init__(self, model_name: str) -> None:
        self.model_name = model_name
        self.client = ollama.Client()
    
    def __call__(self, messages: List[Union[str, Dict[str, Any]]], **kwargs: Any) -> Message:
        """Método de chamada síncrona"""
        formatted_messages = self._format_messages(messages)
        response: Dict[str, Any] = self.client.chat(
            model=self.model_name,
            messages=formatted_messages,
            options={'temperature': 0.7, 'stream': False}
        )
        return Message(
            content=response.get("message", {}).get("content", "")
        )
    
    async def acall(self, messages: List[Union[str, Dict[str, Any]]], **kwargs: Any) -> Message:
        """Método de chamada assíncrona"""
        formatted_messages = self._format_messages(messages)
        loop = asyncio.get_event_loop()
        response: Dict[str, Any] = await loop.run_in_executor(
            None,
            lambda: self.client.chat(
                model=self.model_name,
                messages=formatted_messages,
                options={'temperature': 0.7, 'stream': False}
            )
        )
        return Message(
            content=response.get("message", {}).get("content", "")
        )
    
    def _format_messages(self, messages: List[Union[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
        formatted_messages: List[Dict[str, Any]] = []
        for msg in messages:
            if isinstance(msg, str):
                formatted_messages.append({
                    "role": "user",  # Padrão para 'user' em strings simples
                    "content": msg
                })
            elif isinstance(msg, dict):
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if isinstance(content, list):
                    content = " ".join(part.get("text", "") for part in content if isinstance(part, dict) and "text" in part)
                formatted_messages.append({
                    "role": role if role in ['user', 'assistant', 'system', 'tool'] else 'user',
                    "content": content
                })
            else:
                formatted_messages.append({
                    "role": "user",  # Papel padrão para tipos inesperados
                    "content": str(msg)
                })
        return formatted_messages
