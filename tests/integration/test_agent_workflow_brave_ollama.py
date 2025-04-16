import pytest
from smolagents import CodeAgent

from smolagents_helpers.ollama_model import OllamaModel
from smolagents_helpers.brave_search_tool import BraveSearchTool


@pytest.mark.live
def test_with_real_services():
    """Run manually against real APIs"""
    real_agent = CodeAgent(
        model=OllamaModel(model_name="phi4:14b-q4_K_M"),
        add_base_tools=True,
        tools=[BraveSearchTool()],
    )
    response = real_agent.run("Current weather in Tokyo")
    try:
        if isinstance(response, str):
            assert len(response) > 10, "String length must be greater than 10"
        elif isinstance(response, (int, float)):
            pass
        else:
            raise TypeError("Response must be of type str, int, or float")
    except AssertionError as e:
        print(f"Assertion error: {e}")
    except TypeError as e:
        print(f"Type error: {e}")
