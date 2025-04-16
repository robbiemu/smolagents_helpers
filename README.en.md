smolagents-helpers
--

Helper tools for integration with the [`smolagents`](https://github.com/huggingface/smolagents) framework.  
Provides interfaces for APIs (Brave Search, data.europa.eu) and models (Ollama) in agent projects.

### Available modules (`src/smolagents_helpers/`):
| Module               | Description                                                              |
|----------------------|--------------------------------------------------------------------------|
| `brave_search_tool`  | Integration with the [Brave Search](https://brave.com/search/api/) API. |
| `ollama_model`       | Wrapper class for synchronous/asynchronous interaction with [Ollama](https://github.com/ollama/ollama) models. |
| `eu_data_tool`       | Helper for accessing and caching data from the [data.europa.eu](https://data.europa.eu/) portal. |

---

## 🔧 Installation

### 1. For project use
Add as a dependency via **pip**:
```bash
pip install git+https://github.com/robbiemu/smolagents_helpers.git
```
Or with **UV**:
```bash
uv add https://github.com/robbiemu/smolagents_helpers.git
```

### 2. For local development
Clone and install in editable mode:
```bash
git clone https://github.com/robbiemu/smolagents_helpers.git
cd smolagents_helpers
pip install -e .[dev]  # Installs development dependencies
```

---

## 🚀 Quick Start

### Brave Search
```python
from smolagents_helpers.brave_search_tool import BraveSearchTool
import os

brave_api_key = os.getenv("BRAVE_API_KEY")
search_tool = BraveSearchTool(api_key=brave_api_key)

results = search_tool.query("Python smolagents", count=5)
```

### Ollama
```python
from smolagents_helpers.ollama_model import OllamaModel

model = OllamaModel(model_name="llama3")
response = model.generate("Explain RLHF in 1 paragraph")
```

### EU Data Tool
```python
from smolagents_helpers.eu_data_tool import EUDataTool

data_tool = EUDataTool() # Cache enabled by default in '.eu_data_cache'
search_results = data_tool.search_datasets(keyword="environmental protection", limit=3)
print(search_results)

# Example for getting metadata of a specific dataset
# dataset_uri = "YOUR_DATASET_URI_HERE" # Replace with actual URI
# metadata = data_tool.get_dataset_metadata(dataset_uri)
# print(metadata)
```

---

## 🧪 Testing
This project uses pytest for testing. Some tests interact with external services (Brave API, Ollama, data.europa.eu) and are marked with `@pytest.mark.live`.

### Why the live marker?

These tests require network access and potentially API keys (like `BRAVE_API_KEY` set as an environment variable) or running services (like an Ollama instance).
They can be slower and prone to failures due to external factors (network issues, API outages).
The live marker allows running these tests selectively and easily excluding them from CI/CD pipelines where they might be undesirable or unfeasible.

### How to run tests:

- Run only the live tests (require network/services):
  ```bash
  # Ensure you have dev dependencies installed: pip install -e .[dev]
  # Set BRAVE_API_KEY if needed
  uv run pytest -m live -v
  ```
  (The `-v` is for verbose output, optional)
- Run all tests (including live):
  ```bash
  uv run pytest -v
  ```
- Run all tests except live (useful for CI):
  ```bash
  uv run pytest -m "not live" -v
  ```

---

## 📜 License
LGPL-3.0 - See [LICENSE](LICENSE) for details.
