smolagents-helpers
--

Helper tools for integration with the [`smolagents`](https://github.com/robbiemu/smolagents) framework.  
Provides interfaces for APIs (Brave Search) and models (Ollama) in agent projects.

### Available modules (`src/smolagents_helpers/`):
| Module               | Description                                                              |
|----------------------|--------------------------------------------------------------------------|
| `brave_search_tool`  | Integration with the [Brave Search](https://brave.com/search/api/) API. |
| `ollama_model`       | Wrapper class for synchronous/asynchronous interaction with Ollama models. |

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

---

## 📜 License
LGPL-3.0 - See [LICENSE](LICENSE) for details.
