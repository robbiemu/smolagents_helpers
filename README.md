smolagents-helpers
--
[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/robbiemu/smolagents_helpers/blob/master/README.en.md)


Ferramentas auxiliares para integração com a framework [`smolagents`](https://github.com/huggingface/smolagents).  
Fornece interfaces para APIs (Brave Search) e modelos (Ollama) em projetos de agentes.

### Módulos disponíveis (`src/smolagents_helpers/`):
| Módulo               | Descrição                                                                 |
|----------------------|---------------------------------------------------------------------------|
| `brave_search_tool`  | Integração com a API do [Brave Search](https://brave.com/search/api/).   |
| `ollama_model`       | Classe wrapper para interação síncrona/assíncrona com modelos [Ollama](https://github.com/ollama/ollama).    |

---

## 🔧 Instalação

### 1. Para uso em projetos
Adicione como dependência via **pip**:
```bash
pip install git+https://github.com/robbiemu/smolagents_helpers.git
```
Ou com **UV**:
```bash
uv add https://github.com/robbiemu/smolagents_helpers.git
```

### 2. Para desenvolvimento local
Clone e instale em modo editável:
```bash
git clone https://github.com/robbiemu/smolagents_helpers.git
cd smolagents_helpers
pip install -e .[dev]  # Instala dependências de desenvolvimento
```

---

## 🚀 Uso Rápido

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
response = model.generate("Explique RLHF em 1 parágrafo")
```

---

## 📜 Licença
LGPL-3.0 - Consulte [LICENSE](LICENSE) para detalhes.
