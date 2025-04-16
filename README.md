smolagents-helpers
--
[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/robbiemu/smolagents_helpers/blob/master/README.en.md)


Ferramentas auxiliares para integração com a framework [`smolagents`](https://github.com/huggingface/smolagents).  
Fornece interfaces para APIs (Brave Search, data.europa.eu) e modelos (Ollama) em projetos de agentes.

### Módulos disponíveis (`src/smolagents_helpers/`):
| Módulo               | Descrição                                                                 |
|----------------------|---------------------------------------------------------------------------|
| `brave_search_tool`  | Integração com a API do [Brave Search](https://brave.com/search/api/).   |
| `ollama_model`       | Classe wrapper para interação síncrona/assíncrona com modelos [Ollama](https://github.com/ollama/ollama).    |
| `eu_data_tool`       | Auxiliar para aceder e fazer cache de dados do portal [data.europa.eu](https://data.europa.eu/). |

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

### EU Data Tool
```python
from smolagents_helpers.eu_data_tool import EUDataTool

data_tool = EUDataTool() # Cache ativo por defeito em '.eu_data_cache'
search_results = data_tool.search_datasets(keyword="proteção ambiental", limit=3)
print(search_results)

# Exemplo para obter metadados de um dataset específico
# dataset_uri = "URI_DO_DATASET_AQUI" # Substitua pelo URI real
# metadata = data_tool.get_dataset_metadata(dataset_uri)
# print(metadata)
```

---

## 🧪 Testes
Este projeto usa pytest para testes. Alguns testes interagem com serviços externos (Brave API, Ollama, data.europa.eu) e estão marcados como `@pytest.mark.live`.

### Porquê a marca live?

- Estes testes requerem acesso à rede e, potencialmente, chaves de API (como a `BRAVE_API_KEY` definida como variável de ambiente) ou serviços em execução (como uma instância Ollama).
- Podem ser mais lentos e suscetíveis a falhas devido a problemas externos (rede, APIs indisponíveis).
- A marca live permite executar estes testes seletivamente e excluí-los facilmente de pipelines de CI/CD onde podem não ser desejáveis ou viáveis.

### Como executar os testes:

- Executar apenas os testes live (requerem rede/serviços):
  ```bash
  # Certifique-se que tem as dependências de dev instaladas: pip install -e .[dev]
  # Defina a BRAVE_API_KEY se necessário
  uv run pytest -m live -v
  ```
  _(O `-v` é para saída verbosa, opcional)_
- Executar todos os testes (incluindo os live):
  ```bash
  uv run pytest -v
  ```
- Executar todos os testes exceto os live (útil para CI):
  ```bash
  uv run pytest -m "not live" -v
  ```

---

## 📜 Licença
LGPL-3.0 - Consulte [LICENSE](LICENSE) para detalhes.
