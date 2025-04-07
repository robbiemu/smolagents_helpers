# smolagents-helpers

Este projeto fornece ferramentas auxiliares para integração com a framework smolagents. Atualmente, o repositório contém os seguintes módulos dentro do diretório `src/smolagents_helpers`:

- **brave_search_tool.py**  
  Implementa a classe `BraveSearchTool`, que integra a API de busca do Brave Search. Inclui métodos para carregar e configurar a chave da API e para realizar buscas via HTTP, retornando resultados formatados.

- **ollama_model.py**  
  Contém a classe `OllamaModel`, que facilita a interação com o modelo do Ollama tanto de forma síncrona quanto assíncrona. Possui também a definição da classe `Message`, usada para padronizar as respostas dos modelos.

## Instalação

Certifique-se de ter o Python (>= 3.13) instalado. Instale as dependências utilizando:

```bash
pip install -e .
```

## Uso com UV

Para utilizar este pacote diretamente em um projeto que utiliza o UV, adicione-o como dependência via URL rodando no terminal:

```bash
uv add https://github.com/robbiemu/smolagents_helpers.git
```

Após a adição, você poderá importar os módulos normalmente em seu código:

```python
from smolagents_helpers.ollama_model import OllamaModel
```

Consulte a [documentação do UV](https://github.com/robbiemu/uv) para mais detalhes sobre gerenciamento de dependências e configurações.

## Licença

Este projeto está licenciado sob a LGPL. Para mais detalhes, consulte o arquivo [LICENSE](LICENSE).