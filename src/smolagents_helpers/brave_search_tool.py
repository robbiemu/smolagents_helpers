import os
import json
import requests
from typing import Dict, List, Any, Optional
from smolagents.tools import Tool

class BraveSearchTool(Tool):
    """Tool for interacting with the Brave Search API within the smolagents framework."""

    name = "brave_search"
    description = "Search the web using Brave Search API. Useful for finding current information on topics, people, or events."
    
    inputs = {
        "query": {
            "type": "string",
            "description": "The search query text"
        },
        "count": {
            "type": "integer",
            "description": "Number of results to return (max 20)",
            "default": 10,
            "nullable": True
        },
        "country": {
            "type": "string",
            "description": "Country code for search results",
            "default": "US",
            "nullable": True
        },
        "search_lang": {
            "type": "string",
            "description": "Language for search results",
            "default": "en",
            "nullable": True
        }
    }
    output_type = "array"
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the Brave Search tool.
        
        Args:
            api_key: Optional API key for Brave Search. If not provided, will try to load from
                     config file or environment variable.
        """
        super().__init__()
        self.api_endpoint = "https://api.search.brave.com/res/v1/web/search"
        self.config_path = os.path.expanduser("~/.config/brave_search_tool.json")
        self.api_key = api_key or self._load_api_key()
        
    def _load_api_key(self) -> Optional[str]:
        """Load API key from config file or environment variable."""
        # First check environment variable
        api_key = os.environ.get("BRAVE_SEARCH_API_KEY")
        if api_key:
            return api_key
            
        # Then check config file
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    return config.get("api_key")
            except (json.JSONDecodeError, IOError):
                pass
                
        return None
        
    def configure(self, api_key: str) -> bool:
        """Configure the tool with a new API key.
        
        Args:
            api_key: The Brave Search API key to save
            
        Returns:
            bool: True if configuration was successful
        """
        self.api_key = api_key
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        
        with open(self.config_path, 'w') as f:
            json.dump({"api_key": api_key}, f)
            
        return True
    
    def forward(self, query: str, count: int = 10, country: str = "US", 
               search_lang: str = "en") -> List[Dict[str, str]]:
        """
        Perform a search using Brave Search API and return formatted results.
        
        Args:
            query: The search query text
            count: Number of results to return (max 20)
            country: Country code for search results
            search_lang: Language for search results
            
        Returns:
            List of formatted search result entries
            
        Raises:
            ValueError: If API key is not configured
            RuntimeError: If the API request fails
        """
        if not self.api_key:
            raise ValueError("API key not configured. Use the configure() method first.")
            
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.api_key
        }
        
        params = {
            "q": query,
            "count": min(count, 20),  # API limit is 20
            "country": country,
            "search_lang": search_lang
        }
        
        try:
            response = requests.get(self.api_endpoint, headers=headers, params=params)
            response.raise_for_status()
            results = response.json()
            return self.format_results(results)
        except requests.RequestException as e:
            raise RuntimeError(f"Search request failed: {str(e)}")
            
    def format_results(self, results: Dict[str, Any]) -> List[Dict[str, str]]:
        """Format raw API results into a more readable structure.
        
        Args:
            results: Raw results from the Brave Search API
            
        Returns:
            List of formatted search result entries
        """
        formatted = []
        
        if "web" not in results or "results" not in results["web"]:
            return []
            
        for item in results["web"]["results"]:
            formatted.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "description": item.get("description", "")
            })
            
        return formatted
