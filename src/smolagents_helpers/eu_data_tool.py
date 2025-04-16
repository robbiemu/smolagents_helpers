import os
import hashlib
import json
import logging
import requests
import re  
import time
from typing import List, Dict, Optional, Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class EUDataTool:
    """
    An enhanced helper class to query High-Value Datasets from data.europa.eu.
    Prioritizes the REST API for metadata retrieval and falls back to SPARQL.
    Includes search, caching, and metadata retrieval capabilities.
    """

    SPARQL_ENDPOINT = "https://data.europa.eu/sparql"
    REST_API_BASE = "https://data.europa.eu/api/hub/repo/datasets/"
    DEFAULT_CACHE_TTL = 86400  
    REQUEST_TIMEOUT = 120
    REQUEST_DELAY = 0.5  # Seconds between requests to avoid rate limiting

    def __init__(
        self,
        user_agent: Optional[str] = None,
        cache_enabled: bool = True,
        cache_dir: str = ".eu_data_cache",
        cache_ttl: int = DEFAULT_CACHE_TTL,
        preferred_formats: Optional[List[str]] = None,
    ):
        # Headers for general REST API calls (preferring JSON-LD)
        self.headers = {
            "Accept": "application/ld+json, application/json, */*",
            "User-Agent": user_agent
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        }
        # Specific headers for SPARQL endpoint
        self.sparql_headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": self.headers["User-Agent"],  # Use same UA
        }
        self.cache_enabled = cache_enabled
        self.cache_dir = cache_dir
        self.cache_ttl = cache_ttl
        self.preferred_formats = preferred_formats or ["CSV", "JSON", "XML", "RDF"]
        self._last_request_time = 0
        if self.cache_enabled:
            os.makedirs(self.cache_dir, exist_ok=True)
            logging.info(
                f"EUDataTool cache enabled at: {os.path.abspath(self.cache_dir)}"
            )

    def _ensure_request_delay(self):
        """Ensures a minimum delay between consecutive requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self._last_request_time = time.time()

    def _sanitize_filename(self, uri_or_key: str) -> str:
        """Sanitize a string (URI or key) to create a safe filename component."""
        sanitized = (
            uri_or_key.replace("http://", "")
            .replace("https://", "")
            .replace("/", "_")
            .replace(":", "_")
            .replace("?", "_")
            .replace("&", "_")
            .replace("=", "_")
        )
        sanitized = "".join(c for c in sanitized if c.isalnum() or c in ("_", "-", "."))
        # Limit length to avoid issues with long filenames
        return sanitized[:150]

    def _execute_sparql_query(
        self, query: str, force_refresh: bool = False, cache_key_suffix: str = "query"
    ) -> Dict[str, Any]:
        """
        Executes a SPARQL query, handling caching and potential errors.
        Used for search and as a fallback for metadata.
        """
        # Generate cache key based on query hash
        query_hash = hashlib.md5(query.encode("utf-8")).hexdigest()
        # Add prefix to distinguish SPARQL cache files
        cache_path = os.path.join(
            self.cache_dir, f"sparql_{query_hash}_{cache_key_suffix}.json"
        )

        # Check cache first
        if self.cache_enabled and not force_refresh and os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                cache_time = cached_data.get("_cache_timestamp", 0)
                if time.time() - cache_time < self.cache_ttl:
                    logging.debug(
                        f"SPARQL Cache hit for query hash: {query_hash} ({cache_key_suffix})"
                    )
                    # Return without internal cache fields
                    return {
                        k: v for k, v in cached_data.items() if not k.startswith("_")
                    }
                else:
                    logging.debug(
                        f"SPARQL Cache expired for query hash: {query_hash} ({cache_key_suffix})"
                    )
            except (json.JSONDecodeError, IOError, UnicodeDecodeError) as e:
                logging.warning(
                    f"SPARQL Cache read error for {cache_path}: {e}. Fetching fresh data."
                )
                try:
                    os.remove(cache_path)  # Remove corrupted cache file
                except OSError:
                    pass  # Ignore if file cannot be removed

        # --- Fetch fresh data ---
        self._ensure_request_delay()
        logging.debug(f"Executing SPARQL query ({cache_key_suffix}):\n{query}")
        try:
            response = requests.get(
                self.SPARQL_ENDPOINT,
                params={"query": query},
                headers=self.sparql_headers,  # Use SPARQL specific headers
                timeout=self.REQUEST_TIMEOUT,
            )
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            results = response.json()

            # Cache the results
            if self.cache_enabled:
                results_to_cache = (
                    results.copy()
                )  # Avoid modifying original results dict
                results_to_cache["_cache_timestamp"] = time.time()
                try:
                    temp_file = cache_path + ".tmp"
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(results_to_cache, f, ensure_ascii=False)
                    os.replace(temp_file, cache_path)
                    logging.debug(
                        f"SPARQL Cached results for query hash: {query_hash} ({cache_key_suffix})"
                    )
                except IOError as e:
                    logging.error(
                        f"Failed to write SPARQL cache file {cache_path}: {e}"
                    )

            # Return full results (internal methods might need the structure)
            # Callers that need cleaned data should strip "_cache_timestamp"
            return results

        except requests.exceptions.RequestException as e:
            error_msg = f"SPARQL query failed ({cache_key_suffix}): {str(e)}"
            if e.response is not None:
                error_msg += f" - Status Code: {e.response.status_code}"
                try:
                    # Try to get more detailed error from response body if available
                    error_detail = e.response.text
                    error_msg += (
                        f" - Response: {error_detail[:500]}"  # Limit response length
                    )
                except Exception:
                    pass  # Ignore if response body cannot be read
            logging.error(error_msg)
            # Return consistent error structure with empty bindings
            return {"error": error_msg, "results": {"bindings": []}}
        except json.JSONDecodeError as e:
            error_msg = (
                f"Failed to decode SPARQL JSON response ({cache_key_suffix}): {e}"
            )
            logging.error(error_msg)
            # Return consistent error structure
            return {"error": error_msg, "results": {"bindings": []}}

    def search_datasets(
        self,
        keyword: Optional[str] = None,
        topic: Optional[str] = None,  # Note: DCAT theme URIs are better for filtering
        publisher: Optional[str] = None,
        date_from: Optional[str] = None,  # YYYY-MM-DD
        date_to: Optional[str] = None,  # YYYY-MM-DD
        language: Optional[str] = None,  # ISO 639-1 code (e.g., 'en')
        sort_by: str = "date",  # Note: in SPARQL 'date' or 'title' are supported.
        sort_order: str = "desc",  # SPARQL ORDER BY applies to query variables
        preferred_formats: Optional[List[str]] = None,  # Used post-query
        limit: int = 10,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Search for datasets using SPARQL, performs initial search then enhances with distribution details.
        NOTE: Keyword search *only* matches exact dcat:keyword values (case-insensitive) for reliability.
              SPARQL doesn't have built-in 'relevance' sorting. Sorting by 'date' or 'title' is possible.
        """
        if preferred_formats is None:
            preferred_formats = self.preferred_formats  # Use instance default

        # --- 1. Construct the initial SPARQL query for datasets ---
        select_vars = "?dataset ?title (GROUP_CONCAT(DISTINCT ?kw; SEPARATOR='|') AS ?keywords) (SAMPLE(?pubName) AS ?publisher) (MAX(?mod) AS ?modified)"
        where_clauses = ["?dataset a dcat:Dataset ."]
        where_clauses.append(
            "OPTIONAL { ?dataset dct:title ?title . FILTER(LANGMATCHES(LANG(?title), 'en') || LANG(?title) = '') }"
        )
        where_clauses.append(
            "OPTIONAL { ?dataset dct:publisher ?pubURI . ?pubURI foaf:name ?pubName . }"
        )
        where_clauses.append("OPTIONAL { ?dataset dct:modified ?mod . }")
        where_clauses.append("OPTIONAL { ?dataset dct:issued ?iss . }")

        # --- Keyword Filtering Logic ---
        if keyword:
            where_clauses.append(
                "?dataset dcat:keyword ?kw ."
            )  # Require keyword triple
            keyword_pattern = (
                keyword.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace('"', '\\"')
                .replace("^", "\\^")
                .replace("$", "\\$")
            )
            where_clauses.append(f'FILTER REGEX(STR(?kw), "{keyword_pattern}", "i")')
        else:
            where_clauses.append(
                "OPTIONAL { ?dataset dcat:keyword ?kw . }"
            )  # Optional for display

        # --- Other Filters ---
        if publisher:
            publisher_pattern = (
                publisher.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
            )
            where_clauses.append(
                f"""FILTER EXISTS {{
                    ?dataset dct:publisher ?pubCheckURI .
                    ?pubCheckURI foaf:name ?pubCheckName .
                    FILTER REGEX(STR(?pubCheckName), "{publisher_pattern}", "i")
                }}"""
            )
        if topic:
            # Assume topic is a full URI or a fragment thereof
            # Note: This requires the dataset to have *this specific* theme URI.
            # Matching labels would require more complex queries.
            # Escape < and > if they are part of the topic string itself, otherwise assume it's a URI
            if not topic.startswith("<") and not topic.startswith("http"):
                # Simple keyword match on theme URI string if not a full URI
                topic_pattern = (
                    topic.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
                )
                where_clauses.append(
                    f"FILTER EXISTS {{ ?dataset dcat:theme ?themeUri . FILTER CONTAINS(STR(?themeUri), '{topic_pattern}') }}"
                )
            else:
                # Assume it's a URI, ensure it's enclosed in <> if needed
                topic_uri = topic if topic.startswith("<") else f"<{topic}>"
                where_clauses.append(f"?dataset dcat:theme {topic_uri} .")

        if language:
            where_clauses.append(
                f"?dataset dct:language <http://publications.europa.eu/resource/authority/language/{language.upper()}> ."
            )
        if date_from:
            where_clauses.append(f"FILTER(BOUND(?iss) || BOUND(?mod))")
            where_clauses.append(
                f"FILTER ((BOUND(?iss) && ?iss >= '{date_from}'^^xsd:date) || (BOUND(?mod) && ?mod >= '{date_from}'^^xsd:date))"
            )
        if date_to:
            where_clauses.append(f"FILTER(BOUND(?iss) || BOUND(?mod))")
            where_clauses.append(
                f"FILTER ((BOUND(?iss) && ?iss <= '{date_to}'^^xsd:date) || (BOUND(?mod) && ?mod <= '{date_to}'^^xsd:date))"
            )

        # --- Ordering ---
        order_clause = ""
        if sort_by == "date":
            sort_dir = "DESC" if sort_order == "desc" else "ASC"
            order_clause = (
                f"ORDER BY {sort_dir}(MAX(?mod))"  # Order by aggregated modified date
            )
        elif sort_by == "title":
            sort_dir = "DESC" if sort_order == "desc" else "ASC"
            order_clause = f"ORDER BY {sort_dir}(SAMPLE(?title))"  # Need SAMPLE or similar inside GROUP BY context
        # Relevance sorting is not directly supported

        query_body = "\n".join(where_clauses)

        initial_query = f"""
        PREFIX dcat: <http://www.w3.org/ns/dcat#>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT {select_vars}
        WHERE {{
            {query_body}
        }}
        GROUP BY ?dataset ?title
        {order_clause}
        LIMIT {limit}
        OFFSET {offset}
        """

        # --- 2. Execute the initial search query ---
        search_response = self._execute_sparql_query(
            initial_query, cache_key_suffix="search"
        )

        if "error" in search_response:
            # Propagate error, ensure 'results' key exists for compatibility
            return {
                "error": search_response["error"],
                "results": [],
                "total_results": 0,
            }  # No total_results available easily

        initial_results = search_response.get("results", {}).get("bindings", [])
        if not initial_results:
            return {"total_results": 0, "results": []}  # No results found

        # --- 3. Enhance results with distribution details ---
        enhanced_results = []
        dataset_uris = [
            res.get("dataset", {}).get("value")
            for res in initial_results
            if res.get("dataset")
        ]

        # Fetch distributions for all datasets found in the initial search
        distributions_by_dataset = {}
        if dataset_uris:
            # Chunk dataset URIs if the list is very long to avoid overly long SPARQL queries
            chunk_size = 50  # Adjust as needed
            for i in range(0, len(dataset_uris), chunk_size):
                chunk_uris = dataset_uris[i : i + chunk_size]
                uri_values = " ".join(f"<{uri}>" for uri in chunk_uris if uri)
                if not uri_values:
                    continue

                dist_query = f"""
                PREFIX dcat: <http://www.w3.org/ns/dcat#>
                PREFIX dct: <http://purl.org/dc/terms/>

                SELECT ?dataset ?dist ?format ?downloadURL ?accessURL ?mediaType ?byteSize WHERE {{
                  VALUES ?dataset {{ {uri_values} }} # Filter for datasets in this chunk
                  ?dataset dcat:distribution ?dist .
                  OPTIONAL {{ ?dist dct:format ?formatURI . BIND(STR(?formatURI) AS ?format) }}
                  OPTIONAL {{ ?dist dcat:downloadURL ?downloadURL . }}
                  OPTIONAL {{ ?dist dcat:accessURL ?accessURL . }}
                  OPTIONAL {{ ?dist dcat:mediaType ?mediaType . }}
                  OPTIONAL {{ ?dist dcat:byteSize ?byteSize . }}
                }} LIMIT 1000
                """
                # Use URI hash in cache key to distinguish chunks if needed, though results are combined later
                dist_response = self._execute_sparql_query(
                    dist_query, cache_key_suffix=f"dists_chunk_{i//chunk_size}"
                )
                dist_results = dist_response.get("results", {}).get("bindings", [])

                # Organize distributions by dataset URI
                for res in dist_results:
                    ds_uri = res.get("dataset", {}).get("value")
                    dist_uri = res.get("dist", {}).get(
                        "value"
                    )  # Get dist URI to avoid duplicates
                    if not ds_uri or not dist_uri:
                        continue

                    dist = {
                        "uri": dist_uri,  # Include distribution URI
                        "format": res.get("format", {}).get("value"),
                        "mediaType": res.get("mediaType", {}).get("value"),
                        "downloadURL": res.get("downloadURL", {}).get("value"),
                        "accessURL": res.get("accessURL", {}).get("value"),
                        "byteSize": res.get("byteSize", {}).get("value"),
                    }
                    dist = {k: v for k, v in dist.items() if v}  # Remove empty keys

                    if dist:
                        if ds_uri not in distributions_by_dataset:
                            distributions_by_dataset[
                                ds_uri
                            ] = {}  # Use dict keyed by dist URI
                        # Store distribution keyed by its URI to prevent duplicates
                        if dist_uri not in distributions_by_dataset[ds_uri]:
                            distributions_by_dataset[ds_uri][dist_uri] = dist

        # --- 4. Combine initial results with distributions ---
        for basic_result in initial_results:
            dataset_uri = basic_result.get("dataset", {}).get("value")
            if not dataset_uri:
                continue

            # Get distributions as a list from the dictionary structure
            dataset_distributions_dict = distributions_by_dataset.get(dataset_uri, {})
            distributions = list(dataset_distributions_dict.values())

            # Find best download option based on preferred formats
            best_download = None
            norm_preferred = [f.upper() for f in preferred_formats]

            for fmt_pref in norm_preferred:
                if best_download:
                    break
                for dist in distributions:
                    # Prioritize downloadURL, but consider accessURL if downloadURL is absent
                    dist_url = dist.get("downloadURL") or dist.get("accessURL")
                    if not dist_url:
                        continue

                    format_value = (dist.get("format") or "").upper()
                    media_type = (dist.get("mediaType") or "").upper()
                    url_lower = dist_url.lower()

                    # Use the same matching logic as in get_dataset_content
                    matches_format = (
                        (format_value and fmt_pref in format_value)
                        or (
                            media_type
                            and any(
                                f"/{fmt_pref}" in mt_part
                                for mt_part in media_type.split("/")
                            )
                        )
                        or any(
                            ext in url_lower.split("?")[0].split("/")[-1]
                            for ext in [
                                f".{fmt_pref.lower()}",
                                f"format={fmt_pref.lower()}",
                            ]
                        )
                        or (
                            fmt_pref == "RDF"
                            and any(
                                rdf_fmt in format_value
                                for rdf_fmt in ["RDF", "XML", "TURTLE", "N3", "JSON-LD"]
                            )
                        )
                        or (fmt_pref == "XML" and "XML" in media_type)
                        or (fmt_pref == "JSON" and "JSON" in media_type)
                        or (
                            fmt_pref == "CSV"
                            and ("CSV" in media_type or "csv" in format_value)
                        )
                    )

                    if matches_format:
                        best_download = {
                            "url": dist_url,
                            "format": dist.get("format") or fmt_pref,  # Best guess
                            "mediaType": dist.get("mediaType"),
                            "byteSize": dist.get("byteSize"),
                        }
                        break

            # Fallback: If no preferred format found, take the first distribution with a download/access URL
            if not best_download:
                for dist in distributions:
                    dist_url = dist.get("downloadURL") or dist.get("accessURL")
                    if dist_url:
                        best_download = {
                            "url": dist_url,
                            "format": dist.get("format"),
                            "mediaType": dist.get("mediaType"),
                            "byteSize": dist.get("byteSize"),
                        }
                        break

            enhanced_dataset = {
                "uri": dataset_uri,
                "title": basic_result.get("title", {}).get("value"),
                "publisher": basic_result.get("publisher", {}).get("value"),
                "keywords": [
                    kw
                    for kw in basic_result.get("keywords", {})
                    .get("value", "")
                    .split("|")
                    if kw
                ],  # Clean empty strings
                "modified": basic_result.get("modified", {}).get("value"),
                "distributions": distributions,
                "download": best_download,  # May be None
            }
            enhanced_results.append(enhanced_dataset)

        # Note: Total results count from SPARQL with GROUP BY and LIMIT/OFFSET is tricky.
        # A separate COUNT query would be needed, potentially expensive.
        # Returning only the results found in the current page.
        return {"results": enhanced_results}

    # --- Metadata Retrieval Logic (REST first, SPARQL fallback) ---

    def get_dataset_metadata(
        self, dataset_uri: str, force_refresh: bool = False, locale: str = "en"
    ) -> Dict[str, Any]:
        """
        Retrieve detailed metadata for a specific dataset URI.

        Attempts to use the REST API (JSON-LD) first for comprehensive data.
        Falls back to a limited multi-query SPARQL approach if the API fails or UUID extraction fails.

        Args:
            dataset_uri: The URI of the dataset (e.g., from data.europa.eu website or SPARQL search).
            force_refresh: If True, bypass cache and fetch fresh data.
            locale: Preferred language for metadata (used in API call).

        Returns:
            Dictionary containing dataset metadata or an error dictionary.
        """
        # --- Caching Logic (applies to the final result of this function) ---
        cache_key_base = self._sanitize_filename(f"metadata_{dataset_uri}")
        cache_key_rest = f"{cache_key_base}_rest_{locale}"
        cache_key_sparql = f"{cache_key_base}_sparql"  # Fallback uses different key
        final_metadata = None
        cache_to_use = None  # Will be set based on successful method

        # --- Attempt 1: REST API (JSON-LD) ---
        # Check REST cache first
        rest_cache_path = os.path.join(self.cache_dir, cache_key_rest + ".json")
        if self.cache_enabled and not force_refresh and os.path.exists(rest_cache_path):
            try:
                with open(rest_cache_path, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                cache_time = cached_data.get("_cache_timestamp", 0)
                if time.time() - cache_time < self.cache_ttl:
                    logging.info(
                        f"Metadata cache hit (REST strategy) for: {dataset_uri}"
                    )
                    return {
                        k: v for k, v in cached_data.items() if not k.startswith("_")
                    }
                else:
                    logging.debug(
                        f"Metadata cache expired (REST strategy) for: {dataset_uri}"
                    )
            except (json.JSONDecodeError, IOError, UnicodeDecodeError) as e:
                logging.warning(
                    f"Metadata cache read error for {rest_cache_path}: {e}. Attempting fetch."
                )
                try:
                    os.remove(rest_cache_path)
                except OSError:
                    pass

        # Try fetching from REST API
        metadata_from_rest = self._get_metadata_from_rest_api(dataset_uri, locale)

        if metadata_from_rest and "error" not in metadata_from_rest:
            logging.info(
                f"Successfully retrieved metadata via REST API for {dataset_uri}"
            )
            final_metadata = metadata_from_rest
            cache_to_use = rest_cache_path  # Use the REST cache path
        else:
            rest_error_msg = (
                metadata_from_rest.get("error", "Unknown REST error")
                if metadata_from_rest
                else "REST method returned None"
            )
            logging.warning(
                f"REST API method failed for {dataset_uri}. Error: {rest_error_msg}. Falling back to SPARQL."
            )

            # --- Attempt 2: SPARQL Fallback ---
            # Check SPARQL fallback cache
            sparql_cache_path = os.path.join(self.cache_dir, cache_key_sparql + ".json")
            if (
                self.cache_enabled
                and not force_refresh
                and os.path.exists(sparql_cache_path)
            ):
                try:
                    with open(sparql_cache_path, "r", encoding="utf-8") as f:
                        cached_data = json.load(f)
                    cache_time = cached_data.get("_cache_timestamp", 0)
                    if time.time() - cache_time < self.cache_ttl:
                        logging.info(
                            f"Metadata cache hit (SPARQL strategy) for: {dataset_uri}"
                        )
                        return {
                            k: v
                            for k, v in cached_data.items()
                            if not k.startswith("_")
                        }
                    else:
                        logging.debug(
                            f"Metadata cache expired (SPARQL strategy) for: {dataset_uri}"
                        )
                except (json.JSONDecodeError, IOError, UnicodeDecodeError) as e:
                    logging.warning(
                        f"SPARQL fallback cache read error for {sparql_cache_path}: {e}. Fetching fresh."
                    )
                    try:
                        os.remove(sparql_cache_path)
                    except OSError:
                        pass

            # Fetch using SPARQL fallback
            metadata_from_sparql = self._get_metadata_from_sparql_fallback(
                dataset_uri, force_refresh
            )  # Pass force_refresh for SPARQL sub-queries

            if metadata_from_sparql and "error" not in metadata_from_sparql:
                logging.info(
                    f"Successfully retrieved limited metadata via SPARQL fallback for {dataset_uri}"
                )
                final_metadata = metadata_from_sparql
                cache_to_use = sparql_cache_path  # Use the SPARQL cache path
            else:
                sparql_error_msg = (
                    metadata_from_sparql.get("error", "Unknown SPARQL error")
                    if metadata_from_sparql
                    else "SPARQL method returned None"
                )
                logging.error(
                    f"SPARQL fallback method also failed for {dataset_uri}. Error: {sparql_error_msg}"
                )
                # Return a consolidated error
                return {
                    "error": f"Failed to retrieve metadata. REST Error: {rest_error_msg}. SPARQL Error: {sparql_error_msg}"
                }

        # --- Cache the final successful result ---
        if final_metadata and cache_to_use and self.cache_enabled:
            metadata_to_cache = final_metadata.copy()
            metadata_to_cache["_cache_timestamp"] = time.time()
            try:
                temp_file = cache_to_use + ".tmp"
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(metadata_to_cache, f, ensure_ascii=False)
                os.replace(temp_file, cache_to_use)
                logging.debug(
                    f"Cached final metadata ({'REST' if cache_to_use == rest_cache_path else 'SPARQL'}) for: {dataset_uri}"
                )
            except IOError as e:
                logging.error(
                    f"Failed to write final metadata cache {cache_to_use}: {e}"
                )

        # Return metadata without internal cache field
        if final_metadata:
            return {k: v for k, v in final_metadata.items() if not k.startswith("_")}
        else:
            # Should have been caught earlier, but as a failsafe
            return {"error": "Metadata retrieval failed through all methods."}

    def _extract_uuid_from_uri(self, dataset_uri: str) -> Optional[str]:
        """Extracts the UUID from known data.europa.eu URI patterns."""
        # Pattern for /data/datasets/{UUID} or /88u/dataset/{UUID} or /set/{UUID}
        # Example: http://data.europa.eu/88u/dataset/somerandom-uuid-goes-here
        # Example: https://data.europa.eu/data/datasets/somerandom-uuid-goes-here
        # Example: https://data.europa.eu/set/data/some-name-with-uuid-goes-here
        match = re.search(
            r"/(?:datasets|dataset|set/data)/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
            dataset_uri,
        )
        if match:
            return match.group(1)
        # Check for pattern like /set/{UUID}/resource/...
        match_set = re.search(
            r"/set/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(/|$)",
            dataset_uri,
        )
        if match_set:
            return match_set.group(1)

        logging.warning(f"Could not extract UUID from URI pattern: {dataset_uri}")
        return None

    def _get_value(
        self,
        node: Optional[Dict],
        property_uri: str,
        prefer_locale: Optional[str] = "en",
        allow_list: bool = False,
    ) -> Any:
        """
        Helper to extract value(s) from a JSON-LD node, handling language tags and lists.
        Returns a list if allow_list is True, otherwise a single value or None.
        """
        if node is None:
            return [] if allow_list else None

        value = node.get(property_uri)
        if value is None:
            return [] if allow_list else None

        results_list = []
        preferred_result = None
        any_result = None  # Fallback if preferred locale not found or not applicable

        values_to_process = value if isinstance(value, list) else [value]

        for item in values_to_process:
            item_value = None
            item_lang = None

            if isinstance(item, dict):
                item_value = item.get("@value")  # Literal value
                item_lang = item.get("@language")
                if item_value is None:
                    item_value = item.get("@id")  # URI reference
            elif isinstance(item, str):  # Plain string (could be literal or URI)
                item_value = item

            if item_value is not None:
                results_list.append(item_value)
                if prefer_locale and item_lang == prefer_locale:
                    if preferred_result is None:  # Take the first preferred match
                        preferred_result = item_value
                if any_result is None:  # Take the first value found as a fallback
                    any_result = item_value

        if allow_list:
            return results_list  # Return all collected values
        else:
            # Return preferred language match, or the first value found, or None
            return preferred_result or any_result

    def _get_metadata_from_rest_api(
        self, dataset_uri: str, locale: str
    ) -> Optional[Dict[str, Any]]:
        """Internal method to fetch and parse metadata using the REST API (JSON-LD)."""
        dataset_uuid = self._extract_uuid_from_uri(dataset_uri)
        if not dataset_uuid:
            # Cannot use REST API without UUID
            return {
                "error": f"Could not extract UUID from URI for REST API call: {dataset_uri}"
            }

        # Construct API URL
        # useNormalizedId=true helps ensure consistent IDs, locale helps get translated fields
        api_url = f"{self.REST_API_BASE}{dataset_uuid}.jsonld?useNormalizedId=true&locale={locale}"
        logging.info(f"Attempting REST API fetch: {api_url}")

        self._ensure_request_delay()
        try:
            response = requests.get(
                api_url, 
                headers=self.headers, 
                timeout=self.REQUEST_TIMEOUT 
            )
            response.raise_for_status()
            json_ld_data = response.json()

            # --- Parse JSON-LD Graph ---
            graph = json_ld_data.get("@graph")
            if not graph or not isinstance(graph, list):
                return {
                    "error": f"JSON-LD response missing or invalid '@graph' array. URL: {api_url}"
                }

            # Find the main dataset node and distribution nodes
            dataset_node = None
            distribution_nodes = []
            nodes_by_id = {
                node.get("@id"): node
                for node in graph
                if node.get("@id") and isinstance(node, dict)
            }

            # Try to find the dataset node matching the input URI or containing the UUID
            for node in graph:
                if not isinstance(node, dict):
                    continue
                node_type = node.get("@type", [])
                node_id = node.get("@id", "")
                # Check type and if ID matches original URI or contains the UUID
                if "dcat:Dataset" in node_type and (
                    node_id == dataset_uri or dataset_uuid in node_id
                ):
                    dataset_node = node
                    break  # Found likely primary dataset node

            # Fallback: If no match, assume the first dcat:Dataset is the right one
            if not dataset_node:
                for node in graph:
                    if isinstance(node, dict) and "dcat:Dataset" in node.get(
                        "@type", []
                    ):
                        dataset_node = node
                        logging.warning(
                            f"Could not directly match URI/UUID in dataset node ID, using first dcat:Dataset found: {dataset_node.get('@id')}"
                        )
                        break
                if not dataset_node:
                    return {
                        "error": f"Could not find dcat:Dataset node in @graph for UUID {dataset_uuid}. URL: {api_url}"
                    }

            # Find all distribution nodes associated with the dataset node
            dist_uris = self._get_value(
                dataset_node, "dcat:distribution", allow_list=True
            )
            for dist_uri in dist_uris:
                if isinstance(dist_uri, str) and dist_uri in nodes_by_id:
                    dist_node = nodes_by_id[dist_uri]
                    if isinstance(
                        dist_node, dict
                    ) and "dcat:Distribution" in dist_node.get("@type", []):
                        distribution_nodes.append(dist_node)
                # Handle distributions embedded directly (less common but possible)
                elif isinstance(dist_uri, dict) and "dcat:Distribution" in dist_uri.get(
                    "@type", []
                ):
                    distribution_nodes.append(dist_uri)

            # --- Extract Metadata ---
            metadata = {
                "uri": dataset_node.get("@id") or dataset_uri
            }  # Prefer ID from graph

            metadata["title"] = self._get_value(
                dataset_node, "dct:title", prefer_locale=locale
            )
            metadata["description"] = self._get_value(
                dataset_node, "dct:description", prefer_locale=locale
            )
            metadata["modified"] = self._get_value(dataset_node, "dct:modified")
            metadata["issued"] = self._get_value(
                dataset_node, "dct:issued"
            ) or self._get_value(dataset_node, "dct:created")

            # Publisher (might be URI needing lookup or direct object/string)
            publisher_ref = self._get_value(dataset_node, "dct:publisher")
            if isinstance(publisher_ref, str) and publisher_ref.startswith(
                "http"
            ):  # URI
                publisher_node = nodes_by_id.get(publisher_ref)
                if publisher_node:
                    # Try common properties for name
                    metadata["publisher"] = (
                        self._get_value(
                            publisher_node, "foaf:name", prefer_locale=locale
                        )
                        or self._get_value(
                            publisher_node, "skos:prefLabel", prefer_locale=locale
                        )
                        or self._get_value(
                            publisher_node, "rdfs:label", prefer_locale=locale
                        )
                    )
                else:
                    metadata["publisher"] = None  # Node not found
                    metadata[
                        "publisher_uri"
                    ] = publisher_ref  # Store URI if name unknown
            elif isinstance(publisher_ref, str):  # Simple string name
                metadata["publisher"] = publisher_ref
            elif isinstance(publisher_ref, dict):  # Embedded publisher object
                metadata["publisher"] = self._get_value(
                    publisher_ref, "foaf:name", prefer_locale=locale
                )  # etc.
                metadata["publisher_uri"] = self._get_value(publisher_ref, "@id")
            else:
                metadata["publisher"] = None

            # Multi-valued properties (get all as list)
            metadata["keywords"] = self._get_value(
                dataset_node, "dcat:keyword", prefer_locale=locale, allow_list=True
            )
            metadata["themes"] = self._get_value(
                dataset_node, "dcat:theme", allow_list=True
            )  # URIs
            metadata["languages"] = self._get_value(
                dataset_node, "dct:language", allow_list=True
            )  # URIs
            metadata["licenses"] = self._get_value(
                dataset_node, "dct:license", allow_list=True
            )  # URIs or embedded objects

            # Distributions
            metadata["distributions"] = []
            processed_dist_ids = set()  # Avoid duplicates if graph has redundancy

            for dist_node in distribution_nodes:
                dist_id = dist_node.get("@id")
                if dist_id and dist_id in processed_dist_ids:
                    continue
                if dist_id:
                    processed_dist_ids.add(dist_id)

                dist_data = {
                    "uri": dist_id,
                    "title": self._get_value(
                        dist_node, "dct:title", prefer_locale=locale
                    ),
                    "downloadURL": self._get_value(dist_node, "dcat:downloadURL"),
                    "accessURL": self._get_value(dist_node, "dcat:accessURL"),
                    "format": self._get_value(
                        dist_node, "dct:format"
                    ),  # URI or literal
                    "mediaType": self._get_value(
                        dist_node, "dcat:mediaType"
                    ),  # URI or literal
                    "byteSize": self._get_value(dist_node, "dcat:byteSize"),
                    "modified": self._get_value(dist_node, "dct:modified"),
                    "issued": self._get_value(dist_node, "dct:issued"),
                    "license": self._get_value(
                        dist_node, "dct:license"
                    ),  # URI or literal
                    "description": self._get_value(
                        dist_node, "dct:description", prefer_locale=locale
                    ),
                }

                # Attempt to get labels for format/mediaType if they are URIs
                if isinstance(dist_data["format"], str) and dist_data[
                    "format"
                ].startswith("http"):
                    format_node = nodes_by_id.get(dist_data["format"])
                    dist_data["format_label"] = self._get_value(
                        format_node, "skos:prefLabel", prefer_locale=locale
                    ) or self._get_value(
                        format_node, "rdfs:label", prefer_locale=locale
                    )

                if isinstance(dist_data["mediaType"], str) and dist_data[
                    "mediaType"
                ].startswith("http"):
                    mt_node = nodes_by_id.get(dist_data["mediaType"])
                    dist_data["mediaType_label"] = self._get_value(
                        mt_node, "skos:prefLabel", prefer_locale=locale
                    ) or self._get_value(mt_node, "rdfs:label", prefer_locale=locale)

                # Add only distributions with some useful info (e.g., a URL or URI)
                cleaned_dist = {k: v for k, v in dist_data.items() if v is not None}
                if (
                    cleaned_dist.get("uri")
                    or cleaned_dist.get("downloadURL")
                    or cleaned_dist.get("accessURL")
                ):
                    metadata["distributions"].append(cleaned_dist)

            # Clean top-level None values and empty lists before returning
            return {k: v for k, v in metadata.items() if v is not None and v != []}

        except requests.exceptions.RequestException as e:
            error_msg = f"REST API request failed: {str(e)}"
            if e.response is not None:
                error_msg += f" - Status Code: {e.response.status_code}"
                try:
                    error_msg += f" - Response: {e.response.text[:500]}"
                except Exception:
                    pass
            logging.error(f"{error_msg} for URL: {api_url}")
            return {"error": error_msg}
        except (json.JSONDecodeError, AttributeError, KeyError, TypeError) as e:
            error_msg = f"Failed to parse REST API JSON-LD response: {str(e)}"
            logging.error(
                f"{error_msg} for URL: {api_url}", exc_info=True
            )  # Log traceback
            return {"error": error_msg}
        except Exception as e:  # Catch any other unexpected errors during parsing
            error_msg = f"Unexpected error processing REST API response: {str(e)}"
            logging.error(f"{error_msg} for URL: {api_url}", exc_info=True)
            return {"error": error_msg}

    def _get_metadata_from_sparql_fallback(
        self, dataset_uri: str, force_refresh: bool
    ) -> Dict[str, Any]:
        """Internal method for SPARQL fallback using multiple simple queries."""
        logging.info(f"Executing SPARQL fallback for {dataset_uri}")

        # --- Use the original dataset URI directly in SPARQL queries ---
        # While 88u format exists, the canonical URI provided should resolve.
        # Using the input URI increases robustness if UUID extraction fails or other patterns exist.
        sparql_uri = dataset_uri
        logging.debug(f"Using SPARQL URI: {sparql_uri}")

        metadata = {
            "uri": dataset_uri,
            "sparql_uri_used": sparql_uri,
        }  # Store original and SPARQL URI used
        combined_errors = []

        # --- Define simple queries ---
        # Using placeholder <URI> which will be replaced by sparql_uri
        # select_agg is used for MAX aggregates
        # is_multi indicates if multiple values are expected
        queries = {
            "title": (
                'dct:title ?value . FILTER(LANGMATCHES(LANG(?value), "en") || LANG(?value) = "")',
                False,
                "dct",
                None,
            ),
            "description": (
                'dct:description ?value . FILTER(LANGMATCHES(LANG(?value), "en") || LANG(?value) = "")',
                False,
                "dct",
                None,
            ),
            "publisherName": (
                'dct:publisher ?p . OPTIONAL { ?p foaf:name ?value . } OPTIONAL { ?p rdfs:label ?value . FILTER(LANGMATCHES(LANG(?value), "en") || LANG(?value) = "") } OPTIONAL { ?p skos:prefLabel ?value . FILTER(LANGMATCHES(LANG(?value), "en") || LANG(?value) = "") } FILTER(BOUND(?value))',
                False,
                "dct foaf rdfs skos",
                None,
            ),
            "publisherUri": (
                "dct:publisher ?value . FILTER(ISURI(?value))",
                False,
                "dct",
                None,
            ),  # Get publisher URI too
            "modified": (
                "dct:modified ?date .",
                False,
                "dct",
                "(MAX(?date) AS ?value)",
            ),  # Aggregate
            "issued": (
                "dct:issued ?date .",
                False,
                "dct",
                "(MAX(?date) AS ?value)",
            ),  # Aggregate
            "keywords": ("dcat:keyword ?value .", True, "dcat", None),
            "themes": (
                "dcat:theme ?value . FILTER(ISURI(?value))",
                True,
                "dcat",
                None,
            ),  # Theme URIs
            "languages": (
                "dct:language ?value . FILTER(ISURI(?value))",
                True,
                "dct",
                None,
            ),  # Language URIs
            "licenses": (
                "dct:license ?value .",
                True,
                "dct",
                None,
            ),  # License URIs or Literals
        }
        prefixes = {
            "dct": "PREFIX dct: <http://purl.org/dc/terms/>\n",
            "foaf": "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n",
            "skos": "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n",
            "rdfs": "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n",
            "dcat": "PREFIX dcat: <http://www.w3.org/ns/dcat#>\n",
        }

        # --- Execute core property queries ---
        for prop_name, (pattern, is_multi, req_prefixes, select_agg) in queries.items():
            # Determine the variable name in the pattern (usually ?value or ?date for aggregates)
            var_match = re.search(r"\?(\w+)\s*\.", pattern)
            variable_in_pattern = (
                var_match.group(0)[:-2] if var_match else "?value"
            )  # e.g. ?value or ?date

            # Construct SELECT clause
            select_expr = select_agg if select_agg else variable_in_pattern

            # Grouping needed for aggregate functions like MAX
            group_by_clause = "GROUP BY ?dataset" if select_agg else ""
            # Need to bind the dataset URI for grouping
            where_clause = f"BIND(<{sparql_uri}> AS ?dataset) . ?dataset {pattern}"

            # Build necessary prefixes string
            query_prefixes = "".join(
                prefixes[p] for p in req_prefixes.split() if p in prefixes
            )

            query = f"""
                {query_prefixes}
                SELECT {select_expr}
                WHERE {{ {where_clause} }}
                {group_by_clause}
                LIMIT {200 if is_multi else 1}
            """
            # Unique cache suffix for each property query
            prop_cache_suffix = (
                f"sparql_prop_{self._sanitize_filename(sparql_uri)}_{prop_name}"
            )
            response = self._execute_sparql_query(
                query, force_refresh=force_refresh, cache_key_suffix=prop_cache_suffix
            )

            # Check response structure before accessing results
            if (
                isinstance(response, dict)
                and "results" in response
                and isinstance(response["results"], dict)
            ):
                bindings = response["results"].get("bindings", [])
            else:
                bindings = []  # Treat invalid response structure as no results

            if isinstance(response, dict) and "error" in response:
                error_msg = f"Failed to fetch SPARQL property '{prop_name}': {response['error']}"
                combined_errors.append(error_msg)
                logging.warning(error_msg + f" for {sparql_uri}")
                metadata[f"error_{prop_name}"] = error_msg  # Add specific error
            elif bindings:
                target_var = "value"  # The name used in SELECT (or AS alias)
                if is_multi:
                    metadata[prop_name] = [
                        b.get(target_var, {}).get("value")
                        for b in bindings
                        if b.get(target_var, {}).get("value")
                    ]
                elif bindings[0].get(
                    target_var
                ):  # Check if the target variable exists in the first binding
                    metadata[prop_name] = bindings[0].get(target_var, {}).get("value")
            # else: No bindings found, property remains absent from metadata dict

        # Use publisherName as publisher if available, otherwise keep None/URI
        metadata["publisher"] = metadata.pop("publisherName", None) or metadata.get(
            "publisher"
        )
        # Add publisher URI if name wasn't found but URI was
        if not metadata.get("publisher") and metadata.get("publisherUri"):
            metadata["publisher_uri"] = metadata.pop("publisherUri")
        else:
            metadata.pop("publisherUri", None)  # Remove helper URI if name was found

        # --- Execute distribution query ---
        dist_query = f"""
            PREFIX dcat: <http://www.w3.org/ns/dcat#>
            PREFIX dct: <http://purl.org/dc/terms/>
            SELECT DISTINCT ?dist ?downloadURL ?accessURL ?distTitle ?format_str ?mediaType ?byteSize
            WHERE {{
              <{sparql_uri}> dcat:distribution ?dist .
              OPTIONAL {{ ?dist dcat:downloadURL ?downloadURL . }}
              OPTIONAL {{ ?dist dcat:accessURL ?accessURL . }}
              OPTIONAL {{ ?dist dct:title ?distTitle . FILTER(LANGMATCHES(LANG(?distTitle), "en") || LANG(?distTitle) = "") }}
              OPTIONAL {{ ?dist dct:format ?formatURI . BIND(COALESCE(STR(?formatURI), "") AS ?format_str) }} # Handle unbound format
              OPTIONAL {{ ?dist dcat:mediaType ?mediaType . }}
              OPTIONAL {{ ?dist dcat:byteSize ?byteSize . }}
            }} ORDER BY ?dist LIMIT 200
        """
        dist_cache_suffix = f"sparql_dist_{self._sanitize_filename(sparql_uri)}"
        dist_response = self._execute_sparql_query(
            dist_query, force_refresh=force_refresh, cache_key_suffix=dist_cache_suffix
        )

        metadata["distributions"] = []  # Initialize even if query fails

        if (
            isinstance(dist_response, dict)
            and "results" in dist_response
            and isinstance(dist_response["results"], dict)
        ):
            dist_bindings = dist_response["results"].get("bindings", [])
        else:
            dist_bindings = []

        if isinstance(dist_response, dict) and "error" in dist_response:
            error_msg = (
                f"Failed to fetch SPARQL distributions: {dist_response['error']}"
            )
            combined_errors.append(error_msg)
            logging.warning(error_msg + f" for {sparql_uri}")
            metadata["error_distributions"] = error_msg  # Add specific error
        else:
            # Process distributions
            processed_dist_uris = set()
            for row in dist_bindings:
                dist_uri = row.get("dist", {}).get("value")
                # Ensure we have a dist_uri and haven't processed it
                if dist_uri and dist_uri not in processed_dist_uris:
                    dist_data = {
                        "uri": dist_uri,
                        "downloadURL": row.get("downloadURL", {}).get("value"),
                        "accessURL": row.get("accessURL", {}).get("value"),
                        "title": row.get("distTitle", {}).get("value"),
                        "format": row.get("format_str", {}).get(
                            "value"
                        ),  # Use format_str from BIND/COALESCE
                        "mediaType": row.get("mediaType", {}).get("value"),
                        "byteSize": row.get("byteSize", {}).get("value"),
                    }
                    # Add only non-empty values
                    cleaned_dist = {
                        k: v for k, v in dist_data.items() if v is not None and v != ""
                    }
                    # Add if it has at least a URI or a URL
                    if (
                        cleaned_dist.get("uri")
                        or cleaned_dist.get("downloadURL")
                        or cleaned_dist.get("accessURL")
                    ):
                        metadata["distributions"].append(cleaned_dist)
                        processed_dist_uris.add(dist_uri)

        # Check if *any* data beyond URI was fetched
        core_data_keys = set(queries.keys()) | {"publisher"}
        core_data_found = any(k in metadata for k in core_data_keys)

        if (
            not core_data_found
            and not metadata.get("distributions")
            and combined_errors
        ):
            # If nothing worked at all, return a consolidated error
            return {
                "error": f"SPARQL fallback failed to retrieve any metadata or distributions for {sparql_uri}. Errors: {'; '.join(combined_errors)}"
            }

        # Add combined errors if any occurred but some data was fetched
        if combined_errors:
            metadata["sparql_errors"] = combined_errors

        # Return the potentially limited metadata collected via SPARQL
        return metadata

    def get_distribution_formats(
        self, dataset_uri: str, force_refresh: bool = False
    ) -> List[Dict[str, str]]:
        """Gets available distribution details using get_dataset_metadata."""
        # Call the main metadata function which handles REST/SPARQL and caching
        metadata = self.get_dataset_metadata(dataset_uri, force_refresh=force_refresh)

        # Check for errors or empty distributions list
        if "error" in metadata:
            logging.warning(
                f"Cannot get formats for {dataset_uri}, metadata fetch failed: {metadata['error']}"
            )
            return []  # Return empty list on error
        elif not metadata.get("distributions"):
            # Log specifically if distributions part failed during SPARQL fallback
            if "error_distributions" in metadata:
                logging.warning(
                    f"No distributions found for {dataset_uri}, fetch failed: {metadata['error_distributions']}"
                )
            else:
                logging.info(f"No distributions listed in metadata for {dataset_uri}.")
            return []  # Return empty list if no distributions found
        else:
            # Successfully retrieved distributions
            return metadata.get("distributions", [])

    def get_dataset_content(
        self,
        dataset_uri: str,
        preferred_formats: Optional[List[str]] = None,
        force_refresh: bool = False,  # Force refresh for metadata AND download
    ) -> Dict[str, Any]:
        """
        Retrieve actual dataset content, selecting the best format and handling caching.
        Relies on get_dataset_metadata to find distributions. Handles both REST/SPARQL metadata structures.
        """
        if preferred_formats is None:
            preferred_formats = self.preferred_formats

        # --- 1. Get metadata to find distributions ---
        # Pass force_refresh down to metadata fetching; this handles REST/SPARQL and caching
        dataset_metadata = self.get_dataset_metadata(
            dataset_uri, force_refresh=force_refresh
        )

        if "error" in dataset_metadata:
            return {
                "error": f"Could not get metadata before fetching content: {dataset_metadata['error']}"
            }

        distributions = dataset_metadata.get("distributions", [])
        if not distributions:
            # Provide context if SPARQL fallback was used and failed for distributions
            if "error_distributions" in dataset_metadata:
                return {
                    "error": f"No distributions could be fetched for this dataset ({dataset_metadata.get('error_distributions', 'Unknown error')})"
                }
            elif dataset_metadata.get(
                "sparql_uri_used"
            ):  # Check if SPARQL fallback was the source
                return {
                    "error": "No distributions found for this dataset (via SPARQL fallback)."
                }
            else:  # REST API succeeded but reported no distributions
                return {
                    "error": "No distributions listed for this dataset in the retrieved metadata."
                }

        # --- 2. Select the best distribution ---
        selected_dist = None
        selected_format_label = None  # The format identifier (label, URI, or literal)
        norm_preferred = [f.upper() for f in preferred_formats]

        for fmt_pref in norm_preferred:
            if selected_dist:
                break
            for dist in distributions:
                # Prioritize downloadURL, fallback to accessURL
                # Some accessURLs might point to landing pages, not direct downloads
                target_url = dist.get("downloadURL") or dist.get("accessURL")
                if not target_url:
                    continue

                # Extract format info carefully (might be URI, label, or plain string)
                format_val = dist.get("format")  # Could be URI or literal
                format_label = dist.get(
                    "format_label"
                )  # Present if REST found label for URI
                media_type = (dist.get("mediaType") or "").upper()
                # Use the most specific format info available for matching
                format_string_for_match = (format_label or format_val or "").upper()

                url_lower = target_url.lower()

                # Consistent matching logic
                matches_format = (
                    (format_string_for_match and fmt_pref in format_string_for_match)
                    or (
                        media_type
                        and any(
                            f"/{fmt_pref}" in mt_part
                            for mt_part in media_type.split("/")
                        )
                    )
                    or
                    # Check file extension or format parameter in URL
                    any(
                        ext in url_lower.split("?")[0].split("/")[-1]
                        for ext in [
                            f".{fmt_pref.lower()}",
                            f"?format={fmt_pref.lower()}",
                            f"&format={fmt_pref.lower()}",
                        ]
                    )
                    or
                    # Specific type matching
                    (
                        fmt_pref == "RDF"
                        and any(
                            rdf_fmt in format_string_for_match
                            for rdf_fmt in [
                                "RDF",
                                "XML",
                                "TURTLE",
                                "N3",
                                "JSON-LD",
                                "OWL",
                            ]
                        )
                    )
                    or (
                        fmt_pref == "XML"
                        and ("XML" in media_type or "XML" in format_string_for_match)
                    )
                    or (
                        fmt_pref == "JSON"
                        and ("JSON" in media_type or "JSON" in format_string_for_match)
                    )
                    or (
                        fmt_pref == "CSV"
                        and (
                            "CSV" in media_type
                            or "CSV" in format_string_for_match
                            or "comma-separated" in format_string_for_match
                        )
                    )
                )

                if matches_format:
                    selected_dist = dist
                    # Store the best available format identifier
                    selected_format_label = format_label or format_val or fmt_pref
                    break

        # Fallback: If no preferred format found, take first distribution with a download/access URL
        if not selected_dist:
            for dist in distributions:
                target_url = dist.get("downloadURL") or dist.get("accessURL")
                if target_url:
                    selected_dist = dist
                    selected_format_label = dist.get("format_label") or dist.get(
                        "format", "unknown"
                    )
                    break

        if not selected_dist:
            return {
                "error": "No suitable distribution with a download or access URL found matching preferred formats or as fallback."
            }

        # Use the URL from the selected distribution
        download_url = selected_dist.get("downloadURL") or selected_dist.get(
            "accessURL"
        )
        logging.info(f"Selected distribution URL for download: {download_url}")

        # --- 3. Check content cache ---
        # Cache key based on the specific download URL being used
        content_cache_key = self._sanitize_filename(f"content_{download_url}")
        cache_path = os.path.join(self.cache_dir, content_cache_key + ".content")
        meta_path = cache_path + ".meta"

        if (
            self.cache_enabled
            and not force_refresh
            and os.path.exists(cache_path)
            and os.path.exists(meta_path)
        ):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    content_metadata = json.load(f)

                cache_time = content_metadata.get("_cache_timestamp", 0)
                dataset_mod_cached = content_metadata.get("dataset_modified")
                # Get current modified date from the metadata we fetched earlier
                dataset_mod_current = dataset_metadata.get("modified")

                is_expired = time.time() - cache_time >= self.cache_ttl
                # Consider stale if modified date exists in both places and differs
                is_stale = (
                    dataset_mod_cached
                    and dataset_mod_current
                    and dataset_mod_cached != dataset_mod_current
                )

                if not is_expired and not is_stale:
                    logging.info(f"Cache hit for content: {download_url}")
                    is_binary = content_metadata.get("is_binary", False)
                    mode = "rb" if is_binary else "r"
                    encoding = (
                        None if is_binary else "utf-8"
                    )  # Assume UTF-8 for text cache
                    with open(cache_path, mode, encoding=encoding) as f:
                        content = f.read()

                    return {
                        "content": content,
                        "format": content_metadata.get(
                            "format"
                        ),  # Format stored in cache meta
                        "content_type": content_metadata.get("content_type"),
                        "is_binary": is_binary,
                        "size": content_metadata.get("size"),
                        "source_url": content_metadata.get("source_url"),
                    }
                else:
                    logging.info(
                        f"Content cache {'expired' if is_expired else 'stale'} for: {download_url}"
                    )

            except (json.JSONDecodeError, IOError, UnicodeDecodeError) as e:
                logging.warning(
                    f"Content cache read error for {cache_path}: {e}. Fetching fresh data."
                )
                try:  # Attempt cleanup of corrupted cache files
                    if os.path.exists(cache_path):
                        os.remove(cache_path)
                    if os.path.exists(meta_path):
                        os.remove(meta_path)
                except OSError:
                    pass

        # --- 4. Download content ---
        self._ensure_request_delay()
        logging.info(f"Downloading content from: {download_url}")
        try:
            # Use general class headers, not SPARQL specific ones
            response = requests.get(
                download_url,
                headers=self.headers,
                timeout=self.REQUEST_TIMEOUT
                * 2,  # Longer timeout for potential large downloads
                stream=True,  # Use stream to read content type before loading all
                allow_redirects=True,  # Follow redirects
            )
            response.raise_for_status()

            # Determine if binary based on Content-Type header
            content_type = response.headers.get("Content-Type", "").lower()
            # Refined binary check (common text types)
            is_binary = not any(
                ct in content_type
                for ct in [
                    "text",
                    "json",
                    "xml",
                    "csv",
                    "html",
                    "rdf",
                    "turtle",
                    "n3",
                    "sparql-results",
                    "ld+json",
                ]
            )
            logging.debug(
                f"Detected Content-Type: {content_type}, Is binary: {is_binary}"
            )

            # Read content into memory
            content = response.content

            # Try to decode if not detected as binary
            if not is_binary:
                detected_encoding = (
                    response.encoding or "utf-8"
                )  # Use detected or default to utf-8
                logging.debug(
                    f"Attempting to decode as text using encoding: {detected_encoding}"
                )
                try:
                    text_content = content.decode(detected_encoding, errors="replace")
                    content = text_content  # Use decoded string if successful
                    logging.debug("Successfully decoded content as text.")
                except (UnicodeDecodeError, LookupError) as decode_err:
                    logging.warning(
                        f"Could not decode content from {download_url} as text ({detected_encoding}), treating as binary. Error: {decode_err}"
                    )
                    is_binary = True  # Revert to binary if decoding fails

            # --- 5. Cache downloaded content ---
            if self.cache_enabled:
                content_metadata_to_cache = {
                    "format": selected_format_label,  # Store the determined format
                    "content_type": content_type,
                    "is_binary": is_binary,
                    "size": len(
                        content
                    ),  # Store actual size (bytes for binary, chars for text)
                    "source_url": download_url,
                    "dataset_modified": dataset_metadata.get(
                        "modified"
                    ),  # Store dataset mod date for staleness check
                    "_cache_timestamp": time.time(),
                }
                try:
                    # Save content
                    mode = "wb" if is_binary else "w"
                    encoding = (
                        None if is_binary else "utf-8"
                    )  # Always cache text as UTF-8
                    temp_content_file = cache_path + ".tmp"
                    with open(temp_content_file, mode, encoding=encoding) as f:
                        f.write(content)
                    os.replace(temp_content_file, cache_path)

                    # Save metadata
                    temp_meta_file = meta_path + ".tmp"
                    with open(temp_meta_file, "w", encoding="utf-8") as f:
                        json.dump(content_metadata_to_cache, f, ensure_ascii=False)
                    os.replace(temp_meta_file, meta_path)

                    logging.info(f"Cached content from: {download_url}")
                except IOError as e:
                    logging.error(f"Failed to write content cache {cache_path}: {e}")
                except Exception as e:  # Catch potential encoding errors during write
                    logging.error(
                        f"Failed to write content cache {cache_path} due to unexpected error: {e}"
                    )
                    # Attempt to clean up temp files if write failed
                    try:
                        if os.path.exists(temp_content_file):
                            os.remove(temp_content_file)
                        if os.path.exists(temp_meta_file):
                            os.remove(temp_meta_file)
                    except OSError:
                        pass

            return {
                "content": content,
                "format": selected_format_label,  # Return the determined format
                "content_type": content_type,
                "is_binary": is_binary,
                "size": len(content),
                "source_url": download_url,
            }

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to download content from {download_url}: {str(e)}"
            if e.response is not None:
                error_msg += f" - Status Code: {e.response.status_code}"
            logging.error(error_msg)
            return {"error": error_msg}
        except (
            Exception
        ) as e:  # Catch other unexpected errors during download/processing
            error_msg = (
                f"Unexpected error getting content from {download_url}: {str(e)}"
            )
            logging.error(error_msg, exc_info=True)
            return {"error": error_msg}

    def clear_cache(self, dataset_uri: Optional[str] = None):
        """
        Clear the cache.

        If dataset_uri is provided, attempts to clear cached metadata (REST & SPARQL)
        and related SPARQL sub-queries for that specific URI. Clearing specific content
        cache is complex and not fully implemented; use full clear for content.

        If dataset_uri is None, clears the entire cache directory.
        """
        if not self.cache_enabled:
            logging.info("Cache is disabled, nothing to clear.")
            return

        if dataset_uri:
            logging.info(
                f"Clearing cache entries related to dataset URI: {dataset_uri}"
            )
            # --- Clear Metadata Caches ---
            meta_cache_key_base = self._sanitize_filename(f"metadata_{dataset_uri}")
            # Clear known metadata cache patterns (add more locales if used)
            for suffix in ["_rest_en", "_sparql"]:
                filename = meta_cache_key_base + suffix + ".json"
                cache_path = os.path.join(self.cache_dir, filename)
                if os.path.exists(cache_path):
                    try:
                        os.remove(cache_path)
                        logging.debug(f"Removed metadata cache file: {filename}")
                    except OSError as e:
                        logging.warning(
                            f"Could not remove cache file {cache_path}: {e}"
                        )

            # --- Clear SPARQL Sub-Query Caches (Heuristic) ---
            # Used by SPARQL fallback for properties and distributions
            sanitized_uri = self._sanitize_filename(dataset_uri)
            prefixes_to_clear = [
                f"sparql_prop_{sanitized_uri}_",
                f"sparql_dist_{sanitized_uri}",
            ]
            # Also check for 88u format pattern if applicable (though primary uses original URI now)
            dataset_uuid = self._extract_uuid_from_uri(dataset_uri)
            if dataset_uuid:
                sanitized_uri_88u = self._sanitize_filename(
                    f"http://data.europa.eu/88u/dataset/{dataset_uuid}"
                )
                prefixes_to_clear.extend(
                    [
                        f"sparql_prop_{sanitized_uri_88u}_",
                        f"sparql_dist_{sanitized_uri_88u}",
                    ]
                )

            removed_sub_caches = 0
            for filename in os.listdir(self.cache_dir):
                for prefix in prefixes_to_clear:
                    if filename.startswith(prefix) and filename.endswith(".json"):
                        cache_path = os.path.join(self.cache_dir, filename)
                        if os.path.isfile(cache_path):
                            try:
                                os.remove(cache_path)
                                logging.debug(
                                    f"Removed SPARQL sub-cache file: {filename}"
                                )
                                removed_sub_caches += 1
                            except OSError as e:
                                logging.warning(
                                    f"Could not remove cache file {cache_path}: {e}"
                                )
                            break  # Move to next filename once prefix matched

            logging.debug(f"Removed {removed_sub_caches} SPARQL sub-cache files.")

            # --- Content Cache Clearing ---
            # Clearing specific content cache is difficult as keys depend on download URLs,
            # which are only known after fetching metadata.
            logging.warning(
                "Clearing specific content cache by dataset URI is not reliably implemented. "
                "To clear content cache, clear the entire cache or manually delete '.content' and '.meta' files."
            )

            logging.info(f"Finished attempting to clear cache for {dataset_uri}.")

        else:
            # --- Clear Entire Cache Directory ---
            logging.info(f"Clearing all cache files in directory: {self.cache_dir}")
            cleared_files = 0
            failed_files = 0
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        cleared_files += 1
                    # Optionally clear subdirectories if any were created (none currently)
                    # elif os.path.isdir(file_path): shutil.rmtree(file_path)
                except Exception as e:
                    failed_files += 1
                    logging.error(f"Failed to delete {file_path}. Reason: {e}")

            if failed_files == 0:
                logging.info(f"Successfully cleared {cleared_files} files from cache.")
            else:
                logging.warning(
                    f"Cleared {cleared_files} files, but failed to delete {failed_files} files/links."
                )
