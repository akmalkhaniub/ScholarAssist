"""
ScholarAssist — Claim Verifier

Takes extracted claims and verifies them against the OpenSearch Golden Records.
Falls back to Semantic Scholar API if local index has no hits.
"""

import logging
import httpx
from typing import Dict, Any, List
from opensearchpy import OpenSearch

logger = logging.getLogger(__name__)

def search_semantic_scholar(query: str) -> List[Dict[str, Any]]:
    """
    Fallback data connector: Queries Semantic Scholar for live academic data.
    """
    logger.info(f"Querying Semantic Scholar for: '{query}'")
    url = "https://api.semanticscholar.org/graph/v1/paper/search"
    params = {
        "query": query,
        "limit": 3,
        "fields": "title,abstract,externalIds,authors,year,url"
    }
    
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = []
            for item in data.get("data", []):
                doi = item.get("externalIds", {}).get("DOI")
                results.append({
                    "_id": item["paperId"],
                    "_source": {
                        "title": item.get("title", ""),
                        "abstract": item.get("abstract", ""),
                        "doi": doi,
                        "url": item.get("url", ""),
                        "year": item.get("year", ""),
                        "authors": [{"name": a["name"]} for a in item.get("authors", [])]
                    }
                })
            return results
    except Exception as e:
        logger.error(f"Semantic Scholar API failed: {e}")
        return []

def verify_claim(claim: Dict[str, Any], os_client: OpenSearch, index_name: str) -> Dict[str, Any]:
    """
    1. Converts the claim text into an OpenSearch query.
    2. Retrieves top matching papers (falls back to Semantic Scholar).
    3. Evaluates if the paper supports or refutes the claim.
    """
    logger.info(f"Verifying claim: {claim['text']}")
    
    # 1. Search OpenSearch for papers relevant to the claim
    query_body = {
        "query": {
            "match": {
                "title": claim["text"]
            }
        },
        "size": 3
    }
    
    hits = []
    try:
        response = os_client.search(index=index_name, body=query_body)
        hits = response["hits"]["hits"]
    except Exception as e:
        logger.error(f"OpenSearch error: {e}")
        
    # Fallback to Semantic Scholar Connector if no local hits
    if not hits:
        logger.info("No local hits found, falling back to Semantic Scholar API.")
        hits = search_semantic_scholar(claim["text"])

    # 2. Mock LLM Evaluation logic
    # In production, we would pass the claim and the retrieved abstract to an LLM
    evidence = []
    for hit in hits:
        source = hit["_source"]
        
        # We assume supported if there's any abstract overlap (Naive Mock)
        abstract = source.get("abstract") or ""
        judgment = "Supported" if len(abstract) > 0 else "Neutral"
        
        evidence.append({
            "paper_id": hit["_id"],
            "title": source.get("title", ""),
            "doi": source.get("doi"),
            "url": source.get("url"),
            "judgment": judgment,
            "confidence_score": 0.89
        })
        
    # We define status as Supported if any evidence supports it
    status = "Unverified"
    if any(e["judgment"] == "Supported" for e in evidence):
        status = "Supported"
        
    return {
        "claim_id": claim.get("claim_id", "unknown"),
        "claim_text": claim.get("text", ""),
        "verification_status": status,
        "evidence": evidence
    }
