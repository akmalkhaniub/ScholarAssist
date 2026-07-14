"""
ScholarAssist - Claim Extractor

Uses an LLM to scan document chunks and extract falsifiable academic claims.
"""

import logging
import os
from typing import List, Dict, Any
from pydantic import BaseModel, Field

# We use instructor to easily parse structured output from OpenAI
try:
    import instructor
    from openai import OpenAI
except ImportError:
    instructor = None
    OpenAI = None

logger = logging.getLogger(__name__)

class Claim(BaseModel):
    claim_id: str = Field(..., description="A unique identifier for the claim, e.g., c_001")
    text: str = Field(..., description="The falsifiable academic claim extracted from the text.")
    context: str = Field(..., description="The direct sentence or paragraph from the text where the claim was found.")

class ClaimExtractionResult(BaseModel):
    claims: List[Claim] = Field(..., description="A list of extracted claims.")

def _mock_extract(text: str) -> List[Dict[str, Any]]:
    logger.info("Extracting claims via MOCK LLM (No API key found)...")
    return [
        {
            "claim_id": "c_001",
            "text": "Transformer architectures outperform CNNs on large-scale vision tasks.",
            "context": "Recent studies have shown that Transformer architectures outperform CNNs on large-scale vision tasks when pre-trained on enough data."
        },
        {
            "claim_id": "c_002",
            "text": "The learning rate warmup strategy prevents early divergence in Adam.",
            "context": "We implement a learning rate warmup strategy, which prevents early divergence in Adam as noted by previous authors."
        }
    ]

def extract_claims(text: str) -> List[Dict[str, Any]]:
    """
    Extracts falsifiable academic claims from raw text.
    Uses OpenAI GPT-4o if OPENAI_API_KEY is available, else falls back to mock.
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    
    if not api_key or not OpenAI or not instructor:
        logger.warning("OPENAI_API_KEY not found or libraries not installed. Falling back to mock extraction.")
        return _mock_extract(text)

    try:
        logger.info("Extracting claims via OpenAI API...")
        
        # Initialize the instructor-patched OpenAI client
        client = instructor.from_openai(OpenAI(api_key=api_key))
        
        system_prompt = (
            "You are an expert academic research assistant. "
            "Your goal is to extract strictly falsifiable, empirical academic claims from the provided text. "
            "A falsifiable claim is a statement that can be proven true or false via evidence. "
            "Do not extract opinions, generic statements, or definitions. "
            "Extract up to 5 of the most important claims."
        )

        response = client.chat.completions.create(
            model="gpt-4o",
            response_model=ClaimExtractionResult,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Extract claims from the following text:\n\n{text}"}
            ],
            temperature=0.1,
        )
        
        # Convert Pydantic objects to dictionaries for downstream compatibility
        return [claim.model_dump() for claim in response.claims]
        
    except Exception as e:
        logger.error(f"Failed to extract claims via LLM: {e}")
        logger.info("Falling back to mock extraction due to error.")
        return _mock_extract(text)
