"""
Embedding service for generating vector embeddings
"""

import asyncio
import logging
from typing import Any

from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating text embeddings"""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """Initialize embedding service with specified model.

        Args:
            model_name: Name of the sentence-transformers model to use
                       Default is 'all-MiniLM-L6-v2' which produces 384-dim embeddings
        """
        self.model_name = model_name
        self._model: SentenceTransformer | None = None
        self._lock = asyncio.Lock()

    @property
    def embedding_dim(self) -> int:
        """Get embedding dimension for the model"""
        if self.model_name == "all-MiniLM-L6-v2":
            return 384
        if self.model_name in {"all-mpnet-base-v2", "all-distilroberta-v1"}:
            return 768
        # Default dimension, will be updated when model loads
        return 768

    def _get_model(self) -> SentenceTransformer:
        """Get or initialize the model (lazy loading)"""
        if self._model is None:
            logger.info(f"Loading embedding model: {self.model_name}")
            self._model = SentenceTransformer(self.model_name)
            logger.info(f"Model loaded, embedding dimension: {self._model.get_sentence_embedding_dimension()}")
        return self._model

    async def generate_embedding(self, text: str) -> list[float]:
        """Generate embedding for a single text string.

        Args:
            text: Text to generate embedding for

        Returns:
            List of float values representing the embedding
        """
        async with self._lock:
            # Run model inference in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._generate_embedding_sync, text)

    def _generate_embedding_sync(self, text: str) -> list[float]:
        """Synchronous embedding generation"""
        model = self._get_model()
        embedding = model.encode(text, convert_to_numpy=True)
        return embedding.tolist()

    async def generate_embeddings(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts.

        Args:
            texts: List of texts to generate embeddings for

        Returns:
            List of embeddings (each embedding is a list of floats)
        """
        async with self._lock:
            # Run batch inference in thread pool
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._generate_embeddings_sync, texts)

    def _generate_embeddings_sync(self, texts: list[str]) -> list[list[float]]:
        """Synchronous batch embedding generation"""
        model = self._get_model()
        embeddings = model.encode(texts, convert_to_numpy=True)
        return [emb.tolist() for emb in embeddings]

    async def generate_from_dict(self, data: dict[str, Any]) -> list[float]:
        """Generate embedding from dictionary data.

        Extracts text fields from dictionary and generates embedding.

        Args:
            data: Dictionary containing data fields

        Returns:
            Embedding vector as list of floats
        """
        # Extract text fields
        text_parts = []
        for key, value in data.items():
            if isinstance(value, str):
                # Add field name for context
                text_parts.append(f"{key}: {value}")
            elif isinstance(value, list):
                # Handle list of strings
                for item in value:
                    if isinstance(item, str):
                        text_parts.append(item)
                    elif isinstance(item, dict) and "text" in item:
                        text_parts.append(item["text"])
            elif isinstance(value, dict):
                # Recursively extract text from nested dict
                nested_text = await self._extract_text_from_dict(value)
                if nested_text:
                    text_parts.append(nested_text)

        # Combine all text parts
        combined_text = " ".join(text_parts)

        # Generate embedding
        if combined_text.strip():
            return await self.generate_embedding(combined_text)
        # Return zero vector if no text found
        return [0.0] * self.embedding_dim

    async def _extract_text_from_dict(self, data: dict) -> str:
        """Recursively extract text from nested dictionary"""
        text_parts = []
        for key, value in data.items():
            if isinstance(value, str):
                text_parts.append(f"{key}: {value}")
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        text_parts.append(item)
            elif isinstance(value, dict):
                nested_text = await self._extract_text_from_dict(value)
                if nested_text:
                    text_parts.append(nested_text)
        return " ".join(text_parts)


# Global embedding service instance
_embedding_service = None


def get_embedding_service(model_name: str = "all-MiniLM-L6-v2") -> EmbeddingService:
    """Get or create global embedding service instance.

    Args:
        model_name: Model to use for embeddings

    Returns:
        EmbeddingService instance
    """
    global _embedding_service  # noqa: PLW0603
    if _embedding_service is None or _embedding_service.model_name != model_name:
        _embedding_service = EmbeddingService(model_name)
    return _embedding_service
