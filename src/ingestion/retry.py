"""
ScholarAssist — Retry Manager

Provides configurable retry logic with exponential back-off, jitter, a
dead-letter queue backed by S3, and checkpoint persistence for long-running
ingestion jobs.

Usage::

    cfg     = RetryConfig(max_retries=5, initial_delay=1.0)
    manager = RetryManager(s3_settings, cfg)

    result = await manager.execute_with_retry(some_async_fn, arg1, arg2)
    manager.save_checkpoint("openalex-bulk", {"page": 42})
"""

from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional, TypeVar

import boto3
import structlog
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from src.config.settings import S3Settings

logger = structlog.get_logger(__name__)

T = TypeVar("T")


# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

@dataclass(frozen=True)
class RetryConfig:
    """Parameters that govern retry behaviour.

    Attributes
    ----------
    max_retries:
        Maximum number of retry attempts (not counting the first call).
    initial_delay:
        Base delay in seconds before the first retry.
    max_delay:
        Upper bound for any single back-off delay.
    exponential_base:
        Multiplier applied on each successive retry.
    jitter:
        If ``True``, add a random component (0 – 50 % of delay) to avoid
        thundering-herd issues.
    retryable_exceptions:
        Exception types that should trigger a retry.  All other exceptions
        propagate immediately.
    """

    max_retries: int = 5
    initial_delay: float = 1.0
    max_delay: float = 120.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple[type[BaseException], ...] = field(
        default=(Exception,),
    )


# ------------------------------------------------------------------
# Retry Manager
# ------------------------------------------------------------------

class RetryManager:
    """Execute callables with retry, persist dead-letters, and manage checkpoints.

    Parameters
    ----------
    s3_settings:
        S3/MinIO connection and bucket settings.
    config:
        Retry behaviour parameters.
    """

    def __init__(
        self,
        s3_settings: S3Settings,
        config: Optional[RetryConfig] = None,
    ) -> None:
        self._settings = s3_settings
        self._config = config or RetryConfig()
        self._client: Optional[S3Client] = None

    # ------------------------------------------------------------------
    # Lazy S3 client
    # ------------------------------------------------------------------

    @property
    def client(self) -> S3Client:
        """Lazy-initialised boto3 S3 client."""
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self._settings.endpoint_url,
                aws_access_key_id=self._settings.access_key,
                aws_secret_access_key=self._settings.secret_key,
                region_name=self._settings.region,
                use_ssl=self._settings.use_ssl,
            )
        return self._client

    # ------------------------------------------------------------------
    # Core retry logic
    # ------------------------------------------------------------------

    async def execute_with_retry(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute *func* with exponential back-off retry.

        If *func* is a coroutine function it is ``await``-ed; otherwise it is
        called synchronously.

        Raises
        ------
        Exception
            The last caught exception after all retries are exhausted.
        """
        cfg = self._config
        last_exception: Optional[BaseException] = None

        for attempt in range(1, cfg.max_retries + 2):  # +2: 1-indexed + initial try
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)

            except cfg.retryable_exceptions as exc:
                last_exception = exc

                if attempt > cfg.max_retries:
                    logger.error(
                        "retry.exhausted",
                        func=getattr(func, "__name__", str(func)),
                        attempts=attempt,
                        error=str(exc),
                    )
                    raise

                delay = min(
                    cfg.initial_delay * (cfg.exponential_base ** (attempt - 1)),
                    cfg.max_delay,
                )
                if cfg.jitter:
                    delay += random.uniform(0, delay * 0.5)

                logger.warning(
                    "retry.attempt",
                    func=getattr(func, "__name__", str(func)),
                    attempt=attempt,
                    next_delay_s=round(delay, 2),
                    error=str(exc),
                )
                await asyncio.sleep(delay)

        # Should be unreachable, but keep mypy happy.
        if last_exception is not None:
            raise last_exception  # pragma: no cover

    # ------------------------------------------------------------------
    # Dead-letter queue
    # ------------------------------------------------------------------

    def send_to_dead_letter(
        self,
        source: str,
        record: Any,
        error: str | BaseException,
    ) -> str:
        """Upload a failed record to the S3 dead-letter bucket.

        Returns
        -------
        str
            The S3 object key of the dead-letter entry.
        """
        now = datetime.now(timezone.utc)
        key = (
            f"{source}/{now.strftime('%Y-%m-%d')}"
            f"/{now.strftime('%H%M%S')}_{id(record)}.json"
        )

        payload = {
            "source": source,
            "timestamp": now.isoformat(),
            "error": str(error),
            "record": record if isinstance(record, (dict, list, str)) else repr(record),
        }

        self.client.put_object(
            Bucket=self._settings.dead_letter_bucket,
            Key=key,
            Body=json.dumps(payload, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )

        logger.info(
            "dead_letter.sent",
            source=source,
            key=key,
            error=str(error),
        )
        return key

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def save_checkpoint(self, key: str, state: dict[str, Any]) -> None:
        """Persist an arbitrary checkpoint state to S3.

        Checkpoints are stored in the manifests bucket under the
        ``_checkpoints/`` prefix so they sit beside manifests but are
        easy to distinguish.

        Parameters
        ----------
        key:
            A unique identifier for this checkpoint,
            e.g. ``"openalex-bulk-2026-07-01"``.
        state:
            JSON-serialisable dictionary with the checkpoint payload.
        """
        s3_key = f"_checkpoints/{key}.json"

        envelope = {
            "checkpoint_key": key,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "state": state,
        }

        self.client.put_object(
            Bucket=self._settings.manifests_bucket,
            Key=s3_key,
            Body=json.dumps(envelope, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )

        logger.debug("checkpoint.saved", key=key)

    def load_checkpoint(self, key: str) -> Optional[dict[str, Any]]:
        """Load a previously saved checkpoint.

        Returns ``None`` if the checkpoint does not exist.
        """
        s3_key = f"_checkpoints/{key}.json"

        try:
            response = self.client.get_object(
                Bucket=self._settings.manifests_bucket,
                Key=s3_key,
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

        envelope = json.loads(response["Body"].read().decode("utf-8"))
        logger.debug("checkpoint.loaded", key=key)
        return envelope.get("state")

    def delete_checkpoint(self, key: str) -> None:
        """Remove a checkpoint from S3 once it is no longer needed."""
        s3_key = f"_checkpoints/{key}.json"

        try:
            self.client.delete_object(
                Bucket=self._settings.manifests_bucket,
                Key=s3_key,
            )
            logger.debug("checkpoint.deleted", key=key)
        except ClientError:
            logger.warning("checkpoint.delete_failed", key=key, exc_info=True)
