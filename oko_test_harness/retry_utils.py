"""Common retry utilities using tenacity library."""

from tenacity import retry, stop_after_delay, wait_fixed


class StopRetryingException(Exception):
    """Exception to immediately halt retry attempts.

    Used for unrecoverable conditions like crash loops where
    continuing to retry would be pointless.
    """

    pass


def _should_retry(retry_state):
    """Custom retry condition: retry on any exception except StopRetryingException."""
    if retry_state.outcome.failed:
        exception = retry_state.outcome.exception()
        return not isinstance(exception, StopRetryingException)
    return False


def oko_retry(timeout_seconds: int, interval_seconds: int):
    """Retry with custom interval for specific use cases.

    Args:
        timeout_seconds: Maximum time to retry in seconds
        interval_seconds: Seconds to wait between attempts

    Returns:
        tenacity retry decorator configured with custom interval
    """
    return retry(
        stop=stop_after_delay(timeout_seconds),
        wait=wait_fixed(interval_seconds),
        retry=_should_retry,
        reraise=True,
    )
