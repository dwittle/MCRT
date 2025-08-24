    #!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Timeout utilities.
"""

from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeout
from typing import Any, Callable

def with_timeout(fn: Callable[..., Any], seconds: float, *args, **kwargs) -> Any:
    if seconds is None or seconds <= 0:
        return fn(*args, **kwargs)
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn, *args, **kwargs)
        try:
            return fut.result(timeout=seconds)
        except _FuturesTimeout as e:
            raise TimeoutError(f"Operation exceeded {seconds} seconds") from e
