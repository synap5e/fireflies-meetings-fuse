"""Test-wide fixtures.

Pin timezone to Pacific/Auckland so date-derivation tests (projection folding,
backfill-in-progress rendering) produce identical calendar dates regardless of
the host's local timezone. Fixtures like ``date_epoch_ms=1774891800000.0``
resolve to "2026-03-31" in NZ but "2026-03-30" in the US west coast; the
service itself derives dates from local time (that's the correct product
behavior), so the fix is to normalize the test environment, not the fixtures.
"""

from __future__ import annotations

import os
import time

os.environ["TZ"] = "Pacific/Auckland"
time.tzset()
