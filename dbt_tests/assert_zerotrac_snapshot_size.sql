-- The pinned snapshot must carry exactly the record count it was measured at.
--
-- This is a PIN INTEGRITY check, not a data-quality one. The upstream file is
-- rewritten weekly after each contest, so a changed count here means the
-- warehouse is reading a different snapshot than the one recorded in LEGAL.md
-- and in the dimension's snapshot_version column - which is precisely the
-- silent drift that pinning a SHA exists to prevent.
--
-- 2,549 records at 881a239306ce7a339e32e7825cdb9c00fead00f1, committed
-- 2026-08-01. If this fails, the pin moved. Update the SHA, the count and the
-- LEGAL.md read-date together, in one change - never just this number.

select
    2549 as expected_records,
    count(*) as actual_records
from {{ ref('stg_zerotrac_ratings') }}
having count(*) <> 2549
