"""The one browser User-Agent every exchange / news fetch sends.

Exchange edges reject stale browser versions: on 2026-09-24 (05:51-06:10 IST; the engine's fetch at
19:00 the day before still passed) BSE's API host answered 403 to ``Chrome/126`` while ``Chrome/153``
passed, on the scrip master, an SHP endpoint and the insider disclosure endpoint. When a feed starts 403-ing,
bump this string to the current stable Chrome and re-probe.
"""

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/153.0.0.0 Safari/537.36"
)
