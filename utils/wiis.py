from channels.cmoc import count_contest_submissions_per_wii
from channels.evc import count_user_polls_per_wii
from channels.nc import fetch_metrics_per_wii, fetch_time_played_per_wii
from utils.utils import _resolve_wii_number


def fetch_wii_color_from_number(wii_number):
    """Get color unique color for a given Wii number. Returns a hex color string."""
    if not wii_number:
        return "#888888"  # Default gray color for invalid Wii numbers

    # Normalize the Wii number to ensure consistent color generation
    normalized_wii = str(wii_number).strip().upper()

    # Generate a unique color based on the normalized Wii number
    hash_value = hash(normalized_wii)
    r = (hash_value & 0xFF0000) >> 16
    g = (hash_value & 0x00FF00) >> 8
    b = hash_value & 0x0000FF

    # Ensure the RGB values are within the valid range
    r = max(0, min(255, r))
    g = max(0, min(255, g))
    b = max(0, min(255, b))

    return "#{:02X}{:02X}{:02X}".format(r, g, b)


def build_wii_breakdown(serial_prefixes, wii_numbers):
    """Per-Wii metric rows for the home stat-tile popovers."""
    metrics = fetch_metrics_per_wii(serial_prefixes)
    polls = count_user_polls_per_wii(wii_numbers) if wii_numbers else {}
    contests = count_contest_submissions_per_wii(wii_numbers) if wii_numbers else {}

    breakdown = {}
    for serial, m in metrics.items():
        wii_number = _resolve_wii_number(serial)
        entry = breakdown.setdefault(wii_number, {"wii_number": wii_number})
        entry.update(m)
    for wii_number, count in polls.items():
        breakdown.setdefault(wii_number, {"wii_number": wii_number})["polls"] = count
    for wii_number, count in contests.items():
        breakdown.setdefault(wii_number, {"wii_number": wii_number})["contests"] = count
    return list(breakdown.values())


def attach_time_breakdown(rows, serial_prefixes):
    """Attach per-Wii time/times-played rows to each game family."""
    per_wii = fetch_time_played_per_wii(serial_prefixes)
    if not per_wii:
        return
    serial_to_wii = {}
    for row in per_wii:
        if row["serial"] not in serial_to_wii:
            serial_to_wii[row["serial"]] = _resolve_wii_number(row["serial"])
    families = {}
    for row in per_wii:
        families.setdefault(row["game_id"][:4], []).append(
            {
                "wii_number": serial_to_wii[row["serial"]],
                "time_played": row["time_played"],
                "times_played": row["times_played"],
            }
        )
    for row in rows:
        row["wii_breakdown"] = families.get(row["game_id"][:4], [])
