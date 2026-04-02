def format_serial(s):
    """Format serial number with dashes every 4 characters"""
    s = str(s)
    return "-".join([s[i : i + 4] for i in range(0, len(s), 4)])


def format_playtime(minutes):
    """Format minutes as days, hours, minutes"""
    if not minutes:
        return "0m"
    minutes = int(minutes)
    days = minutes // (24 * 60)
    remaining = minutes % (24 * 60)
    hours = remaining // 60
    mins = remaining % 60

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if mins > 0 or not parts:
        parts.append(f"{mins}m")
    return " ".join(parts)
