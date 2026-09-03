from datetime import date

from flask import current_app

import calendar as pycalendar

from channels.nc import (
    fetch_time_played_calendar,
    fetch_time_played_latest_date,
    fetch_time_played_active_months,
)


def get_oidc():
    return current_app.extensions.get("oidc")


def parse_int(value):
    """Parse string to int, return None if invalid"""
    return int(value) if value.isdigit() else None


def is_public_profile(user_profile, logged_in_user):
    if logged_in_user and user_profile.get("username") == logged_in_user.get(
        "username"
    ):
        return True
    public_profile = user_profile.get("attributes", {}).get("public_profile")
    print(public_profile)
    return public_profile if public_profile is not None else False


def build_calendar_context(serial_prefixes, month_param=None, serial_to_wii=None):
    today = date.today()
    year, month = today.year, today.month
    if month_param:
        try:
            parsed_year, parsed_month = month_param.split("-")
            year, month = int(parsed_year), int(parsed_month)
            if not 1 <= month <= 12 or not 2000 <= year <= 2100:
                raise ValueError
        except ValueError:
            year, month = today.year, today.month
    else:
        latest = fetch_time_played_latest_date(serial_prefixes)
        if latest:
            year, month = latest.year, latest.month

    entries = fetch_time_played_calendar(
        serial_prefixes, year, month, serial_to_wii=serial_to_wii
    )

    by_day = {}
    for entry in entries:
        by_day.setdefault(entry["day"], []).append(entry)

    weeks = []
    for week in pycalendar.Calendar(firstweekday=6).monthdayscalendar(year, month):
        days = []
        for day_number in week:
            if day_number:
                day = date(year, month, day_number)
                days.append({"date": day, "entries": by_day.get(day, [])})
            else:
                days.append({"date": None, "entries": []})
        weeks.append(days)

    # Days of this month where something was actually played (mobile agenda).
    active_days = [
        {"date": day["date"], "entries": day["entries"]}
        for week in weeks
        for day in week
        if day["entries"]
    ]

    prev_year, prev_month = (year - 1, 12) if month == 1 else (year, month - 1)
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)

    # Adjacent months that actually have plays, for the double-arrow jumps.
    active_months = [
        ym
        for ym in fetch_time_played_active_months(serial_prefixes)
        if ym[0] is not None and ym[1] is not None
    ]
    current = (year, month)
    earlier = [ym for ym in active_months if ym < current]
    later = [ym for ym in active_months if ym > current]
    prev_active_param = (
        f"{earlier[-1][0]:04d}-{earlier[-1][1]:02d}" if earlier else None
    )
    next_active_param = f"{later[0][0]:04d}-{later[0][1]:02d}" if later else None

    return {
        "calendar_weeks": weeks,
        "active_days": active_days,
        "today": today,
        "month_label": f"{pycalendar.month_name[month]} {year}",
        "month_param": f"{year:04d}-{month:02d}",
        "prev_month_param": f"{prev_year:04d}-{prev_month:02d}",
        "next_month_param": f"{next_year:04d}-{next_month:02d}",
        "prev_active_param": prev_active_param,
        "next_active_param": next_active_param,
    }
