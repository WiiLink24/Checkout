from flask import session
from utils.utils import generate_gravatar_url


def get_user_profile():
    """Get OIDC profile from session"""
    return session.get("oidc_auth_profile") or {}


def build_user_info(profile):
    """Build user info dict from OIDC profile"""
    username = profile.get("preferred_username")
    email = profile.get("email") or ""

    wiis = profile.get("wiis") or []
    wii_numbers = []
    serial_numbers = []

    if isinstance(wiis, list):
        for wii in wiis:
            if isinstance(wii, dict):
                wii_number = wii.get("wii_number")
                if wii_number:
                    wii_numbers.append(wii_number)
                serial_number = wii.get("serial_number")
                if serial_number:
                    serial_numbers.append(serial_number)

    return {
        "username": username,
        "full_name": profile.get("name") or "",
        "profile_picture": generate_gravatar_url(email),
        "linked_wii_no": wii_numbers,
        "serial_number": serial_numbers if serial_numbers else [],
    }
