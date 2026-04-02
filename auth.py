from flask import session
from utils import generate_gravatar_url


def get_user_profile():
    """Get OIDC profile from session"""
    return session.get("oidc_auth_profile") or {}


def build_user_info(profile):
    """Build user info dict from OIDC profile"""
    username = profile.get("preferred_username")
    email = profile.get("email") or ""
    return {
        "username": username,
        "full_name": profile.get("name") or "",
        "profile_picture": generate_gravatar_url(email),
        "linked_wii_no": profile.get("wiis") or []
    }
