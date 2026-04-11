from flask import current_app


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
