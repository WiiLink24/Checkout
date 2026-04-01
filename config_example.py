db_url = "postgresql://username:password@localhost/nc"

# Used to secure the web panel.
secret_key = "please_change_thank_you"

# Authentik API configuration
authentik_api_url = ""
authentik_service_account_token = ""

# OpenID Connect configuration
oidc_redirect_uri = ""
oidc_client_secrets_json = {
    "web": {
        "client_id": "",
        "client_secret": "",
        "auth_uri": "",
        "token_uri": "",
        "userinfo_uri": "",
        "issuer": "",
        "redirect_uris": "",
    }
}
oidc_logout_url = ""