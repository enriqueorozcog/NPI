import getpass

import keyring
import requests

from pmx_auth import PMX_CREDENTIAL_SERVICE, PMX_LOGIN_URL


def main():
    username = input("PMX domain account (without PEGA\\): ").strip()
    password = getpass.getpass("PMX password: ")
    if not username or not password:
        raise SystemExit("Account and password are required.")

    print("Validating the account with PMX (this may take up to 90 seconds)...")
    try:
        response = requests.post(
            PMX_LOGIN_URL,
            json={"username": username, "password": password},
            timeout=(5, 90),
        )
    except requests.RequestException as error:
        raise SystemExit(f"Unable to contact PMX: {error}") from error
    if response.status_code in (401, 403):
        raise SystemExit("PMX rejected the account or password.")
    response.raise_for_status()
    if not response.json().get("token"):
        raise SystemExit("PMX login did not return an authentication token.")

    keyring.set_password(PMX_CREDENTIAL_SERVICE, username, password)
    credential = keyring.get_credential(PMX_CREDENTIAL_SERVICE, None)
    if credential is None:
        raise SystemExit("Windows Credential Manager did not save the credential.")

    print(f"PMX credentials saved securely for {credential.username}.")
    print(f"The app will renew tokens automatically through {PMX_LOGIN_URL}.")


if __name__ == "__main__":
    main()