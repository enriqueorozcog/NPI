import os
from threading import Lock

import keyring
import requests


PMX_CREDENTIAL_SERVICE = "Performance PMX Agile"
PMX_LOGIN_URL = "http://10.235.4.15:8080/PegApRestful/service/login"


class PmxAuthError(RuntimeError):
    pass


class PmxClient:
    def __init__(self):
        self._session = requests.Session()
        self._token = os.environ.get("PEGAP_AUTH_TOKEN", "").strip()
        self._lock = Lock()

    def _login(self):
        credential = keyring.get_credential(PMX_CREDENTIAL_SERVICE, None)
        if credential is None:
            raise PmxAuthError(
                "PMX credentials are not configured. Run: python3.12 configure_pmx.py"
            )

        response = self._session.post(
            PMX_LOGIN_URL,
            json={"username": credential.username, "password": credential.password},
            timeout=(5, 90),
        )
        if response.status_code in (401, 403):
            raise PmxAuthError("PMX rejected the saved Windows credential.")
        response.raise_for_status()
        token = response.json().get("token", "").strip()
        if not token:
            raise PmxAuthError("PMX login did not return an authentication token.")
        self._token = token

    def request(self, method, url, **kwargs):
        with self._lock:
            if not self._token:
                self._login()
            token = self._token

        headers = dict(kwargs.pop("headers", {}) or {})
        headers.setdefault("Accept", "application/json")
        headers["Authorization"] = f"Bearer {token}"
        response = self._session.request(method, url, headers=headers, **kwargs)
        if response.status_code != 401:
            return response

        with self._lock:
            if self._token == token:
                self._login()
            headers["Authorization"] = f"Bearer {self._token}"
        return self._session.request(method, url, headers=headers, **kwargs)

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)