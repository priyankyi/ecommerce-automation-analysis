from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_credentials(
    credentials_path: Path | None = None,
    token_path: Path | None = None,
    scopes: Iterable[str] = SCOPES,
    service_account_info: dict[str, object] | None = None,
) -> Credentials:
    if service_account_info:
        return ServiceAccountCredentials.from_service_account_info(service_account_info, scopes=list(scopes))

    root = project_root()
    credentials_path = credentials_path or root / "credentials" / "credentials.json"
    token_path = token_path or root / "credentials" / "token.json"

    creds: Credentials | None = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), list(scopes))

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        ensure_parent(token_path)
        token_path.write_text(creds.to_json(), encoding="utf-8")
        return creds

    if creds and creds.valid:
        return creds

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Missing OAuth client file: {credentials_path}. "
            "Place your Google OAuth client JSON there before running login."
        )

    flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), list(scopes))
    creds = flow.run_local_server(port=0)
    ensure_parent(token_path)
    token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def load_service_account_credentials(
    service_account_info: dict[str, object],
    scopes: Iterable[str] = SCOPES,
) -> ServiceAccountCredentials:
    return ServiceAccountCredentials.from_service_account_info(service_account_info, scopes=list(scopes))


def build_services_from_credentials(creds: Credentials) -> Tuple[object, object]:
    sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False, static_discovery=False)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False, static_discovery=False)
    return sheets_service, drive_service


def build_services(
    credentials_path: Path | None = None,
    token_path: Path | None = None,
    scopes: Iterable[str] = SCOPES,
    service_account_info: dict[str, object] | None = None,
) -> Tuple[object, object, Credentials]:
    creds = load_credentials(
        credentials_path=credentials_path,
        token_path=token_path,
        scopes=scopes,
        service_account_info=service_account_info,
    )
    sheets_service, drive_service = build_services_from_credentials(creds)
    return sheets_service, drive_service, creds


def main() -> None:
    creds = load_credentials()
    payload = json.loads(creds.to_json())
    print("OAuth login complete.")
    print(f"Token saved to: {project_root() / 'credentials' / 'token.json'}")
    print(f"Scopes: {', '.join(payload.get('scopes', SCOPES))}")


if __name__ == "__main__":
    main()
