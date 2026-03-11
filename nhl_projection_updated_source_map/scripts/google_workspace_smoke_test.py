from __future__ import annotations

import os.path
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def get_credentials() -> Credentials:
    creds = None
    token_path = Path("token.json")
    creds_path = Path("credentials.json")

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not creds_path.exists():
                raise FileNotFoundError(
                    "credentials.json not found. Create OAuth desktop credentials in Google Cloud and place the file in the repo root."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def main() -> None:
    creds = get_credentials()

    try:
        drive_service = build("drive", "v3", credentials=creds)
        sheets_service = build("sheets", "v4", credentials=creds)

        drive_files = (
            drive_service.files()
            .list(pageSize=10, fields="files(id, name, mimeType)")
            .execute()
            .get("files", [])
        )
        print("Drive access OK. Sample files:")
        for f in drive_files[:10]:
            print(f"- {f['name']} ({f['mimeType']})")

        print("Sheets API access OK.")
        _ = sheets_service.spreadsheets()

    except HttpError as err:
        raise RuntimeError(f"Google API smoke test failed: {err}") from err


if __name__ == "__main__":
    main()
