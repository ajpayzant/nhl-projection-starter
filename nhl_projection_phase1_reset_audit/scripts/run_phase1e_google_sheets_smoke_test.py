"""Optional Google Sheets auth smoke test.

Only run this after creating OAuth desktop credentials and saving them as
`credentials.json` in the repo root, as described in Google's Python quickstart.
"""

from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from nhl_model.config import PATHS

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def main() -> None:
    cred_path = PATHS.project_root / 'credentials.json'
    token_path = PATHS.project_root / 'token.json'
    if not cred_path.exists():
        raise FileNotFoundError('Missing credentials.json in project root')

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(cred_path), SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding='utf-8')

    service = build('sheets', 'v4', credentials=creds)
    print('Google Sheets auth successful.')
    print('Service object created:', bool(service))


if __name__ == '__main__':
    main()
