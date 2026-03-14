import argparse

from nhl_model.audit.sheets_templates import generate_templates
from nhl_model.config import PATHS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--slate-date', required=True)
    args = parser.parse_args()

    templates = generate_templates(args.slate_date)
    print("\nPhase 1C Google Sheets template generation finished.\n")
    print(f"Templates written to: {PATHS.templates / 'google_sheets'}")
    print("Generated files:")
    for name in templates:
        print(f"- {name}")


if __name__ == '__main__':
    main()
