"""Generate your personal watsonx.data SQL statements.

Asks for your attendee ID and the COS bucket name, then prints the contents of
kafka_events.sql with both placeholders filled in. Copy the output into the
watsonx.data Query workspace.

Run from the project root:

    uv run python schema/generate_sql.py
"""

import re
import sys
from pathlib import Path

TEMPLATE = Path(__file__).with_name("kafka_events.sql")

ID_PLACEHOLDER = "<your-attendee-id>"
BUCKET_PLACEHOLDER = "<cos-bucket-name>"

# Schema names in Presto are lower case letters, digits and underscores.
ID_PATTERN = re.compile(r"[a-z0-9_]+")
# S3 bucket names: 3-63 characters, lower case letters, digits, dots, hyphens.
BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")


def ask(prompt: str, pattern: re.Pattern, hint: str, clean=lambda s: s) -> str:
    """Prompt until the answer fully matches the pattern."""
    while True:
        answer = clean(input(prompt).strip())
        if pattern.fullmatch(answer):
            return answer
        print(f"  That does not look right. {hint}\n")


def clean_id(value: str) -> str:
    return value.lower()


def clean_bucket(value: str) -> str:
    """Accept a bucket pasted as s3a://name/ or name/ and keep just the name."""
    for prefix in ("s3a://", "s3://"):
        if value.startswith(prefix):
            value = value[len(prefix):]
    return value.strip("/")


def main() -> None:
    print("Generate your personal watsonx.data SQL\n")

    attendee_id = ask(
        "Your attendee ID (e.g. student1): ",
        ID_PATTERN,
        "Use only letters, digits and underscores, as on your credential sheet.",
        clean_id,
    )
    bucket = ask(
        "COS bucket name (from Infrastructure manager): ",
        BUCKET_PATTERN,
        "Copy the bucket name exactly as shown in the console.",
        clean_bucket,
    )

    print(f"\nAttendee ID : {attendee_id}")
    print(f"COS bucket  : {bucket}")
    if input("Is this correct? [Y/n]: ").strip().lower() not in ("", "y", "yes"):
        print("Nothing generated. Run the script again.")
        sys.exit(1)

    # Drop the instruction comments that talk about the placeholders or this
    # script; they make no sense once the values are filled in.
    lines = [
        line
        for line in TEMPLATE.read_text(encoding="utf-8").splitlines()
        if not (
            line.startswith("--")
            and (
                ID_PLACEHOLDER in line
                or BUCKET_PLACEHOLDER in line
                or "generate_sql.py" in line
            )
        )
    ]
    sql = "\n".join(lines).replace(ID_PLACEHOLDER, attendee_id)
    sql = sql.replace(BUCKET_PLACEHOLDER, bucket)

    print("\n" + "=" * 70)
    print("Copy everything below into the watsonx.data Query workspace.")
    print("=" * 70 + "\n")
    print(sql)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(1)
