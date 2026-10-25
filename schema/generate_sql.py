"""Generate your personal watsonx.data SQL statements.

Asks for your attendee ID and the COS bucket name, then writes every query of
the lab (kafka_events.sql) to my_kafka_events.sql with both placeholders filled
in. Copy the statements from that file into the watsonx.data Query workspace.

Run from the project root:

    uv run python schema/generate_sql.py
"""

import re
import sys
from pathlib import Path

TEMPLATE = Path(__file__).with_name("kafka_events.sql")
OUTPUT = Path(__file__).with_name("my_kafka_events.sql")

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
        "Use only letters, digits and underscores, as assigned by your instructor.",
        clean_id,
    )
    bucket = ask(
        "COS bucket name (from watsonx-data-connect-details.txt): ",
        BUCKET_PATTERN,
        "Copy the bucket name exactly as shown in watsonx-data-connect-details.txt.",
        clean_bucket,
    )

    print(f"\nAttendee ID : {attendee_id}")
    print(f"COS bucket  : {bucket}")
    if input("Is this correct? [Y/n]: ").strip().lower() not in ("", "y", "yes"):
        print("Nothing generated. Run the script again.")
        sys.exit(1)

    # Drop the instruction comments that talk about filling in the placeholders
    # or about this script; they make no sense once the values are filled in.
    lines = [
        line
        for line in TEMPLATE.read_text(encoding="utf-8").splitlines()
        if not (
            line.startswith("-- Replace <")
            or "generate_sql.py" in line
        )
    ]
    sql = "\n".join(lines).replace(ID_PLACEHOLDER, attendee_id)
    sql = sql.replace(BUCKET_PLACEHOLDER, bucket)

    OUTPUT.write_text(sql + "\n", encoding="utf-8")

    print("\n" + "=" * 70)
    print("Your SQL is saved in: schema/my_kafka_events.sql")
    print("Open that file in Bob's editor and copy the statements from there.")
    print("It has five parts, each labelled with the lab guide section that uses it.")
    print("Run Part 1 now (CREATE SCHEMA, CREATE TABLE, SELECT), one statement at a time.")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(1)
