import time
import os
import random
from faker import Faker

os.environ["SS_LOG_LEVEL"] = "warn"
from snowflake.ingest.streaming import StreamingIngestClient

fake = Faker()

# --- Configuration ---
BATCH_SIZE = 5
DEMO_MINUTES = 3
NUM_BATCHES = DEMO_MINUTES * 120  # 120 batches per minute (5 rows every 0.5s)
DATABASE = "SSV2_QUICKSTART_DB"
SCHEMA   = "SSV2_SCHEMA"

# Default auto-created pipe: <TABLE_NAME>-streaming (hyphen, not underscore)
PIPE     = "SSV2_QUICKSTART_USERS-streaming"

PROFILE_JSON_PATH = "profile.json"

# --- Initialize Streaming Client ---
print(f"Connecting to Snowflake...")
print(f"  Database: {DATABASE}")
print(f"  Schema:   {SCHEMA}")
print(f"  Pipe:     {PIPE} (default auto-created pipe)")

try:
    client = StreamingIngestClient(
        "SSV2_QUICKSTART_CLIENT",
        DATABASE,
        SCHEMA,
        PIPE,
        profile_json=PROFILE_JSON_PATH,
        properties=None,
    )
except Exception as e:
    print(f"\n[ERROR] Failed to create StreamingIngestClient:")
    print(f"  {e}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify profile.json exists and has correct values")
    print(f"  2. Check that rsa_key.p8 exists in the same directory")
    print(f"  3. Verify the public key is registered: DESC USER SSV2_DEMO_USER;")
    print(f"  4. Ensure network allows outbound HTTPS to Snowflake")
    raise SystemExit(1)

# --- Open channel ---
print(f"\nOpening channel...")
try:
    channel, status = client.open_channel("SSV2_QUICKSTART_CHANNEL")
    print(f"  Channel: {status.channel_name}")
    print(f"  Status:  {status.status_code}")
    print(f"  Latest committed offset: {status.latest_committed_offset_token}")
except Exception as e:
    print(f"\n[ERROR] Failed to open channel:")
    print(f"  {e}")
    print(f"\nTroubleshooting:")
    print(f"  1. Verify the table exists: SELECT * FROM {DATABASE}.{SCHEMA}.SSV2_QUICKSTART_USERS LIMIT 1;")
    print(f"  2. Check role has INSERT privilege on the table")
    print(f"  3. Verify database and schema names are correct")
    client.close()
    raise SystemExit(1)

# --- Stream fake user data in batches ---
total_rows = BATCH_SIZE * NUM_BATCHES
print(f"\nStreaming {total_rows} rows ({NUM_BATCHES} batches of {BATCH_SIZE}) over ~{DEMO_MINUTES} minute(s)...")
print(f"Watch your Streamlit dashboard to see data arrive in real-time!\n")
errors = []
row_id = 0
for batch in range(1, NUM_BATCHES + 1):
    for _ in range(BATCH_SIZE):
        row_id += 1
        row = {
            "user_id": row_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "address": fake.address().replace("\n", ", "),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "registration_date": fake.date_time_this_year().isoformat(),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "order_amount": round(random.uniform(1, 100), 2),
        }
        try:
            channel.append_row(row, offset_token=str(row_id))
        except Exception as e:
            errors.append((row_id, str(e)))
            if len(errors) >= 5:
                print(f"\n[ERROR] Too many row errors ({len(errors)}). Stopping.")
                break
    if len(errors) >= 5:
        break

    if batch % 60 == 0:
        print(f"  [producer] batch {batch}/{NUM_BATCHES} (row {row_id})")

    time.sleep(0.5)

if errors:
    print(f"\n[WARNING] {len(errors)} rows failed to send:")
    for offset, err in errors[:5]:
        print(f"  Row {offset}: {err}")

# --- Wait for all data to be committed ---
print(f"\n[producer] Waiting for commits to reach offset {total_rows}...")
try:
    channel.wait_for_commit(
        lambda token: token is not None and int(token) >= total_rows,
        timeout_seconds=120
    )
    print("All rows committed.")
except Exception as e:
    print(f"\n[WARNING] Commit wait timed out or failed: {e}")
    print("Some rows may still be in flight. Check the table directly.")

# --- Display channel status ---
s = channel.get_channel_status()
print(f"\nChannel status:")
print(f"  Channel:              {s.channel_name}")
print(f"  Committed offset:     {s.latest_committed_offset_token}")
print(f"  Rows inserted:        {s.rows_inserted_count}")
print(f"  Rows errored:         {s.rows_error_count}")
print(f"  Avg server latency:   {s.server_avg_processing_latency}")
print(f"  Last error message:   {s.last_error_message}")

# --- Cleanup ---
print(f"\nFinal committed offset: {channel.get_latest_committed_offset_token()}")
channel.close()
client.close()
print("\nDemo complete!")

