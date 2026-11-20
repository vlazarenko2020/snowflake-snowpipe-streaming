# Snowpipe Streaming (High-Performance Architecture) Demo

A small end-to-end demo of Snowflake's **Snowpipe Streaming high-performance architecture (SSv2)**. A Python producer streams synthetic user/order records into a Snowflake table in near real time, while a Streamlit dashboard shows the data arriving live.

## How it works

```
ssv2_demo.py  ──(Snowpipe Streaming SDK, key-pair auth)──▶  SSV2_QUICKSTART_USERS  ──▶  streamlit_app.py
 Faker data                default pipe: <TABLE>-streaming        landing table           live monitor
```

| File | Purpose |
|------|---------|
| [1-Create-snowflake-Objects-git-version.sql](1-Create-snowflake-Objects-git-version.sql) | Creates the database, schema, landing table, demo role and demo user, and grants. Contains placeholders to fill in. |
| [ssv2_demo.py](ssv2_demo.py) | Producer. Streams 5 fake rows every 0.5 s for ~3 minutes (~1,800 rows), waits for commit, and prints channel status. |
| [streamlit_app.py](streamlit_app.py) | Streamlit-in-Snowflake dashboard that refreshes every 2 s: totals, recent records, cumulative revenue, top countries. |
| [profile-git-version.json](profile-git-version.json) | Template for the connection profile. Copy to `profile.json`. |
| [SSv2-AI-Webinar/](SSv2-AI-Webinar/) | AI-assistant skills that automate the same setup for short-lived demos. See its README. |

## Prerequisites

- A Snowflake account and a role that can create databases, roles and users (e.g. `ACCOUNTADMIN`, or `SYSADMIN` + `USERADMIN`)
- A warehouse you can grant usage on
- Python 3.14+ and [uv](https://docs.astral.sh/uv/)
- OpenSSL, to generate the key pair

## Setup

**1. Install dependencies**

```bash
uv sync
```

**2. Generate an RSA key pair** (unencrypted, for demo use only)

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -inform PEM -out rsa_key.p8
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

**3. Create the Snowflake objects**

Copy the SQL template to a local file, replace `<YOUR_PUBLIC_KEY_BODY>` (the contents of `rsa_key.pub` without the header/footer lines), `<YOUR_WAREHOUSE>` and `<YOUR_CURRENT_ROLE>`, then run it in a Snowflake worksheet.

```bash
cp 1-Create-snowflake-Objects-git-version.sql 1-Create-snowflake-Objects.sql
```

**4. Create the connection profile**

```bash
cp profile-git-version.json profile.json
```

Edit `profile.json`:

```json
{
    "account": "<ACCOUNT_IDENTIFIER>",
    "user": "SSV2_DEMO_USER",
    "url": "https://<ACCOUNT_IDENTIFIER>.snowflakecomputing.com:443",
    "private_key_file": "rsa_key.p8",
    "role": "SSV2_DEMO_ROLE"
}
```

Check the exact field names against the template. For org-based or PrivateLink accounts the URL format differs, for example `https://<org>-<account>.snowflakecomputing.com:443`.

**5. Deploy the dashboard**

Create a Streamlit app in Snowsight and paste in [streamlit_app.py](streamlit_app.py). It uses the active Snowflake session, so no credentials are needed.

## Run

```bash
uv run ssv2_demo.py
```

With the dashboard open, you'll see the row count and revenue climb as batches commit. Tune `BATCH_SIZE` and `DEMO_MINUTES` at the top of [ssv2_demo.py](ssv2_demo.py) to change the load.

## Security notes

- `rsa_key.p8` is **unencrypted**. Use an encrypted key or a secrets manager outside of demos.
- `rsa_key.p8`, `rsa_key.pub`, `profile.json` and the filled-in SQL file are listed in `.gitignore`. Never commit them.
- Drop the demo user, role and database when you're done:

```sql
DROP USER IF EXISTS SSV2_DEMO_USER;
DROP ROLE IF EXISTS SSV2_DEMO_ROLE;
DROP DATABASE IF EXISTS SSV2_QUICKSTART_DB;
```

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Client creation fails | `profile.json` values are correct and `rsa_key.p8` exists at the configured path |
| Authentication error | The public key is registered: `DESC USER SSV2_DEMO_USER;` |
| Channel open fails | The table exists and `SSV2_DEMO_ROLE` owns it or has INSERT on it |
| Connection refused | The account URL format matches your account type |
