# OpenPOTD

OpenPOTD is a Discord bot for running PoTW/POTD style competitions with:

- scheduled posting,
- multi-guild support,
- subproblems with marks,
- automatic integer checking,
- manual marking workflows with review threads and buttons,
- rankings and roles.

## Requirements

- Python `3.11+` (CI runs on `3.11` and `3.13`)
- A Discord bot token
- Bot invited with slash-command + message permissions

## Local Setup

1. Initialize files:

```sh
# Windows
init.bat

# Linux/macOS
./init.sh
```

2. Create/use a virtual environment, then install dependencies:

```sh
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# Linux/macOS
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

3. Configure `config/config.yml`:
   - add your Discord user ID to `authorised`,
   - set `allowed_guild_id` to blank, one guild ID, or a list of guild IDs,
   - set `posting_time` to `HH:MM` (or leave blank to disable auto-posting),
   - keep `allow_local_db_reset: false` outside local testing.

4. Provide token:
   - local: put token in `config/token.txt`, or
   - env var: set `DISCORD_TOKEN` (or the name configured in `token_env_var`).

5. Start:

```sh
python openpotd.py
```

## First-Time In-Server Setup

Run these (slash commands recommended) in each guild:

1. `/init`
2. `/potd_channel <channel>`
3. Optional `/subproblem_thread_channel <text-or-forum-channel>`
4. Optional `/submission_channel <channel>` (manual submissions mirrored to staff)
5. Optional `/submission_ping_role <role>`
6. Optional `/ping_role <role>` (role pinged when posting problems)
7. Optional `/auto_publish_news true|false`

Then create season + problems:

1. `/newseason <name>`
2. `/start_season <season_id>`
3. `/add ...` for main problems
4. `/add_subproblem ...` and `/link_subimg ...` for subproblems

Post immediately with:

- `/post` (today’s scheduled problem),
- `/post_problem problem_id:<id>` (specific problem, also creates subproblem threads).

## Submission/Marking Model

- If a problem has subproblems, users can submit by DM text or `/submit` (interactive picker).
- Subproblem with `answer` set: auto integer check.
- Subproblem without `answer`: manual review flow.
- Manual flow creates mirrored staff review entries with `Claim / Correct / Incorrect` buttons.
- Review state persists across bot restarts.

## Discord Permissions Checklist

Minimum practical permissions in posting/review channels:

- `View Channel`
- `Send Messages`
- `Embed Links`
- `Attach Files` (if using images)
- `Create Public Threads` + `Send Messages in Threads` (for subproblem/review threads)
- `Manage Messages` (only needed for auto-publish in Announcement channels)

If posting fails with `Missing Permissions (50013)`, check that channel-level overrides did not remove one of these.

## Deployment (Railway)

Use this repository as a Railway service. `Procfile` is included:

```txt
worker: python -u openpotd.py
```

## Tests

```sh
python -m unittest discover -s tests -p "test_*.py" -v
python -m compileall -q openpotd.py shared.py cogs tests
```

CI runs automatically on push/PR via `.github/workflows/ci.yml`.
