# OTP Limit Bot — Heroku package

## Deploy
1. Create a new Heroku app.
2. Set config vars (same keys as `.env.example`):
   ```
   BOT_TOKEN
   API_ID
   API_HASH
   OWNER_IDS
   OWNER_USERNAME
   FORCE_CHANNEL   (optional)
   FORCE_GROUP     (optional)
   ```
3. Deploy this folder (git push heroku main or Heroku CLI).
4. Scale the worker:
   ```bash
   heroku ps:scale worker=1
   ```
   (Do **not** scale a web dyno — this bot uses long polling.)

## Notes
- Free / Eco dynos sleep. For continuous code requests use a VPS package or a paid dyno.
- Same resilience as the VPS build: message delivery failures never kill background tasks.
