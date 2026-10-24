# CyberStrike server

The online backend for **[CyberStrike: Tactical Ops](https://github.com/fiercepug/Cyberstrike)**, a free CS2-style FPS that runs in the browser.

**▶ Play: [cyberstrike2-byfelix.netlify.app](https://cyberstrike2-byfelix.netlify.app/)**

## What it does

- **Lobbies:** public and private lobbies with join codes for 1v1 and casual matches. It relays messages between players, and the lobby host runs the match.
- **Accounts:** sign up and log in, with saved stats, level and rank.
- **Leaderboards:** the top players by mode.
- **Anti-cheat:** it rejects impossible stat reports, flags repeat offenders and lets hosts kick cheaters.

## Run it locally

```bash
npm install
npm start
```

The server listens on `PORT` (default from `server.js`). Set `ADMIN_KEY` to enable the admin endpoints.

## Deploy

This repo is deployed on [Render](https://render.com) as a Node web service. Pushing to `main` redeploys it automatically.
