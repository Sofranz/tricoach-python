# TriCoach Python Backend

Python Flask backend for the TriCoach app — handles Garmin Connect authentication and FIT file downloads.

## Deployment

This repo auto-deploys to the Oracle VCN VM via GitHub Actions. Push to `main` and the workflow handles the rest.

### Manual deploy (if needed)

```bash
scp main.py ubuntu@145.241.211.101:~/tricoach-backend/
scp requirements.txt ubuntu@145.241.211.101:~/tricoach-backend/
ssh ubuntu@145.241.211.101 "cd ~/tricoach-backend && venv/bin/pip install -r requirements.txt && sudo systemctl restart tricoach-backend"
```

## Service management

```bash
# Check status
ssh ubuntu@145.241.211.101 "sudo systemctl status tricoach-backend"

# View logs
ssh ubuntu@145.241.211.101 "sudo journalctl -u tricoach-backend -f"

# Restart manually
ssh ubuntu@145.241.211.101 "sudo systemctl restart tricoach-backend"
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/check_rate_limit` | Diagnose Garmin rate limiting |
| POST | `/test_connection` | Test Garmin credentials |
| POST | `/activities` | List Garmin activities |
| POST | `/activity/<id>/fit` | Download FIT file |
