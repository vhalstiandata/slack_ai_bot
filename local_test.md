# Local Testing Guide

This guide explains how to run the Finance AI Bot notebook locally without Slack.

## Prerequisites

- Python 3.x installed
- VS Code or PyCharm installed
- Access to the `finance-ai-bot-headway` GCP project (ask the bot owner to grant you access)

## 1. Get GCP access

You need a Google account that has been granted access to the project. Once granted, log in by running in Terminal:

```bash
gcloud auth application-default login
```

A browser window will open — log in with your Google account. That's it, no extra config needed.

> If you don't have `gcloud` installed: https://cloud.google.com/sdk/docs/install

## 2. Set up the environment

In Terminal, navigate to the project folder and run:

```bash
./start.sh
```

This will automatically:
- Verify your GCP credentials
- Create a Python virtual environment (`.venv`)
- Install all dependencies

You only need to do this once. Re-running is safe.

## 3. Open the notebook

Open `notebooks/dev_chat.ipynb` in VS Code or PyCharm.

**Select the kernel:** when prompted, choose the `.venv` interpreter from the project folder.

- VS Code: click the kernel selector in the top-right corner → **Select Another Kernel** → **Python Environments** → choose `.venv`
- PyCharm: go to **Settings → Python Interpreter** → add the `.venv` from the project folder

## 4. Run the notebook

Run the cells from top to bottom. The first cell imports the bot — you should see:

```
✅ run_analysis imported successfully
```

Then use either:
- **Single query cell** — edit the message and run the cell
- **Interactive chat loop** — run the cell, type messages in the input box, type `exit` to stop

## Troubleshooting

**`gcloud: command not found`** — install Google Cloud SDK: https://cloud.google.com/sdk/docs/install

**`403 Permission denied` on BigQuery or Vertex AI** — your Google account hasn't been granted access yet. Contact the bot owner.

**Kernel not showing `.venv`** — make sure you ran `./start.sh` first, then reload VS Code/PyCharm.
