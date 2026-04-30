# Streamlit Community Cloud Deployment

This dashboard is read-only. It reads Google Sheets tabs and never writes back to Sheets from Streamlit Cloud.

## Before you deploy

1. Push the latest code to GitHub.
2. Confirm the repository is `priyankyi/ecommerce-automation-analysis`.
3. Make sure the dashboard app path is `src/dashboard/flipkart_streamlit_app.py`.
4. Share the Google Sheet with `streamlit-flipkart-dashboard@dn-data-487114.iam.gserviceaccount.com`.

## Create the app

1. Go to https://share.streamlit.io/
2. Sign in with GitHub.
3. Click `New app`.
4. Select the repo `priyankyi/ecommerce-automation-analysis`.
5. Select branch `main`.
6. Set the main file path to `src/dashboard/flipkart_streamlit_app.py`.
7. Choose Python `3.12` if Streamlit offers it. If not, use the default supported version.

## Add secrets

1. Open `Advanced settings`.
2. Add the Google Sheets service account secret using the same structure as `config/streamlit_secrets_template.toml`.
3. Add `MASTER_SPREADSHEET_ID` if you want to override the local metadata file.
4. Save the secrets.

## Finish setup

1. Make sure the Google Sheet is shared with the service account email.
2. Click `Deploy`.
3. Open the app URL after deployment finishes.
4. Invite team viewers inside Streamlit Community Cloud if you want shared access.

## Local vs cloud behavior

- Local launch:
  - `.\run_flipkart_dashboard.ps1`
- Cloud launch:
  - Streamlit Community Cloud URL after deployment
- Data refresh:
  - Run the normal local pipeline or quick refresh locally.
  - Updated tabs appear in Google Sheets.
  - The cloud dashboard reads the updated tabs automatically.
- Read-only rule:
  - The dashboard never writes to Google Sheets from Streamlit Cloud.

## Secret reminder

- Never commit a real `.streamlit/secrets.toml`.
- Use the Streamlit Cloud secrets UI instead.
