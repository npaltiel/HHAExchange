import pandas as pd
import asyncio
from post_caregiver_rates import update_caregiver_rate

semaphore = asyncio.Semaphore(5)

INPUT_CSV = "C:\\Users\\nochu\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\Caregiver Rates to Update.csv"
FAILED_XLSX = "C:\\Users\\nochu\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\Failed_Rate_Updates.xlsx"


async def safe_update_rate(rate_info):
    async with semaphore:
        return await update_caregiver_rate(rate_info)


def build_rate_info(row):
    """Converts a CSV row into the dict shape expected by update_caregiver_rate.
    Blank/NaN DailyRate and VisitRate are sent as empty strings rather than 'nan'."""

    def clean(value):
        if pd.isna(value):
            return ""
        return value

    return {
        'CaregiverRateID': clean(row['CaregiverRateID']),
        'PatientID': clean(row['PatientID']),
        'FromDate': pd.to_datetime(row['FromDate']).strftime('%Y-%m-%d') if not pd.isna(row['FromDate']) else "",
        'ToDate': pd.to_datetime(row['ToDate']).strftime('%Y-%m-%d') if not pd.isna(row['ToDate']) else "",
        'HourlyRate': clean(row['HourlyRate']),
        'DailyRate': clean(row['DailyRate']),
        'VisitRate': clean(row['VisitRate']),
        'Status': clean(row['Status']),
    }


async def main():
    df_rates = pd.read_csv(INPUT_CSV)

    rate_infos = [build_rate_info(row) for _, row in df_rates.iterrows()]

    results = await asyncio.gather(
        *(safe_update_rate(rate_info) for rate_info in rate_infos)
    )

    success_count = sum(1 for _, success, _ in results if success)
    failed_rates = [(rate_id, error_message) for rate_id, success, error_message in results if not success]

    print(f"Successes: {success_count}")
    print(f"Total failures: {len(failed_rates)}")
    print("Failed Caregiver Rate IDs and Error Messages:")

    fail_ids = []
    fail_errors = []
    for rate_id, error_message in failed_rates:
        print(f"CaregiverRateID: {rate_id}, Error: {error_message}")
        fail_ids.append(rate_id)
        fail_errors.append(error_message)

    if fail_ids:
        failures = pd.DataFrame({
            'CaregiverRateID': fail_ids,
            'Error': fail_errors,
        })
        failures.to_excel(FAILED_XLSX, index=False, sheet_name='Sheet1')


asyncio.run(main())
