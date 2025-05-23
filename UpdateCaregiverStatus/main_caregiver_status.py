import pandas as pd
import asyncio
from post_status import update_status

semaphore = asyncio.Semaphore(5)


async def safe_update_status(caregiver_code, status_id):
    async with semaphore:
        return await update_status(caregiver_code, status_id)


async def main():
    df_caregivers = pd.read_csv(
        "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\CDPAP Admission IDs for Status Change.csv")

    status_id = 4

    results = await asyncio.gather(
        *(safe_update_status(caregiver_code, status_id) for caregiver_code in
          df_caregivers['Caregiver Code'])
    )

    # Count successes and collect failure codes
    first_success_count = sum(1 for _, success, _ in results if success)
    # second_success_count = sum(1 for _, success, _ in results2 if success)
    failed_caregivers = [(admission_id, error_message) for admission_id, success, error_message in results if
                         not success]

    # Output results
    print(f"Initial successes: {first_success_count}")
    # print(f"Secondary successes: {second_success_count}")
    print(f"Total failures: {len(failed_caregivers)}")
    print("Failed Caregiver Codes and Error Messages:")

    fail = []
    for admission_id, error_message in failed_caregivers:
        print(f"Admission ID: {admission_id}, Error: {error_message}")
        fail.append(admission_id)

    failures = pd.DataFrame()
    failures['Admission ID'] = fail
    excel_file = f'C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\Failed_CDPAP_Status_Change.xlsx'
    failures.to_excel(excel_file, index=False, sheet_name='Sheet1')


asyncio.run(main())
