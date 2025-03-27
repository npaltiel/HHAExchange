import pandas as pd
import asyncio
from post_status import update_status


async def main():
    df_caregivers = pd.read_csv(
        "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\CDPAP Admission IDs for Status Change.csv")

    status_id = 2

    results = await asyncio.gather(
        *(update_status(caregiver_code, status_id) for caregiver_code in
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

    for admission_id, error_message in failed_caregivers:
        print(f"Admission ID: {admission_id}, Error: {error_message}")


asyncio.run(main())
