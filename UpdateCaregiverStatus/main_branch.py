import pandas as pd
import asyncio
from post_branch import update_branch

semaphore = asyncio.Semaphore(5)


async def safe_update_branch(caregiver_code, branch_id):
    async with semaphore:
        return await update_branch(caregiver_code, branch_id)


async def main():
    df_caregivers = pd.read_csv(
        "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\Caregiver Codes for Branch Updates.csv")

    branch_id = '10106097'

    results = await asyncio.gather(
        *(safe_update_branch(caregiver_code, branch_id) for caregiver_code in
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
