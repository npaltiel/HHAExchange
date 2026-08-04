import pandas as pd
import asyncio
from post_demographics import update_demographics

semaphore = asyncio.Semaphore(5)


async def safe_update_demographics(caregiver_code):
    async with semaphore:
        return await update_demographics(caregiver_code)


async def main():
    df_caregivers = pd.read_csv(
        "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\Caregiver Codes for Discipline Updates.csv")

    results = await asyncio.gather(
        *(safe_update_demographics(row['Caregiver Code'])
          for _, row in df_caregivers.iterrows())
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

    df_failed = pd.DataFrame(failed_caregivers, columns=["Caregiver Code", "Error Message"])
    df_failed.to_csv(
        "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\Failures - Discipline Updates.csv",
        index=False)

    print("Error details written to failed_caregivers.csv")


asyncio.run(main())
