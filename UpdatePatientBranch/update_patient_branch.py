import pandas as pd
import asyncio
from HHAExchange.get_requests import get_branches
from post_branch import update_branch

semaphore = asyncio.Semaphore(5)


async def safe_update_branch(admission_id, branch_id):
    async with semaphore:
        print(admission_id)
        return await update_branch(admission_id, branch_id)


async def main():
    df_patients = pd.read_csv(
        "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\ACD Transfer List.csv")

    branches_dict = await get_branches()
    # Gather async tasks for team updates

    results = await asyncio.gather(
        *(safe_update_branch(admission_id, branches_dict['ACD TRANSFER']) for admission_id in
          df_patients['Admission ID'])
    )

    # Count successes and collect failure codes
    first_success_count = sum(1 for _, success, _ in results if success)
    # second_success_count = sum(1 for _, success, _ in results2 if success)
    failed_patients = [(admission_id, error_message) for admission_id, success, error_message in results if
                       not success]

    # Output results
    print(f"Initial successes: {first_success_count}")
    # print(f"Secondary successes: {second_success_count}")
    print(f"Total failures: {len(failed_patients)}")
    print("Failed Caregiver Codes and Error Messages:")

    for admission_id, error_message in failed_patients:
        print(f"Admission ID: {admission_id}, Error: {error_message}")

    if failed_patients:
        df_failed = pd.DataFrame(failed_patients, columns=['Admission ID', 'Error Message'])
        df_failed.to_csv(
            "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\Update Patient Branch Failures.csv",
            index=False
        )
        print("Failures written to CSV.")


# Run the asynchronous main function
asyncio.run(main())
