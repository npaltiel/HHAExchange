import pandas as pd
import asyncio
from post_medicals import update_medical, create_medical, process_flu_medical, process_update_only_medical

semaphore = asyncio.Semaphore(3)


# async def safe_update_medical(caregiver_code, medical_id, date_performed, result_id):
#     async with semaphore:
#         return await update_medical(caregiver_code, medical_id, date_performed, result_id)
#
#
# async def safe_create_medical(caregiver_code, medical_id, date_performed, result_id):
#     async with semaphore:
#         return await create_medical(caregiver_code, medical_id, date_performed, result_id)


async def safe_process_flu_medical(caregiver_code, medical_id, date_performed, result_id):
    async with semaphore:
        return await process_flu_medical(caregiver_code, medical_id, date_performed, result_id)


async def safe_process_update_only_medical(caregiver_code, medical_id, date_performed, result_id):
    async with semaphore:
        return await process_update_only_medical(caregiver_code, medical_id, date_performed, result_id)


result_map = {
    'Completed (In Office)': '86358',
    'Completed (Elsewhere)': '86359',  # Flu Vaccine
    'Exempt': '86360',
    'Declined': '86361',
    'Negative': '86379',  # TB Screen
    'Completed': '86350'  # Annual Health Assessment
}

medical_processor_map = {
    '75560': safe_process_flu_medical,  # Flu Vaccine
    '75556': safe_process_update_only_medical,  # Annual Health Assessment
    '75569': safe_process_update_only_medical,  # TB Screen
}


async def main():
    df_caregivers = pd.read_csv(
        "C:\\Users\\nochum.paltiel\\OneDrive - Anchor Home Health care\\Documents\\Exchange API Updates\\Caregiver Codes for Medicals Update.csv")
    df_caregivers = df_caregivers.dropna(subset=['Medical ID'])
    df_caregivers['Medical ID'] = df_caregivers['Medical ID'].astype(int).astype(str)

    tasks = []

    for _, row in df_caregivers.iterrows():
        caregiver_code = row['Caregiver Code']
        date_performed = row['Date Performed']
        result_text = row['Result']
        medical_id = row['Medical ID']

        result_id = result_map.get(result_text)
        processor = medical_processor_map.get(medical_id)

        if not result_id:
            print(f"Skipping {caregiver_code}: unknown result '{result_text}'")
            continue

        if not processor:
            print(f"Skipping {caregiver_code}: no processor configured for Medical ID {medical_id}")
            continue

        tasks.append(
            processor(
                caregiver_code,
                medical_id,
                date_performed,
                result_id
            )
        )

    results = await asyncio.gather(*tasks)

    failure_rows = []
    for caregiver_code, success, error_message in results:
        if not success:
            failure_rows.append({
                "Caregiver Code": caregiver_code,
                "Error Message": error_message
            })

    if failure_rows:
        failures_df = pd.DataFrame(failure_rows)

        failures_df.to_csv(
            r"C:\Users\nochum.paltiel\OneDrive - Anchor Home Health care\Documents\Exchange API Updates\Medical_Update_Failures.csv",
            index=False
        )

        print(f"Saved {len(failure_rows)} failures to CSV")
    else:
        print("No failures 🎉")


asyncio.run(main())
