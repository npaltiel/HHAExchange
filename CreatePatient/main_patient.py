import pandas as pd
import asyncio
from create_patient import create_patient


async def main():
    patients = pd.read_excel("C:\\Users\\nochum.paltiel\\Downloads\\Anchor Core Client Demographics.xlsx")
    patients['DOB'] = pd.to_datetime(patients['DOB'], errors='coerce')
    patients['DOB'] = patients['DOB'].dt.strftime('%Y-%m-%d')

    patients_dict = patients.to_dict(orient='index')

    await asyncio.gather(
        *(create_patient(patients_dict[patient]) for patient in patients_dict)
    )


asyncio.run(main())
