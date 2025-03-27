import pandas as pd
import asyncio
from create_caregiver import create_caregiver


async def main():
    caregivers = pd.read_excel("C:\\Users\\nochum.paltiel\\Downloads\\Anchor Core Staff Demographics.xlsx")
    caregivers['DOB'] = pd.to_datetime(caregivers['DOB'], errors='coerce')
    caregivers['DOB'] = caregivers['DOB'].dt.strftime('%Y-%m-%d')

    caregivers_dict = caregivers.to_dict(orient='index')

    await asyncio.gather(
        *(create_caregiver(caregivers_dict[caregiver]) for caregiver in caregivers_dict)
    )


asyncio.run(main())
