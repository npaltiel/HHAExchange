"""
CaregiverTeams/main.py
Automated caregiver team assignment script.

Reads caregiver data and discipline notes from AnchorHealthDB,
computes required team changes, and updates HHAExchange via SOAP API.

Run: python -m CaregiverTeams.main   (from repo root)
Schedule via Windows Task Scheduler pointing to this file.
"""

import asyncio
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from get_requests import get_teams
from CaregiverTeams.db_queries import get_caregivers, get_discipline_notes, sync_finals, get_finals, compute_team_changes
from CaregiverTeams.post_team import update_team


async def run_updates(make_probation, make_tier1, make_tier2, back_to_1, teams_dict):
    prob_id  = teams_dict['Probation']
    tier1_id = teams_dict['Tier 1']
    tier2_id = teams_dict['Tier 2']

    tasks = [
        *(update_team(r.CaregiverID, r.CaregiverCode, prob_id)                    for _, r in make_probation.iterrows()),
        *(update_team(r.CaregiverID, r.CaregiverCode, tier1_id, add_hcss=True)    for _, r in make_tier1.iterrows()),
        *(update_team(r.CaregiverID, r.CaregiverCode, tier1_id)                   for _, r in back_to_1.iterrows()),
        *(update_team(r.CaregiverID, r.CaregiverCode, tier2_id)                   for _, r in make_tier2.iterrows()),
    ]
    return await asyncio.gather(*tasks)


async def main():
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] Loading data from AnchorHealthDB...")
    df_caregivers = get_caregivers()
    df_notes      = get_discipline_notes()
    print(f"  Caregivers: {len(df_caregivers)}  |  Notes: {len(df_notes)}")

    print("  Syncing Finals table...")
    sync_finals()
    finals_codes = get_finals()
    print(f"  Finals: {len(finals_codes)}")

    make_probation, make_tier1, make_tier2, back_to_1 = compute_team_changes(df_caregivers, df_notes, finals_codes)
    print(
        f"  → Probation: {len(make_probation)}  "
        f"Tier 1: {len(make_tier1)}  "
        f"Tier 2: {len(make_tier2)}  "
        f"Back→1: {len(back_to_1)}"
    )

    if not (len(make_probation) + len(make_tier1) + len(make_tier2) + len(back_to_1)):
        print("No team changes needed.")
        return

    print("Fetching team IDs from HHAExchange API...")
    teams_dict = await get_teams()

    print("Sending updates (pass 1)...")
    results = await run_updates(make_probation, make_tier1, make_tier2, back_to_1, teams_dict)

    failures  = [(code, err) for code, success, err in results if not success]
    pass1_ok  = sum(1 for _, success, _ in results if success)
    print(f"  Pass 1: {pass1_ok} succeeded, {len(failures)} failed")

    results2  = []
    pass2_ok  = 0
    still_failed = []

    if failures:
        failed_codes = {code for code, _ in failures}
        retry_prob   = make_probation[make_probation['CaregiverCode'].isin(failed_codes)]
        retry_tier1  = make_tier1[make_tier1['CaregiverCode'].isin(failed_codes)]
        retry_tier2  = make_tier2[make_tier2['CaregiverCode'].isin(failed_codes)]
        retry_back1  = back_to_1[back_to_1['CaregiverCode'].isin(failed_codes)]

        print(f"Retrying {len(failures)} failures...")
        results2     = await run_updates(retry_prob, retry_tier1, retry_tier2, retry_back1, teams_dict)
        pass2_ok     = sum(1 for _, success, _ in results2 if success)
        still_failed = [(code, err) for code, success, err in results2 if not success]
        print(f"  Pass 2: {pass2_ok} succeeded, {len(still_failed)} still failed")

        for code, err in still_failed:
            print(f"    FAILED  {code}: {err}")

    print(
        f"\n[{datetime.now():%Y-%m-%d %H:%M}] Done — "
        f"total successes: {pass1_ok + pass2_ok}, "
        f"total failures: {len(still_failed)}"
    )


if __name__ == '__main__':
    asyncio.run(main())