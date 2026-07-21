"""
Dry run — prints bucket counts and samples without making any API calls.
Run from repo root: python -m CaregiverTeams.dry_run
"""
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from CaregiverTeams.db_queries import get_caregivers, get_discipline_notes, sync_finals, get_finals, compute_team_changes


print(f"[{datetime.now():%Y-%m-%d %H:%M}] Loading data...")
df_caregivers = get_caregivers()
df_notes      = get_discipline_notes()
print(f"  Caregivers : {len(df_caregivers)}")
print(f"  Notes      : {len(df_notes)}")

print("  Syncing Finals table...")
sync_finals()
finals_codes = get_finals()
print(f"  Finals     : {len(finals_codes)}")

make_probation, make_tier1, make_tier2, back_to_1 = compute_team_changes(df_caregivers, df_notes, finals_codes)

print()
print("=" * 50)
print(f"  make_probation : {len(make_probation)}")
print(f"  make_tier1     : {len(make_tier1)}")
print(f"  make_tier2     : {len(make_tier2)}")
print(f"  back_to_1      : {len(back_to_1)}")
print("=" * 50)


def show_probation(label, df):
    cols = ['CaregiverCode', 'Status', 'Team', 'HireDate', 'RehireDate', 'FirstWorkDate', 'LastWorkDate', 'ProbationStartDate']
    df = df.copy()
    df['EffHire'] = df[['HireDate', 'RehireDate']].max(axis=1)
    cols2 = ['CaregiverCode', 'Status', 'Team', 'HireDate', 'RehireDate', 'EffHire', 'FirstWorkDate', 'LastWorkDate', 'ProbationStartDate']
    print(f"\n-- {label} ({len(df)}) --")
    print(df[cols2].to_string(index=False))

def show_discipline(label, df):
    cols = ['CaregiverCode', 'Status', 'Team', 'LastWorkDate', 'Discipline Date', 'Expiry Date']
    present = [c for c in cols if c in df.columns]
    print(f"\n-- {label} ({len(df)}) --")
    print(df[present].to_string(index=False))


show_probation("make_probation -> Probation", make_probation)
show_probation("make_tier1 -> Tier 1",        make_tier1)
show_discipline("make_tier2 -> Tier 2",        make_tier2)
show_discipline("back_to_1 -> Tier 1",         back_to_1)