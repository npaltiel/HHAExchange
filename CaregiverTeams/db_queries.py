import os
from datetime import datetime, timedelta

import numpy as np
import pyodbc
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DB_CONN = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=anchorHealthdb.cw5ezu5fyhr7.us-east-1.rds.amazonaws.com;"
    "DATABASE=AnchorHealthDB;"
    "UID=AnchorHealthUser;"
    f"PWD={os.environ['SOURCE_PASSWORD']};"
    "TrustServerCertificate=yes;"
)

REPLICA_CONN = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=AnchorHealthDB;"
    "UID=sa;"
    f"PWD={os.environ['SQL_PASSWORD']};"
    "TrustServerCertificate=yes;"
)

OFFICE_ID = 2365


def get_caregivers():
    query = """
    SELECT
        c.CaregiverID,
        c.OfficeAideCode                AS CaregiverCode,
        c.Status,
        ISNULL(c.BranchName, '')        AS Branch,
        c.HireDate,
        c.RehireDate,
        c.FirstDayWorked                AS FirstWorkDate,
        c.LastDayWorked                 AS LastWorkDate,
        ISNULL(ct.CaregiverTeam, '')    AS Team
    FROM dbo.Caregivers c
    LEFT JOIN dbo.CaregiverTeams ct
        ON c.TeamID = ct.CaregiverTeamID
    WHERE c.OfficeID = ?
      AND c.EmployeeType = 'E'
    """
    with pyodbc.connect(DB_CONN) as conn:
        df = pd.read_sql(query, conn, params=[OFFICE_ID])

    df['HireDate']      = pd.to_datetime(df['HireDate'])
    df['RehireDate']    = pd.to_datetime(df['RehireDate'])
    df['FirstWorkDate'] = pd.to_datetime(df['FirstWorkDate'])
    df['LastWorkDate']  = pd.to_datetime(df['LastWorkDate'])
    return df


def get_discipline_notes():
    """
    Returns non-Final disciplinary notes within the last 90 days for office 2365.
    Finals are tracked separately via sync_finals() / get_finals().
    Queries dbo.CaregiverNotesDisciplinary on the local replica (faster, pre-filtered).
    """
    query = """
    SELECT
        c.OfficeAideCode    AS CaregiverCode,
        cn.NoteDate         AS Date,
        cn.Subject
    FROM dbo.CaregiverNotesDisciplinary cn
    JOIN dbo.Caregivers c
        ON cn.CaregiverID = c.CaregiverID
    WHERE c.OfficeID = ?
      AND cn.Subject != 'Disciplinary Final'
      AND cn.NoteDate >= DATEADD(day, -90, GETDATE())
    """
    with pyodbc.connect(REPLICA_CONN) as conn:
        df = pd.read_sql(query, conn, params=[OFFICE_ID])

    df['Date'] = pd.to_datetime(df['Date'], format='mixed')
    return df


def sync_finals():
    """
    Refreshes dbo.CaregiverFinals for the replica's coverage window.
    - Deletes all dated rows within the coverage window (handles changed/deleted/switched Finals).
    - Re-inserts current Finals from CaregiverNotesDisciplinary with their NoteID.
    - Historical NULL-date rows are never touched.
    """
    with pyodbc.connect(REPLICA_CONN) as conn:
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute("SELECT MIN(NoteDate) FROM dbo.CaregiverNotesDisciplinary WHERE Subject = 'Disciplinary Final'")
        min_date = cur.fetchone()[0]
        if min_date is None:
            return
        min_date = min_date.date()

        cur.execute("""
            DELETE FROM dbo.CaregiverFinals
            WHERE NoteDate IS NOT NULL AND NoteDate >= ?
        """, min_date)

        cur.execute("""
            INSERT INTO dbo.CaregiverFinals (CaregiverCode, NoteDate, NoteID)
            SELECT c.OfficeAideCode, cn.NoteDate, cn.CaregiverNoteID
            FROM dbo.CaregiverNotesDisciplinary cn
            JOIN dbo.Caregivers c ON cn.CaregiverID = c.CaregiverID
            WHERE cn.Subject = 'Disciplinary Final'
        """)

        conn.commit()


def get_finals():
    """Returns the set of CaregiverCodes with a permanent Disciplinary Final."""
    with pyodbc.connect(REPLICA_CONN) as conn:
        cur = conn.cursor()
        cur.execute("SELECT CaregiverCode FROM dbo.CaregiverFinals")
        return {row[0] for row in cur.fetchall()}


def compute_team_changes(df_caregivers, df_notes, finals_codes):
    today = datetime.today()
    df_notes = df_notes.rename(columns={'Date': 'Discipline Date'})
    conditions = [
        df_notes['Subject'] == 'Disciplinary Action',
        df_notes['Subject'] == 'Disciplinary Verbal',
        df_notes['Subject'] == 'Disciplinary Written 1',
        df_notes['Subject'] == 'Disciplinary Written 2',
    ]
    choices = [
        df_notes['Discipline Date'] + pd.Timedelta(days=30),
        df_notes['Discipline Date'] + pd.Timedelta(days=30),
        df_notes['Discipline Date'] + pd.Timedelta(days=60),
        df_notes['Discipline Date'] + pd.Timedelta(days=90),
    ]
    df_notes['Expiry Date'] = np.select(conditions, choices, default=pd.NaT)
    df_notes_active = df_notes.dropna(subset=['Expiry Date'])
    if not df_notes_active.empty:
        idx = df_notes_active.groupby('CaregiverCode')['Expiry Date'].idxmax()
        latest_expiry = df_notes.loc[idx, ['CaregiverCode', 'Discipline Date', 'Expiry Date']].reset_index(drop=True)
        latest_expiry['Expiry Date'] = pd.to_datetime(latest_expiry['Expiry Date']).dt.date
    else:
        latest_expiry = pd.DataFrame(columns=['CaregiverCode', 'Discipline Date', 'Expiry Date'])

    df = pd.merge(df_caregivers, latest_expiry, on='CaregiverCode', how='left')
    df['EffectiveHireDate'] = df[['HireDate', 'RehireDate']].max(axis=1)

    not_disciplinary = (
        (~df['CaregiverCode'].isin(finals_codes))
        & (
            (df['Expiry Date'].isnull() & (df['LastWorkDate'] > (today - timedelta(days=90))))
            | (df['Expiry Date'].isnull() & (df['Team'] != 'Tier 2'))
            | ((df['Expiry Date'] <= today.date()) & (df['LastWorkDate'] > df['Discipline Date']))
        )
    )
    df['Disciplinary'] = ~not_disciplinary

    def probation_start(row):
        if pd.isnull(row['FirstWorkDate']):
            return today
        if pd.isnull(row['EffectiveHireDate']):
            return row['FirstWorkDate']
        if row['FirstWorkDate'] >= row['EffectiveHireDate']:
            return row['FirstWorkDate']
        if row['LastWorkDate'] >= row['EffectiveHireDate']:
            return row['EffectiveHireDate']
        return today

    df['ProbationStartDate'] = df.apply(probation_start, axis=1)

    make_probation = df[
        (df['Status'] == 'Active') & (df['Team'] != 'Probation') &
        (~df['Disciplinary']) & (df['ProbationStartDate'].dt.date >= (today - timedelta(days=30)).date())
    ].copy().reset_index(drop=True)

    make_tier1 = df[
        (df['Status'] == 'Active') & (df['Team'].isin(['Probation', ''])) &
        (df['ProbationStartDate'] < (today - timedelta(days=30))) & (~df['Disciplinary'])
    ].copy().reset_index(drop=True)

    make_tier2 = df[
        (df['Team'] != 'Tier 2') & df['Disciplinary']
    ].copy().reset_index(drop=True)

    back_to_1 = df[
        (df['Status'] == 'Active') & (df['Team'] == 'Tier 2') &
        (~df['Disciplinary']) & (~df['CaregiverCode'].isin(finals_codes))
    ].copy().reset_index(drop=True)

    return make_probation, make_tier1, make_tier2, back_to_1