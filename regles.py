import sys
import os
import pandas as pd
import re
import datetime
from datetime import datetime, timedelta
import numpy as np

def v_today1(df):
    today = datetime.now()
    def check_dates(row):
        for col in ['DateOfBirth', 'DateofDeath']:
            if pd.to_datetime(row[col], errors='coerce') > today:
                return False
        return True

    df = df[df.apply(check_dates, axis=1)]

    return df

df = pd.read_csv(sys.stdin)
df = v_today1(df)

df.to_csv(sys.stdout, index=False)