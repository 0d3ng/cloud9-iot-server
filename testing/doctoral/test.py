import pandas as pd
import sys

l = (pd.DataFrame(columns=['NULL'],
                  index=pd.date_range('2021-12-24T17:30:00Z', '2021-12-24T18:00:00Z',
                                      freq='1T'))
       .index.strftime('%Y-%m-%dT%H:%M')
       .tolist()
)
print(l)
sys.stdout.flush()