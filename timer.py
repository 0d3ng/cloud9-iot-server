from datetime import datetime
from pytz import timezone
import time
for x in range(1000):
    print(str(x)+" -- "+str(round(datetime.now(timezone('Asia/Jakarta')).timestamp() * 1000)) )
    time.sleep(1)
