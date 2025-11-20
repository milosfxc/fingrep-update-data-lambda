import sys

import edgar

import config
import fundamentals_service
import utils
from edgar import get_filings
if __name__ == "__main__":
    #fundamentals_service.get_filing_details('0000950170-22-021893', 1, 4)
    x = 6
    try:
        x = x + None
    except Exception as e:
        print('Error')
        sys.exit()
    print(x)
