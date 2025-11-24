import sys

import edgar

import config
import fundamentals_service
import utils
from edgar import get_filings
if __name__ == "__main__":
    filing = edgar.get_by_accession_number('0000950170-22-021893')
    if hasattr(filing,'obj'):
        print(filing.obj())
    if hasattr(filing.obj(), 'financials'):
        print(True)

