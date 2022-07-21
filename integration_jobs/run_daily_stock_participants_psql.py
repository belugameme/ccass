import os
import subprocess
import logging
from pathlib import Path

start = 0
end = 5365
step = 100

for i in range(start, end, step):
    logging.warning(f"kedro run node for stock list index range between {i} and {i+step-1}")
    cmd=f"kedro run --node=get_stocks_participants_psqltable_concurrent --params=modular_pipeline.start:{i},modular_pipeline.end:{i+step-1}"
    return_code=subprocess.run(cmd, shell=True,cwd=Path.cwd()).returncode