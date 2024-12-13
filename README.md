## 포스증권 SP_BATCH_FEED_FOSSEXCEPTION_V2 -> main.py

### Prerequisites

- Local Python Version: `3.11.*`
  ```bash
  # check installed python version on your local machine
  python --version
  # Python 3.11.*
  ```

### Virtual Environment

- Create & activate Virtual Environment

  ```bash
  # using required python version, create venv
  python3.11 -m venv env

  # now you created venv at the root of nudge dir,
  # activate venv by:
  source env/bin/activate

  # to check if everything is setup correctly, run
  which python
  # /Users/.../foss-sftp/env/bin/python

  which pip
  # /Users/.../foss-sftp/env/bin/pip

  python --version
  # Python 3.11.*

  pip --version
  # pip 23.*.* from /Users/.../foss-sftp/env/lib/python3.11/site-packages/pip (python 3.11)
  ```

### Dependencies

- Install Dependencies

  ```bash
  pip install -r requirements.txt
  ```

### Batch

- Batch configuration

  ```bash
  # 1. Set up the batch
  crontab -e
  # Enter the command
  10 08 * * * /bin/bash /Users/mac/work/foss-sftp/batch_script.sh
  # This cron job is scheduled to run at 8:10 AM every day.

  # 2. Check if it is properly set up
  crontab -l
  ```

### Package Management

- Installing a new package

  ```bash
  # 1. Make sure to have a proper 'pip'
  which pip
  # better-backend/apps/nudge/env/bin/pip

  # 2. Install a new dependency, i.e.: pandas
  pip install pandas
  ```

- If you install a new package, make sure to update `requirements.txt``:
  ```bash
  python -m pip freeze > requirements.txt
  ```
