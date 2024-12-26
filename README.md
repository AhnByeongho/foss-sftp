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
  python -m venv env

  # activate venv by:
  env\Scripts\activate

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

### Package Management

- If you install a new package, make sure to update `requirements.txt``:
  ```bash
  python -m pip freeze > requirements.txt
  ```
