# taska
Code runner with typing-hint.

## Folder structure:

`WORK_DIR > Python > Venv > Workspace > Job`

### Demo:

- /work_dir
  > work_dir=`$WORK_DIR/$CWD`
  - /default_python (`executable=sys.executable`)
    - /venv1 (`requirements.txt < pip,six,morebuiltins`)
      - /workspaces/workspace1 (`code1.py, code2.py, package1/module.py`)
        - /jobs
          - /job1
            - /runner.py
            - /meta.json
              > cwd=/workspace1(`const`)\
              > python_path=/work_dir/default_python/venv1/bin/python.exe\
              > entrypoint=package1.module:function1\
              > params={"arg1": 1, "arg2": "str"}\
              > enable=1\
              > crontab=0 0 * * *\
              > mem_limit="1g"\
              > result_limit="15m"\
              > stdout_limit="10m"\
              > timeout=60
            - /job.pid(int)
              > 29238
            - /stdout.log
            - /result.log
              > {"start": "2024-07-14 23:30:57", "end": "2024-07-14 23:33:57", "result": 321}
          - /job2
            - /runner.py
            - /meta.json
              > cwd=/workspace1(`const`)\
              > python_path=/work_dir/default_python/venv1/bin/python.exe\
              > entrypoint=code1:function2\
              > params={}\
              > crontab=0 */5 * * *\
              > mem_limit="100m"\
              > result_limit="10m"\
              > stdout_limit="10m"\
              > timeout=10
            - /job.pid(int)
              > 32162
            - /stdout.log
            - /result.log
              > ({"start": "2024-07-14 23:30:57", "end": "2024-07-14 23:33:57", "result": 321}\n)
      - /workspaces/workspace2 (`code3.py`)
  - /default_python2 (`executable=/usr/bin/python3.11`)
    - /venv1 (`requirements.txt < requests,selectolax`)
