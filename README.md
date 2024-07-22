# taska
Code runner with typing-hint. Less is more, simple is the best.

## Folder structure:

`WORK_DIR > Python > Venv > Workspace > Job`

### Demo:

- /root_dir
  > root_dir=`$WORK_DIR/$CWD`
  - /runner.py
  - /pids/
  - /default_python
    - python_path(`sys.executable`)
    - /venv1
      - requirements.md5
      - requirements.txt
        - morebuiltins
      - /workspaces/workspace1 (`code1.py, code2.py, package1/module.py`)
        - > `sys.path.insert(0, workspace1)`
        - /jobs
          - /job1
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
  - /default_python2
    - python_path(`executable=/usr/bin/python3.11`)
    - /venv2
      - requirements.md5
      - requirements.txt
        - requests
        - selectolax
