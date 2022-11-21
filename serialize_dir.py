import os
import sys
import fnmatch
import re

exclude = ["__pycache__", "build", ".pytest_cache", "*.egg-info", ".eggs"]


def should_exclude(path):
    is_dir = any((fnmatch.fnmatch(path, f"*/{e}/*") for e in exclude))
    is_subdir = any((fnmatch.fnmatch(path, f"*/{e}") for e in exclude))
    return is_dir or is_subdir


def generating_command(path):
    cmd = f"mkdir -p {os.path.dirname(path)}"
    cmd += "\n"
    with open(path) as f:
        cmd += 'echo -e "'
        for line in f:
            escaped = re.sub("\\\\", "\\\\\\\\", line)
            escaped = re.sub('"', '\\"', escaped)
            escaped = re.sub("`", "\\`", escaped)
            cmd += escaped
        cmd += f'" > {path}'
    return cmd


to_serialize = []
for (path, dirs, files) in os.walk(sys.argv[1]):
    if should_exclude(path) or not files:
        continue
    to_serialize += [os.path.join(path, f) for f in files]

for path in to_serialize:
    print(generating_command(path))
