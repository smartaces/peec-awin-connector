# cell_00_pip_installs.py — Install runtime dependencies
# Run this cell once per kernel session.

import subprocess, sys
subprocess.check_call([
    sys.executable, "-m", "pip", "install", "--quiet",
    "requests", "pandas", "python-dotenv", "ipywidgets",
])
print("\u2705 Dependencies installed.")
