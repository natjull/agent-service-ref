import os
os.environ.pop("CLAUDECODE", None)
# Clear dummy API key so the SDK falls back to Claude Max OAuth
if os.environ.get("ANTHROPIC_API_KEY") in ("", "dummy", None):
    os.environ.pop("ANTHROPIC_API_KEY", None)

from src.cli import main

raise SystemExit(main())
