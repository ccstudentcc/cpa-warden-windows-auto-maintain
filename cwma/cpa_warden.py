from .apps.cpa_warden import *  # noqa: F401,F403


if __name__ == "__main__":
    from .apps.cpa_warden import main

    raise SystemExit(main())
