@echo off
REM Wrapper for running the bot from Windows Task Scheduler.
REM
REM NOT currently registered as a scheduled task — the bot is run manually for
REM now. When you do schedule it, a wrapper is far easier to debug than raw
REM schtasks arguments, and it pins the working directory.
REM
REM Task settings that matter when that day comes:
REM   - Trigger daily around midday LOCAL time. The idempotency gate keys on the
REM     UTC date, so a trigger near local midnight straddles the UTC boundary and
REM     DST shifts it twice a year — giving either a double-fire or a skipped day.
REM   - "Run task as soon as possible after a scheduled start is missed": ON
REM     (this PC is not always on). It is a duplicate-run generator, which is why
REM     the idempotency gate must stay fail-closed.
REM   - "Repeat task every N minutes": OFF.
REM   - "Stop the task if it runs longer than": 1 hour.
REM
REM Drop --live once DRY_RUN=false is set in .env, or keep it explicit here.

cd /d "%~dp0"

if exist ".venv\Scripts\activate.bat" call ".venv\Scripts\activate.bat"

python bybit_bot.py %*
echo [%date% %time%] exit code: %ERRORLEVEL% >> log\scheduler.log
exit /b %ERRORLEVEL%
