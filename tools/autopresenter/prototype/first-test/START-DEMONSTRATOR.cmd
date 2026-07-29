@echo off
setlocal
cd /d "%~dp0"
title KenigEvents Autopresenter - First Test
echo.
echo  AUTOPRESENTER FIRST TEST
echo  Preparing the Windows demonstrator...
echo.
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%~dp0bootstrap.ps1"
set "RESULT=%ERRORLEVEL%"
if not "%RESULT%"=="0" (
  echo.
  echo  START FAILED. See the error above.
  echo  Send the logs folder to the developer.
  pause
)
exit /b %RESULT%
