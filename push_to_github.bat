@echo off
set "PATH=C:\Users\fe\AppData\Local\Microsoft\WinGet\Packages\Git.MinGit_Microsoft.Winget.Source_8wekyb3d8bbwe\cmd;C:\Users\fe\AppData\Local\Microsoft\WinGet\Packages\Git.MinGit_Microsoft.Winget.Source_8wekyb3d8bbwe\ucrt64\bin;%PATH%"
cd /d "C:\Users\fe\Desktop\de"
echo ==========================================================
echo Pushing RetailPulse to GitHub:
echo https://github.com/vaidehip844-ux/RETAILPULSE-DE-MINI-RPOJECT.git
echo ==========================================================
echo.
git push -u origin main
echo.
echo ==========================================================
echo If prompted above, authenticate with your GitHub account.
echo ==========================================================
pause
