@echo off
title Meerkat's Observatory
cd /d "%~dp0"

REM --- GitHub auto-sync (offline-safe: silently skip on failure) ---
echo [Sync] Updating from GitHub...
powershell -ExecutionPolicy Bypass -NoProfile -Command "$ErrorActionPreference='Continue'; try { $hdr=@{'User-Agent'='mo-bootstrap'}; $tree=(Invoke-WebRequest -Uri 'https://api.github.com/repos/akmoonge/meerkat-observatory/git/trees/main?recursive=1' -UseBasicParsing -TimeoutSec 10 -Headers $hdr).Content | ConvertFrom-Json; $upd=0; foreach ($f in $tree.tree) { if ($f.type -ne 'blob') { continue }; try { $tmp=[System.IO.Path]::GetTempFileName(); Invoke-WebRequest -Uri ('https://raw.githubusercontent.com/akmoonge/meerkat-observatory/main/'+$f.path) -OutFile $tmp -UseBasicParsing -TimeoutSec 10 -Headers $hdr; $changed=$true; if (Test-Path $f.path) { $h1=(Get-FileHash -Path $f.path -Algorithm MD5).Hash; $h2=(Get-FileHash -Path $tmp -Algorithm MD5).Hash; if ($h1 -eq $h2) { $changed=$false } }; if ($changed) { $dir=Split-Path -Parent $f.path; if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }; Move-Item -Path $tmp -Destination $f.path -Force; Write-Host ('  upd: '+$f.path); $upd++ } else { Remove-Item $tmp -Force -ErrorAction SilentlyContinue } } catch { Write-Host ('  skip: '+$f.path) } }; if ($upd -eq 0) { Write-Host '[Sync] up to date.' } else { Write-Host ('[Sync] '+$upd+' file(s) updated.') } } catch { Write-Host '[Sync] offline or GitHub unreachable - running with local files.' }"

echo.
python -m streamlit run meerkat_observatory.py
pause
