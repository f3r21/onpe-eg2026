@echo off
REM =====================================================================
REM onpe-eg2026 — setup y launch del worker de descarga distribuida
REM
REM Prerequisito: copiar el archivo de credenciales GCP (pdfs-uploader.json)
REM a %USERPROFILE%\onpe-eg2026-creds\pdfs-uploader.json ANTES de correr.
REM
REM Correr como Administrador (click derecho -> Run as administrator).
REM
REM Uso:
REM   setup-worker.bat 1 3        Worker 1 de 3 shards totales
REM   setup-worker.bat 2 3        Worker 2 de 3 shards totales
REM =====================================================================

setlocal EnableDelayedExpansion
chcp 65001 >nul

REM ---------------------------------------------------------------------
REM 1) Verificar admin
REM ---------------------------------------------------------------------
net session >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Se requiere Administrador.
    echo Cerra esta ventana, click derecho en el .bat -^> Run as administrator.
    pause
    exit /b 1
)

REM ---------------------------------------------------------------------
REM 2) Validar parametros: shard M y N + bucket
REM ---------------------------------------------------------------------
set "SHARD_M=%~1"
set "SHARD_N=%~2"

if "%SHARD_M%"=="" set /p SHARD_M="Worker M (0-based, ej. 1): "
if "%SHARD_N%"=="" set /p SHARD_N="Total shards N (ej. 3): "

if "%SHARD_M%"=="" goto bad_shard
if "%SHARD_N%"=="" goto bad_shard

REM Nombre del bucket GCS privado donde viven los PDFs + bootstrap.
REM Configurable via variable GCS_BUCKET_NAME (sin prefijo gs://).
if "%GCS_BUCKET_NAME%"=="" set /p GCS_BUCKET_NAME="Nombre bucket GCS (sin gs://, ej. my-onpe-bucket): "
if "%GCS_BUCKET_NAME%"=="" (
    echo [ERROR] Se requiere nombre de bucket GCS.
    pause
    exit /b 1
)

echo.
echo [INFO] Shard asignado: %SHARD_M%/%SHARD_N%
echo [INFO] Bucket GCS: gs://%GCS_BUCKET_NAME%
echo.

REM ---------------------------------------------------------------------
REM 3) Validar credencial presente
REM ---------------------------------------------------------------------
set "CRED_SRC=%USERPROFILE%\onpe-eg2026-creds\pdfs-uploader.json"
if not exist "%CRED_SRC%" (
    echo [ERROR] Falta el archivo de credenciales:
    echo   %CRED_SRC%
    echo.
    echo Copiar el pdfs-uploader.json alli antes de correr este bat.
    echo Transferencia segura: USB cifrado, 1Password, o canal privado.
    pause
    exit /b 1
)
echo [OK] Credencial encontrada en %CRED_SRC%

REM ---------------------------------------------------------------------
REM 4) Instalar prereqs via winget (idempotente)
REM ---------------------------------------------------------------------
echo.
echo [INFO] Instalando git...
winget install -e --id Git.Git --silent --accept-package-agreements --accept-source-agreements --disable-interactivity 2>nul
if errorlevel 1 echo [INFO] git ya instalado o falla no-fatal
echo [INFO] Instalando uv (astral-sh)...
winget install -e --id astral-sh.uv --silent --accept-package-agreements --accept-source-agreements --disable-interactivity 2>nul
if errorlevel 1 echo [INFO] uv ya instalado o falla no-fatal

REM Refresh PATH en la sesion actual (winget no la propaga automatico)
for /f "tokens=2*" %%A in ('reg query "HKCU\Environment" /v Path 2^>nul ^| findstr "Path"') do set "USER_PATH=%%B"
for /f "tokens=2*" %%A in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Environment" /v Path 2^>nul ^| findstr "Path"') do set "SYS_PATH=%%B"
set "PATH=%SYS_PATH%;%USER_PATH%"

REM Fallback: uv.exe en %USERPROFILE%\.local\bin si PATH no se actualizo
set "UV_BIN=uv"
where uv >nul 2>&1
if errorlevel 1 (
    if exist "%USERPROFILE%\.local\bin\uv.exe" (
        set "UV_BIN=%USERPROFILE%\.local\bin\uv.exe"
        echo [INFO] usando uv en fallback path
    ) else (
        echo [ERROR] uv no encontrado en PATH ni en fallback.
        echo Cerrar esta ventana, abrir una nueva CMD Admin y re-ejecutar.
        pause
        exit /b 1
    )
)

REM ---------------------------------------------------------------------
REM 5) Clonar repo
REM ---------------------------------------------------------------------
set "REPO_DIR=%USERPROFILE%\onpe-eg2026"
if exist "%REPO_DIR%\.git" (
    echo [INFO] Repo ya clonado en %REPO_DIR%, actualizando
    pushd "%REPO_DIR%"
    git pull --ff-only
    popd
) else (
    echo [INFO] Clonando https://github.com/f3r21/onpe-eg2026.git
    git clone https://github.com/f3r21/onpe-eg2026.git "%REPO_DIR%"
    if errorlevel 1 (
        echo [ERROR] git clone fallo.
        pause
        exit /b 1
    )
)

cd /d "%REPO_DIR%"

REM ---------------------------------------------------------------------
REM 6) Instalar deps con uv
REM ---------------------------------------------------------------------
echo.
echo [INFO] uv sync (puede tardar 2-3 min la primera vez)...
%UV_BIN% sync
if errorlevel 1 (
    echo [ERROR] uv sync fallo.
    pause
    exit /b 1
)

REM ---------------------------------------------------------------------
REM 7) Copiar credencial al repo
REM ---------------------------------------------------------------------
if not exist "%REPO_DIR%\credentials" mkdir "%REPO_DIR%\credentials"
copy /Y "%CRED_SRC%" "%REPO_DIR%\credentials\pdfs-uploader.json" >nul
set "CRED_DEST=%REPO_DIR%\credentials\pdfs-uploader.json"
echo [OK] Credencial copiada a %CRED_DEST%

REM Setear GOOGLE_APPLICATION_CREDENTIALS permanente + sesion actual
setx GOOGLE_APPLICATION_CREDENTIALS "%CRED_DEST%" >nul
set "GOOGLE_APPLICATION_CREDENTIALS=%CRED_DEST%"

REM ---------------------------------------------------------------------
REM 8) Descargar curated parquets desde el bucket (bootstrap)
REM ---------------------------------------------------------------------
if not exist "%REPO_DIR%\data\curated" mkdir "%REPO_DIR%\data\curated"

echo.
echo [INFO] Descargando curated parquets desde gs://%GCS_BUCKET_NAME%/bootstrap/curated/
%UV_BIN% run python -c "import os; from google.cloud import storage; bucket=os.environ['GCS_BUCKET_NAME']; c=storage.Client(); b=c.bucket(bucket); [b.blob(f'bootstrap/curated/{f}').download_to_filename(f'data/curated/{f}') or print(f'  ok  {f}') for f in ['actas_cabecera.parquet','actas_archivos.parquet']]"
if errorlevel 1 (
    echo [ERROR] Fallo descargar parquets. Verificar credencial y red.
    pause
    exit /b 1
)
echo [OK] Curated parquets descargados

REM ---------------------------------------------------------------------
REM 9) Power profile: evitar suspend
REM ---------------------------------------------------------------------
echo.
echo [INFO] Desactivando suspend/hibernate mientras corre el downloader...
powercfg /change standby-timeout-ac 0 >nul 2>&1
powercfg /change hibernate-timeout-ac 0 >nul 2>&1
powercfg /change monitor-timeout-ac 0 >nul 2>&1

REM ---------------------------------------------------------------------
REM 10) Launch downloader
REM ---------------------------------------------------------------------
echo.
echo ==========================================================
echo   Arrancando worker shard %SHARD_M%/%SHARD_N%
echo   Rate: 5 PDFs/s * 10 concurrency
echo   Stop: Ctrl+C o cerrar ventana
echo   Resume: relanzar este .bat con los mismos M/N
echo ==========================================================
echo.

%UV_BIN% run python scripts\download_pdfs.py ^
    --gcs-bucket gs://%GCS_BUCKET_NAME% ^
    --rps 5 --concurrency 10 ^
    --shard %SHARD_M%/%SHARD_N%

echo.
echo [INFO] Downloader termino. Codigo de salida: %ERRORLEVEL%
pause
exit /b %ERRORLEVEL%

:bad_shard
echo [ERROR] Debes proveer SHARD_M y SHARD_N, ejemplos:
echo   setup-worker.bat 1 3       (worker 1 de 3)
echo   setup-worker.bat 2 3       (worker 2 de 3)
pause
exit /b 1
