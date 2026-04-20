# Setup worker distribuido en Windows

Una sola batch installs + launch para correr un shard del downloader en cada PC Windows. Idempotente (se puede re-correr).

## Pre-requisitos

Antes de correr el `.bat`:

1. **Windows 10/11** con PowerShell / CMD.
2. **Credencial GCP** (`pdfs-uploader.json`) ya copiada a:
   ```
   %USERPROFILE%\onpe-eg2026-creds\pdfs-uploader.json
   ```
   (p.ej. `C:\Users\USERNAME\onpe-eg2026-creds\pdfs-uploader.json`)
3. **Conexion a Internet** residencial.

## Correr (como Administrador)

```cmd
REM Click derecho sobre setup-worker.bat -> Run as administrator
setup-worker.bat 1 3
```

El primer argumento es **M** (el indice de este worker, 0-based), el segundo es **N** (total de workers).

Ejemplo distribucion 3 workers:

| Maquina | Comando |
|---|---|
| Mac | `uv run python scripts/download_pdfs.py ... --shard 0/3` |
| Windows PC 1 | `setup-worker.bat 1 3` |
| Windows PC 2 | `setup-worker.bat 2 3` |

## Qué hace el bat

1. Verifica que se corre como Admin.
2. Valida que la credencial GCP existe en `%USERPROFILE%\onpe-eg2026-creds\`.
3. Instala `git` + `uv` via `winget` (idempotente).
4. Clona `https://github.com/f3r21/onpe-eg2026` en `%USERPROFILE%\onpe-eg2026`.
5. Corre `uv sync` (instala deps Python).
6. Copia la credencial a `credentials/` dentro del repo.
7. Setea `GOOGLE_APPLICATION_CREDENTIALS` permanente (`setx`).
8. Descarga los 2 parquets de curated desde `gs://onpe-eg2026-pdfs-v2/bootstrap/curated/`.
9. Desactiva suspend/hibernate/monitor-off mientras corre.
10. Lanza `download_pdfs.py` con `--shard M/N`.

## Parar

Ctrl+C en la ventana. El checkpoint se guarda en `data/state/pdfs_download_*.json` y al relanzar con los mismos `M/N` retoma desde donde quedó (via `skip_existing` en GCS).

## Reanudar

Simplemente re-corre el mismo `setup-worker.bat M N`. El `git pull` trae los ultimos cambios y `uv sync` actualiza deps si hubo cambios.

## Monitoreo

La ventana del CMD muestra `progreso X/Y (ok=... skip=... fail=...) @ Z/s` cada ~30s. Si necesitas logearlo a archivo:

```cmd
setup-worker.bat 1 3 > C:\temp\worker1.log 2>&1
```

## Desinstalar al terminar

```cmd
REM 1) Parar el worker (Ctrl+C)
REM 2) Revertir power profile (opcional)
powercfg /change standby-timeout-ac 30
powercfg /change hibernate-timeout-ac 30

REM 3) Remover env var
setx GOOGLE_APPLICATION_CREDENTIALS ""

REM 4) Borrar credencial (IMPORTANTE — no dejar secretos)
del /P "%USERPROFILE%\onpe-eg2026\credentials\pdfs-uploader.json"
del /P "%USERPROFILE%\onpe-eg2026-creds\pdfs-uploader.json"

REM 5) (Opcional) borrar el repo
rmdir /S /Q "%USERPROFILE%\onpe-eg2026"
```

Ademas, el admin del proyecto debe **revocar** la service account key cuando ya no sea necesaria:

```bash
gcloud iam service-accounts keys list \
  --iam-account=pdfs-uploader@onpe-eg2026-v2.iam.gserviceaccount.com

gcloud iam service-accounts keys delete <KEY_ID> \
  --iam-account=pdfs-uploader@onpe-eg2026-v2.iam.gserviceaccount.com
```
