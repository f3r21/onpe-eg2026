# Descarga distribuida de PDFs

Los PDFs escaneados de actas (~725k, ~1 TB) se pueden descargar en paralelo desde multiples maquinas para acelerar el cierre del universo. El patron usa **particiones disjuntas deterministas via md5(archivoId) mod N**, sin coordinacion central.

## Arquitectura

```
universo 725,782 archivoIds
         |
         +-- md5(aid) % N == 0  --> Worker 0 (Mac)
         +-- md5(aid) % N == 1  --> Worker 1 (Windows PC1)
         +-- md5(aid) % N == 2  --> Worker 2 (Windows PC2)

Cada worker:
  - Tiene checkpoint local independiente (no shared state)
  - Sube a mismo bucket GCS (skip_existing deduplica si hay overlap)
  - Usa su propia IP residencial (aumenta rate efectivo vs CloudFront)
```

## Performance esperada

| Setup | Rate combinado | ETA sobre 506k pending |
|---|---:|---:|
| 1 Mac | 3.6/s | 39h |
| Mac + 1 Windows | ~7/s | 20h |
| Mac + 2 Windows | ~10-11/s | 13h |

El rate teorico maximo contra ONPE es 15 rps total (CloudFront ceiling). Con 3 workers a 4-5 rps cada uno se queda holgado.

## Setup en Mac (ya corriendo con shard 0/3)

Si tu Mac ya estaba descargando sin shard, tenes 2 opciones:

**A) Dejarlo terminar sin particionar** (seguir tal cual): es lo mas simple, no necesitas parar ni reiniciar.

**B) Migrar a shard 0/3** (recomendado si queres paralelizar ya): matar el proceso actual y relanzar con `--shard 0/3`.

```bash
# Identificar PID del downloader actual
ps aux | grep download_pdfs | grep -v grep

# Killear
kill <PID>  # SIGTERM, graceful

# Relanzar con shard
cd /path/to/onpe-eg2026
nohup caffeinate -dims uv run python scripts/download_pdfs.py \
  --gcs-bucket gs://<tu-bucket> --rps 5 --concurrency 10 \
  --shard 0/3 > logs/pdfs_shard0.log 2>&1 &!
```

El checkpoint viejo queda y `skip_existing` contra GCS previene re-descargas.

## Setup en Windows (por maquina, ~15-20 min)

### 1. Instalar herramientas

```powershell
# PowerShell con admin
winget install --id Git.Git -e
winget install --id astral-sh.uv -e
winget install --id Python.Python.3.12 -e
winget install --id Google.CloudSDK -e
```

### 2. Clonar repo y sync deps

```powershell
cd $HOME
git clone https://github.com/f3r21/onpe-eg2026
cd onpe-eg2026
uv sync
```

### 3. Autenticar GCP (misma cuenta que el Mac)

```powershell
gcloud auth login
gcloud config set project <tu-proyecto-gcp>
gcloud auth application-default login
```

### 4. Copiar curated/ desde el Mac (necesario para generar la lista de archivoIds)

Opcion simple: scp desde Mac a Windows.

```bash
# En el Mac:
scp -r data/curated user@windows-pc:onpe-eg2026/data/
scp -r data/state user@windows-pc:onpe-eg2026/data/
```

Alternativa: download del bucket GCS (si tenes los parquets ahi):

```powershell
gsutil -m cp -r gs://<tu-bucket>/curated .\data\
```

Solo se necesita `data/curated/actas_cabecera.parquet` y `data/curated/actas_archivos.parquet` (~30 MB combinados).

### 5. Correr el downloader

```powershell
# Worker 1/3
uv run python scripts\download_pdfs.py `
  --gcs-bucket gs://<tu-bucket> `
  --rps 5 --concurrency 10 `
  --shard 1/3
```

Para Worker 2/3 en el otro Windows PC, cambiar a `--shard 2/3`.

### 6. Opcional: mantener despierto el PC

```powershell
# En Windows, powercfg evita suspend mientras corre el script
powercfg /requestsoverride PROCESS python.exe SYSTEM DISPLAY
```

Al terminar, revertir con `powercfg /requestsoverride PROCESS python.exe`.

## Monitoreo desde el Mac

Cada worker escribe progreso a su stdout/log local. Para ver totales combinados, verificar conteo directo en GCS:

```bash
# Cuenta total de PDFs en el bucket
gsutil ls -r gs://<tu-bucket>/eg2026/**/*.pdf | wc -l

# Verificar completitud vs universo esperado
uv run python -c "
import polars as pl
total = pl.scan_parquet('data/curated/actas_archivos.parquet') \
    .select(pl.col('archivoId').n_unique()).collect().item()
print(f'universo: {total:,}')
"
```

## Failure modes y resiliencia

- **Worker crashea mid-download**: relanzar con los mismos flags. `skip_existing=True` verifica blobs existentes antes de re-subir. Sin perdida de progreso.
- **Shard dispar** (uno va mas lento que otros): al final, si queda un shard sin terminar, killear los otros y relanzarlos con `--shard N/M` reducido. O sea, si 2/3 termino pero 1/3 va lento, el Mac puede tomar el mismo `--shard 1/3` y ayudar (skip_existing evita duplicados).
- **CloudFront rate-limit escala**: reducir `--rps` en cada worker (ej. 3 en vez de 5). Fails quedan en el checkpoint; al ceder la presion, relanzar.
- **GCS auth expira**: `gcloud auth application-default login` de nuevo.

## Parar todos los workers

```bash
# En cada maquina:
ps aux | grep download_pdfs | grep -v grep | awk '{print $2}' | xargs kill
```

El estado queda consistente (todos los uploads completados hasta ese punto estan en GCS).
