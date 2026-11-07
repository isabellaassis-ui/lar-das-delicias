# Inicia o Lar das Delícias localmente: carrega o .env e sobe o servidor.
Set-Location $PSScriptRoot
if (-not (Test-Path .env)) { Write-Error "Arquivo .env não encontrado. Copie .env.example para .env e preencha."; exit 1 }
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#=\s]+)\s*=\s*(.*)$') { Set-Item -Path "Env:$($Matches[1])" -Value $Matches[2].Trim() }
}
Start-Process "http://127.0.0.1:8000"
& .\.venv\Scripts\python.exe -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
