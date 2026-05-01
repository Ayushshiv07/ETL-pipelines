Write-Host "🚀 Starting E-commerce ETL Dashboard Command Center..." -ForegroundColor Cyan
Write-Host "-----------------------------------------------------"

if (Test-Path ".\venv\Scripts\Activate.ps1") {
    & ".\venv\Scripts\Activate.ps1"
    streamlit run app.py
} else {
    Write-Host "❌ Error: Virtual environment 'venv' not found. Please run 'python -m venv venv' first." -ForegroundColor Red
}
pause
