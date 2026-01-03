# Nombre del entorno virtual
$envName = "venv"

Write-Host "Buscando o creando el entorno virtual 'envName' ..."

# Verificamos si el entorno ya existe
if (-not (Test-Path -Path $envName -PathType Container)){
    Write-Host "El entorno virtual 'envName' no existe. Creando ..."
    # Asumimos que python está instalado en el Path
    python -m venv $envName
    Write-Host "¡Creación completada!"
}

# Activamos el entorno virtual (con un comando de PowerShell)
Write-Host "Activando el entorno virtual 'envName' ..."

# El comando de powershell debe ejecutarse como un punto de origen
. "$envName/Scripts/Activate.ps1"

Write-Host "Entorno virtual activado, ¡Listo para tarabajar!"

