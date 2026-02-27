#!/bin/bash
set -euo pipefail

ENV_NAME="s3_env"
containers=("P6_TEST_AUTOMATIZE")
DEST_BASE="/srv/contenedores/CONTENEDORES/CONTENEDORES"
SCRIPT_DIR="/home/aac/NoisePort_server"

MODULE_PATH="06_alarms_processing.main"


echo "============================================================"
echo "INITIALIZING CONDA FOR THIS SCRIPT"
echo "============================================================"
CONDA_BASE=$(conda info --base)
if [[ -f "${CONDA_BASE}/etc/profile.d/conda.sh" ]]; then
    source "${CONDA_BASE}/etc/profile.d/conda.sh"
else
    echo "Error: could not find conda.sh in $CONDA_BASE/etc/profile.d/"
    exit 1
fi
echo ""



echo "============================================================"
echo "SETING UP AND ACTIVATE CONDA ENV"
echo "============================================================"
#create a new conda environment with Python 3.9
if conda env list | grep -q "$ENV_NAME"; then
    echo "Conda environment '$ENV_NAME' already exists."
else
    echo "Environment '$ENV_NAME' doesnt exist."
fi

echo "Activating conda environment '$ENV_NAME'..."
conda activate $ENV_NAME

echo ""
echo "Activated conda environment '$ENV_NAME'."
echo ""





for container in "${containers[@]}"; do
     DEST_DIR="${DEST_BASE}/3-Medidas/${container}/acoustic_params"


     if [ ! -d "$DEST_DIR" ]; then
         echo "Directory $DEST_DIR does not exist. Creating..."
	 mkdir -p  "${DEST_DIR}"
         chown aac:aac "${DEST_DIR}"    
     else
	 echo "Changing directory to ${SCRIPT_DIR}..."
     fi

     pushd "${SCRIPT_DIR}" > /dev/null
    
     echo "Executing module ${MODULE_PATH}"
     python -m "${MODULE_PATH}" -f "/srv/contenedores/CONTENEDORES/CONTENEDORES/5-Resultados" --raspbery --port --point "${container}"
     echo "Finished executing module ${MODULE_PATH} at ${container}"

     popd > /dev/null
     
done


echo "=============================================================="
echo "SCRIPT COMPLTED SUCCESSFULLY"
echo "=============================================================="

conda deactivate

echo "End of script."
     






