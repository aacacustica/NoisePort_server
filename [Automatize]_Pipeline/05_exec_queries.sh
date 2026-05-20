#!/bin/bash
set -euo pipefail

ENV_NAME="s3_env"
DEST_BASE="/srv/services/inbox"
SCRIPT_DIR="/home/aac/I+D/CODIGOS/NoisePort_server"

MODULE_PATH="04_queries.queries_server"


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

echo "============================================================"
echo "LAUNCHING QUERIES MODULE"
echo "============================================================"


 pushd "${SCRIPT_DIR}" > /dev/null

 echo "Executing module ${MODULE_PATH}"

 #check if there are sonometer files to process , if they exist , add sonometer processing param 
 
 dir_sonometro="${DEST_BASE}/sonometer_files"

 if [ -z "$(echo "$dir_sonometro"/*)" ] || [ ! -e "$dir_sonometro"/* ]; then
	python -m "${MODULE_PATH}" -t 
 else
	python -m "${MODULE_PATH}" -t -s
 fi
 
 echo "Finished executing module ${MODULE_PATH}"

     popd > /dev/null
     



echo "=============================================================="
echo "SCRIPT COMPLETED SUCCESSFULLY"
echo "=============================================================="

conda deactivate

echo "End of script."
     






