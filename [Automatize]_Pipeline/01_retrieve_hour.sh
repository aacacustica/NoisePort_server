#!/bin/bash
set -euo pipefail

#-----------------------------------------
# Configuration
#-----------------------------------------
ENV_NAME="s3_env"
SCRIPT_DIR="/home/aac/NoisePort_server"
MODULE_PATH="01_retrieve_data.retrieve_data"
ENVIRONMENT_PATH="/home/aac/NoisePort_server/environment.yml"

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



#-----------------------------------------
# Ensure conda is on PATH
#-----------------------------------------
 echo "============================================================"
 echo "CHECKING FOR CONDA IN PATH"
 echo "============================================================"
 if ! command -v conda &> /dev/null; then
     echo "'conda' not found in PATH. Adding common install locations..."
     export PATH="$HOME/miniconda3/bin:$HOME/anaconda3/bin:$PATH"
     if ! command -v conda &> /dev/null; then
         echo "Error: 'conda' still not found. Please install Conda or update the PATH."
         exit 1
     fi
 fi

 echo "Using conda from: $(command -v conda)"

 echo


#-----------------------------------------
# Initialize Conda
#-----------------------------------------
# echo "============================================================"
# echo "INITIALIZING CONDA FOR THIS SCRIPT"
# echo "============================================================"
# CONDA_BASE=$(conda info --base)
# if [[ -f "${CONDA_BASE}/etc/profile.d/conda.sh" ]]; then
#     # shellcheck source=/dev/null
#     source "${CONDA_BASE}/etc/profile.d/conda.sh"
# else
#     echo "Error: could not find conda.sh in ${CONDA_BASE}/etc/profile.d/"
#     exit 1
# fi

# echo



#-----------------------------------------
# Setup and activate Conda environment
#-----------------------------------------
 echo "============================================================"
 echo "SETTING UP AND ACTIVATING CONDA ENV"
 echo "============================================================"
 if conda env list | grep -q "^${ENV_NAME}[[:space:]]"; then
     echo "Conda environment '${ENV_NAME}' already exists."
 else
     echo "Creating Conda environment '${ENV_NAME}' with Python 3.9..."
     conda env create -f "${ENVIRONMENT_PATH}"
 fi

 echo "Activating Conda environment '${ENV_NAME}'..."
 conda activate "${ENV_NAME}"
 echo

# -----------------------------------------
# Retrieve data via Python module
# -----------------------------------------
echo "============================================================"
echo "RETRIEVING HOUR FROM WAV FILES"
echo "============================================================"
if [[ -d "${SCRIPT_DIR}" ]]; then
    echo "Changing directory to ${SCRIPT_DIR}..."
    pushd "${SCRIPT_DIR}" > /dev/null
else
    echo "Error: Directory ${SCRIPT_DIR} does not exist."
    exit 1
fi

echo "Executing module ${MODULE_PATH}..."
python3 -m ${MODULE_PATH}
echo "Finished executing module ${MODULE_PATH}."

echo
# Return to previous directory
popd > /dev/null

echo "============================================================"
echo "SCRIPT COMPLETED SUCCESSFULLY"
echo "============================================================"

#-----------------------------------------
# Deactivate Conda
#-----------------------------------------
conda deactivate


echo "End of script."
