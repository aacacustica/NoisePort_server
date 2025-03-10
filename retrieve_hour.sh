#!/bin/bash
cd /home/aac_s3_test/noisePort_server
#source /home/aac_s3_test/miniconda3/etc/profile.d/conda.sh
conda activate s3_env
python3 -m 01_retrieve_data.retrieve_data
