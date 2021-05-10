#!/bin/bash
#SBATCH -J kunovich_2020
#SBATCH -o kunovich_2020_%A_%a.out
#SBATCH --array=1-2076
#SBATCH -c 2
#SBATCH --mem=12G
#SBATCH -p standard-mem-s

module purge
module use ${SLURM_SUBMIT_DIR}/scraping_tools
module load environment

input="candidate_url.txt"
output="${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out"
save_directory="/scratch/group/oit_research_services/projects/001_kunovich_candidates/${SLURM_ARRAY_JOB_ID}"

id=${SLURM_ARRAY_TASK_ID}

for i in $(seq 1 10)
do 
  python3 candidates_collection.py $(head -${id} ${input} | tail -1) ${save_directory}
  if $(tail -1 ${output} | grep -q "https") && $(tail -1 ${output} | grep -q -v "fail")
  then
    size=$(ls -l ${save_directory}/$(tail -1 ${output} | cut -d, -f2).txz | cut -d' ' -f5)
    if (( ${size} >= 100000 ))
    then
      exit
    fi
  fi
  sleep 1m
done
echo "fail"

