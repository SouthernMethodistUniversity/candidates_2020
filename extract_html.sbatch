#!/bin/bash
#SBATCH -J extract_html
#SBATCH -o extract_html.out
#SBATCH -p amd
#SBATCH --nodes=1
#SBATCH --tasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=500G

module purge
module use /users/rkalescky/testing/users/kbnk/01_amazon_etsy_web_scrapping/ITOM-handmade/scraping_tools
module load environment

python3 extract_html.py $((${SLURM_CPUS_PER_TASK}/2)) $((128*1024)) #${SLURM_MEM_PER_NODE}

