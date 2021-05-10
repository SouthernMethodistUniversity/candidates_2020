require("lfs")
always_load("singularity")

local sif_hash      = 'f4b57eb40aaecb8f75c6d972ea382bbd9a884ad6cbecee22f2adba8f9555c70a'
local sif_directory = os.getenv("WORK") .. '/singularity_containers/'
local sif_file      = sif_directory .. 'itom_web_scraping_sha256.' .. sif_hash .. '.sif'

if not lfs.attributes(sif_file) then
  lfs.mkdir(sif_directory)
  local sif_download = 'singularity pull ' .. sif_file .. ' library://rkalescky/default/itom_web_scraping:sha256.' .. sif_hash
  capture(sif_download)
end

function build_command(app)
  local cmd_beginning = 'singularity exec --writable-tmpfs -B /scratch,/run/user '
  local cmd_ending    = sif_file .. ' xvfb-run -a '
  local sh_ending     = ' "$@"'
  local csh_ending    = ' $*'
  local sh_cmd        = cmd_beginning .. cmd_ending .. app .. sh_ending
  local csh_cmd       = cmd_beginning .. cmd_ending .. app .. csh_ending
  set_shell_function(app , sh_cmd, csh_cmd)
end

setenv("TMPDIR", "/dev/shm")

build_command('python3')
build_command('jupyter')

