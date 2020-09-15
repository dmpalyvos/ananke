COMMIT_HASH=$(git rev-parse --short HEAD)
PLOT_SCRIPT="plot.py"
OUTPUT_PATH="data/output"

countdown() {
  secs=$1
  shift
  msg=$@
  while [ $secs -gt 0 ]
  do
    printf "\r\033[KWaiting %.d seconds $msg" $((secs--))
    sleep 1
  done
  echo
}