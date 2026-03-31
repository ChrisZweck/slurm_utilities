#!/usr/bin/env bash
# Reports inefficient jobs that ran for over 1 hour and used more than 12 cores.
# Usage: ./slurm_inefficient.sh -a <account> [-s <YYYY-MM-DD>] [-e <YYYY-MM-DD>]
#
# Flags jobs where:
#   - Wall-time  > 1 hour
#   - Core count > 12
#   - CPU utilisation  < CPU_THRESHOLD  (default 50%)
#   - Mem utilisation  < MEM_THRESHOLD  (default 50%)

ACCOUNT=""
START_DATE=""
END_DATE=""
CPU_THRESHOLD=50
MEM_THRESHOLD=50
MIN_HOURS=1
MIN_CORES=12

while getopts ":a:s:e:" opt; do
    case $opt in
        a) ACCOUNT="$OPTARG"    ;;
        s) START_DATE="$OPTARG" ;;
        e) END_DATE="$OPTARG"   ;;
        *) echo "Usage: $0 -a <account> [-s <YYYY-MM-DD>] [-e <YYYY-MM-DD>]"; exit 1 ;;
    esac
done

[[ -z "$ACCOUNT" ]] && {
    echo "Usage: $0 -a <account> [-s <YYYY-MM-DD>] [-e <YYYY-MM-DD>]"
    exit 1
}

[[ -z "$START_DATE" ]] && START_DATE=$(date -d "1 month ago" +%Y-%m-%d 2>/dev/null \
                                       || date -v-1m +%Y-%m-%d)
[[ -z "$END_DATE" ]] && END_DATE=$(date +%Y-%m-%d)

echo "Fetching job list for account '$ACCOUNT' from $START_DATE to $END_DATE..."
echo "Filtering for jobs > ${MIN_HOURS}h wall-time and > ${MIN_CORES} cores"
echo "Flagging CPU < ${CPU_THRESHOLD}% or Mem < ${MEM_THRESHOLD}%"
echo ""

# Get job IDs and core counts from sacct
JOBDATA=$(sacct -a \
    --account="$ACCOUNT" \
    --starttime="${START_DATE}T00:00:00" \
    --endtime="${END_DATE}T23:59:59" \
    --state=COMPLETED,FAILED,TIMEOUT \
    --noheader \
    --parsable2 \
    --format="JobID,NCPUS" \
    | grep -v '\.' | grep -v '^$')

echo sacct -a \
    --account="$ACCOUNT" \
    --starttime="${START_DATE}T00:00:00" \
    --endtime="${END_DATE}T23:59:59" \
    --state=COMPLETED,FAILED,TIMEOUT \
    --noheader \
    --parsable2 \
    --format="JobID,NCPUS" 

[[ -z "$JOBDATA" ]] && { echo "No jobs found."; exit 0; }

TOTAL=$(echo "$JOBDATA" | wc -l)
echo "Found $TOTAL jobs. Running seff on each..."
echo ""

MIN_WALL_S=$(( MIN_HOURS * 3600 ))

flagged=""
count=0

while IFS='|' read -r jobid ncpus; do
    [[ -z "$jobid" ]] && continue
    count=$(( count + 1 ))
    printf "\r  Processing job %d / %d ..." "$count" "$TOTAL"

    # Skip jobs that don't meet the core threshold
    [[ "$ncpus" -le "$MIN_CORES" ]] 2>/dev/null && continue

    seff_out=$(seff "$jobid" 2>/dev/null) || continue

    # User
    user=$(sacct -j "$jobid" --noheader --parsable2 --format="User" \
           | grep -v '^$' | head -1)

    # Wall-time — format: "Job Wall-time:  1.1%  00:05:09 of 08:00:00 time limit"
    walltime=$(echo "$seff_out" | awk '/Job Wall-time/ {print $3}')
    wt="${walltime%%.*}"
    days=0
    if [[ "$wt" == *-* ]]; then days="${wt%%-*}"; wt="${wt#*-}"; fi
    colons="${wt//[^:]}"
    [[ ${#colons} -lt 2 ]] && wt="0:$wt"
    IFS=: read -r h m s <<< "$wt"
    wall_s=$(( days*86400 + 10#${h:-0}*3600 + 10#${m:-0}*60 + 10#${s:-0} ))

    # Skip jobs under the wall-time threshold
    [[ "$wall_s" -le "$MIN_WALL_S" ]] && continue

    # CPU utilisation — format: "CPU Utilisation: 86.7% ..."
    cpu_pct=$(echo "$seff_out" | awk '/CPU Utilisation/ {gsub(/%/,"",$3); print $3}')
    cpu_pct="${cpu_pct:-0}"

    # Mem utilisation — format: "Mem Utilisation:  86.7%  27.75 GB of 32.00 GB"
    mem_pct=$(echo "$seff_out" | awk '/Mem Utilisation/ {gsub(/%/,"",$3); print $3}')
    mem_pct="${mem_pct:-0}"
    mem_used=$(echo "$seff_out" | awk '/Mem Utilisation/ {print $4, $5}')
    mem_used="${mem_used:-N/A}"

    # Flag if either CPU or Mem is below threshold
    cpu_int="${cpu_pct%%.*}"
    mem_int="${mem_pct%%.*}"
    [[ "$cpu_int" -ge "$CPU_THRESHOLD" && "$mem_int" -ge "$MEM_THRESHOLD" ]] && continue

    # Mark which metrics are inefficient
    flags=""
    [[ "$cpu_int" -lt "$CPU_THRESHOLD" ]] && flags="CPU"
    [[ "$mem_int" -lt "$MEM_THRESHOLD" ]] && flags="${flags:+$flags+}Mem"

    flagged+="${jobid}|${user}|${ncpus}|${walltime}|${cpu_pct}|${mem_pct}|${mem_used}|${flags}"$'\n'

done <<< "$JOBDATA"

echo -e "\r  Done.                          \n"

if [[ -z "$flagged" ]]; then
    echo "No inefficient jobs found matching the criteria.  Last three:"
    lastthree=$(echo $JOBDATA  | awk -F'[| ]' '{print $(NF-5), $(NF-3), $(NF-1)}')
    for job in $lastthree
    do 
        seff $job
        echo
    done
    exit 0
fi

COUNT=$(echo "$flagged" | grep -c '.' || true)
echo "=== Inefficient Jobs (${COUNT} found) ==="
echo "Criteria: wall-time > ${MIN_HOURS}h, cores > ${MIN_CORES}, CPU or Mem < ${CPU_THRESHOLD}%"
echo ""
printf "%-12s %-12s %6s %12s %8s %8s %12s %10s\n" \
    "JobID" "User" "Cores" "Wall-time" "CPU%" "Mem%" "Mem Used" "Flags"
echo "--------------------------------------------------------------------------------"
echo "$flagged" | sort -t'|' -k4 -r | \
while IFS='|' read -r jobid user ncpus walltime cpu_pct mem_pct mem_used flags; do
    printf "%-12s %-12s %6s %12s %7s%% %7s%% %12s %10s\n" \
        "$jobid" "$user" "$ncpus" "$walltime" "$cpu_pct" "$mem_pct" "$mem_used" "$flags"
done
