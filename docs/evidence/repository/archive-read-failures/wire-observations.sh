#!/bin/bash
# FIX B wire investigation: RustFS 1.0.0-beta.11-preview.1, one byte flipped in a part file.
set -u
W=/tmp/claude-1000/-work/02f7e92e-c625-5244-a36f-6346fd0651a6/scratchpad/fixb
IMG=rustfs/rustfs:1.0.0-beta.11-preview.1
TAG=pm-followups-fixb-$RANDOM
VOL=$TAG-vol
CT=$TAG-rustfs
AK=fixb-access; SK=fixb-secret-$RANDOM
export AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
log() { echo "[$(date -u +%H:%M:%S)] $*"; }
run() { log "\$ $*"; "$@"; echo "   exit=$?"; }
cleanup() { log cleanup; docker rm -f $CT >/dev/null 2>&1; docker volume rm -f $VOL >/dev/null 2>&1; }
trap cleanup EXIT
run docker volume create $VOL
run docker run -d --name $CT -p 127.0.0.1:0:9000 -v $VOL:/data -e RUSTFS_VOLUMES=/data -e RUSTFS_ADDRESS=:9000 -e RUSTFS_CONSOLE_ENABLE=false -e RUSTFS_ACCESS_KEY=$AK -e RUSTFS_SECRET_KEY=<redacted> $IMG /data
PORT=$(docker port $CT 9000/tcp | head -1 | sed 's/.*://'); EP=http://127.0.0.1:$PORT; log "endpoint $EP"
for i in $(seq 1 60); do curl -sf $EP/health >/dev/null && break; sleep 0.5; done; [ -n "${VSMALL:-}" ] && ready
run curl -s -o /dev/null -w 'health %{http_code}\n' $EP/health
B=fixb-bucket
run aws --endpoint-url $EP s3api create-bucket --bucket $B
run aws --endpoint-url $EP s3api put-bucket-versioning --bucket $B --versioning-configuration Status=Enabled
head -c 1048576 /dev/urandom > $W/big.bin; head -c 2048 /dev/urandom > $W/small.bin
sha256sum $W/big.bin $W/small.bin
log "put big (1 MiB) and small (2 KiB)"
aws --endpoint-url $EP s3api put-object --bucket $B --key dir/big.bin --body $W/big.bin --content-type application/octet-stream > $W/put-big.json; cat $W/put-big.json
aws --endpoint-url $EP s3api put-object --bucket $B --key dir/small.bin --body $W/small.bin > $W/put-small.json; cat $W/put-small.json
VBIG=$(jq -r .VersionId $W/put-big.json); VSMALL=$(jq -r .VersionId $W/put-small.json)
SIG=(--aws-sigv4 aws:amz:us-east-1:s3 --user $AK:$SK)
ready() { for i in $(seq 1 120); do c=$(curl -s -o /dev/null -w '%{http_code}' "${SIG[@]}" -I "$EP/$B/dir/small.bin?versionId=$VSMALL"); [ "$c" = 200 ] && { log "ready after $i probes (signed HEAD small -> 200)"; return 0; }; sleep 0.5; done; log "NOT READY: last code $c"; return 1; }
log "=== baseline GET big by version id (curl, direct HTTP client)"
curl -sv "${SIG[@]}" -o $W/baseline-big.bin -w 'code=%{http_code} size_download=%{size_download} exit=%{exitcode}\n' "$EP/$B/dir/big.bin?versionId=$VBIG" 2>&1 | grep -v "^> \|^{ \|^} \|^\* *Trying\|TCP_NODELAY\|^\*  \|Expire\|schannel\|^\* Server auth" ; echo "curl exit=$?"
sha256sum $W/baseline-big.bin
log "=== baseline HEAD big"
curl -sI "${SIG[@]}" "$EP/$B/dir/big.bin?versionId=$VBIG"
log "=== volume layout"
docker run --rm -v $VOL:/data --entrypoint sh $IMG -c "find /data/$B -type f -exec ls -l {} +"
log "=== stop, flip one byte at offset 4096 of the big part file, restart"
run docker stop $CT
docker run --rm -v $VOL:/data --entrypoint sh $IMG -c "set -e; cd /data/$B/dir/big.bin; f=\$(find . -type f -size +524288c | head -1); echo part=\$f; sha256sum \$f; v=\$(dd if=\$f bs=1 skip=4096 count=1 status=none | od -An -tu1 | tr -d ' '); n=\$(( (v ^ 1) & 255 )); printf \"\$(printf '\\\\%03o' \$n)\" | dd of=\$f bs=1 seek=4096 conv=notrunc status=none; sha256sum \$f; echo before=\$v after=\$n; ls -l \$f"
run docker start $CT
PORT=$(docker port $CT 9000/tcp | head -1 | sed "s/.*://"); EP=http://127.0.0.1:$PORT; log "endpoint after restart $EP"
ready
for i in $(seq 1 60); do curl -sf $EP/health >/dev/null && break; sleep 0.5; done; [ -n "${VSMALL:-}" ] && ready
log "=== damaged: HEAD big"
curl -sI "${SIG[@]}" "$EP/$B/dir/big.bin?versionId=$VBIG"; echo "curl exit=$?"
log "=== damaged: GET big by version id, full trace of the response side"
curl -v "${SIG[@]}" -o $W/damaged-big.bin -w 'code=%{http_code} size_download=%{size_download} size_header=%{size_header}\n' "$EP/$B/dir/big.bin?versionId=$VBIG" 2>$W/damaged-get.err; echo "curl exit=$?"
grep -v "^> \|^{ \|^} \|^\* *Trying\|^\*  \|Expire\|^\* Server auth" $W/damaged-get.err
ls -l $W/damaged-big.bin; echo "bytes delivered: $(stat -c %s $W/damaged-big.bin)"
log "=== damaged: second GET (latest, no version id)"
curl -sv "${SIG[@]}" -o $W/damaged-big2.bin -w 'code=%{http_code} size_download=%{size_download}\n' "$EP/$B/dir/big.bin" 2>&1 | grep "^< \|transfer closed\|Recv failure\|code=\|curl:" ; echo "curl exit=${PIPESTATUS[0]}"
log "=== damaged: third GET by version id"
curl -sv "${SIG[@]}" -o $W/damaged-big3.bin -w 'code=%{http_code} size_download=%{size_download}\n' "$EP/$B/dir/big.bin?versionId=$VBIG" 2>&1 | grep "^< \|transfer closed\|Recv failure\|code=\|curl:" ; echo "curl exit=${PIPESTATUS[0]}"
log "=== damaged: Range 0-1023 (before the flipped byte)"
curl -sv "${SIG[@]}" -H "Range: bytes=0-1023" -o $W/range1.bin -w 'code=%{http_code} size_download=%{size_download}\n' "$EP/$B/dir/big.bin?versionId=$VBIG" 2>&1 | grep "^< \|transfer closed\|Recv failure\|code=\|curl:"; echo "curl exit=${PIPESTATUS[0]}"; cmp -n 1024 $W/range1.bin $W/big.bin && echo "range1 bytes equal original prefix"
log "=== damaged: Range 4000-5000 (across the flipped byte)"
curl -sv "${SIG[@]}" -H "Range: bytes=4000-5000" -o $W/range2.bin -w 'code=%{http_code} size_download=%{size_download}\n' "$EP/$B/dir/big.bin?versionId=$VBIG" 2>&1 | grep "^< \|transfer closed\|Recv failure\|code=\|curl:"; echo "curl exit=${PIPESTATUS[0]}"; cmp <(tail -c +4001 $W/big.bin | head -c 1001) $W/range2.bin && echo "range2 bytes equal original" || echo "range2 differs from original"
log "=== damaged: GET with x-amz-checksum-mode: ENABLED"
curl -sv "${SIG[@]}" -H "x-amz-checksum-mode: ENABLED" -o /dev/null -w 'code=%{http_code} size_download=%{size_download}\n' "$EP/$B/dir/big.bin?versionId=$VBIG" 2>&1 | grep "^< \|transfer closed\|Recv failure\|code=\|curl:"; echo "curl exit=${PIPESTATUS[0]}"
log "=== damaged: get-object-attributes"
aws --endpoint-url $EP s3api get-object-attributes --bucket $B --key dir/big.bin --version-id $VBIG --object-attributes ETag Checksum ObjectSize 2>&1 | head -20
log "=== damaged: aws cli get-object (botocore)"
aws --endpoint-url $EP s3api get-object --bucket $B --key dir/big.bin --version-id $VBIG $W/cli-damaged.bin 2>&1 | tail -5; echo "exit=$?"; ls -l $W/cli-damaged.bin 2>/dev/null
log "=== small (intact) GET still works"
curl -s "${SIG[@]}" -o $W/small-get.bin -w 'code=%{http_code} size_download=%{size_download}\n' "$EP/$B/dir/small.bin?versionId=$VSMALL"; cmp $W/small-get.bin $W/small.bin && echo small-equal
log "=== rustfs container log (tail)"
docker logs $CT 2>&1 | tail -40
log "=== rustfs internal log files"
docker exec $CT sh -c 'ls -la /logs 2>/dev/null; for f in /logs/*; do echo "--- $f"; tail -40 "$f"; done' 2>&1 | tail -80
log "=== phase 2: damage the inline small object (data lives in xl.meta), stop, flip a byte 64 bytes before the end of xl.meta, restart"
run docker stop $CT
docker run --rm -v $VOL:/data --entrypoint sh $IMG -c "set -e; f=/data/$B/dir/small.bin/xl.meta; size=\$(stat -c %s \$f); off=\$((size-64)); sha256sum \$f; v=\$(dd if=\$f bs=1 skip=\$off count=1 status=none | od -An -tu1 | tr -d ' '); n=\$(( (v ^ 1) & 255 )); printf \"\$(printf '\\\\%03o' \$n)\" | dd of=\$f bs=1 seek=\$off conv=notrunc status=none; sha256sum \$f; echo size=\$size offset=\$off before=\$v after=\$n"
run docker start $CT
PORT=$(docker port $CT 9000/tcp | head -1 | sed "s/.*://"); EP=http://127.0.0.1:$PORT; log "endpoint after restart $EP"
ready
for i in $(seq 1 60); do curl -sf $EP/health >/dev/null && break; sleep 0.5; done; [ -n "${VSMALL:-}" ] && ready
log "=== inline damaged: HEAD small"
curl -sI "${SIG[@]}" "$EP/$B/dir/small.bin?versionId=$VSMALL"; echo "curl exit=$?"
log "=== inline damaged: GET small"
curl -sv "${SIG[@]}" -o $W/inline-damaged.bin -w 'code=%{http_code} size_download=%{size_download}\n' "$EP/$B/dir/small.bin?versionId=$VSMALL" 2>&1 | grep "^< \|transfer closed\|Recv failure\|code=\|curl:"; echo "curl exit=${PIPESTATUS[0]}"; ls -l $W/inline-damaged.bin 2>/dev/null; cmp $W/inline-damaged.bin $W/small.bin 2>&1 | head -2
log "=== rustfs log lines after the damaged reads (ERROR/WARN mentioning bitrot, checksum, hash, verify, corrupt)"
docker exec $CT sh -c 'grep -i "bitrot\|checksum\|hash\|verif\|corrupt\|part\|read" /logs/rustfs.log | tail -30'
log done
