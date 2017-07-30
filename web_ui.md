
## Accessing Spark Web UI

Create SSH tunnel:
```
gcloud compute ssh  --zone=us-central1-a --ssh-flag="-D 1080" --ssh-flag="-N" --ssh-flag="-n" cluster-1-m
```

Use SOCKS protocol to connect with browser:
```
/usr/bin/chromium-browser --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/
```
