import json, sys, urllib.request, base64

auth = base64.b64encode(b"admin:demo123").decode()
req = urllib.request.Request(
    "http://127.0.0.1:8060/api/v1/verify?confirm=true",
    data=json.dumps({"database": "types", "table": "all_types", "sample": 1000}).encode(),
    headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"}, method="POST")
d = json.load(urllib.request.urlopen(req, timeout=10))["data"]
for line in d["diffs"]:
    pk = line.split()[2]
    src = json.loads(line.split("src=", 1)[1].split("} dst=")[0] + "}")
    dst = json.loads(line.split("dst=", 1)[1])
    for k in src:
        if src[k] != dst.get(k):
            print(f"pk={pk} col={k}: src={src[k]!r} dst={dst.get(k)!r}")
