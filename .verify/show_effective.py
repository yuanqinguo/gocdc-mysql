import json, sys, urllib.request, base64

auth = base64.b64encode(b"admin:demo123").decode()
req = urllib.request.Request("http://127.0.0.1:8060/api/v1/rules/effective",
                             headers={"Authorization": "Basic " + auth})
d = json.load(urllib.request.urlopen(req, timeout=5))["data"]
print("tables:", len(d))
for v in d:
    print(v["key"], "stock=", v["do_stock"], "ddl=", v["ddl_policy"],
          "order=", repr(v["order_by_column"]),
          "mask=", list((v["mask_columns"] or {}).keys()),
          "target_db=", repr(v["target_database"]))
