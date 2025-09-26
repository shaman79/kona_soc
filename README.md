Example configuration that sould be placed in sensors.yaml.

```
- platform: konasoc
  host: 192.168.1.4  #There is a DNAT on 192.168.1.4 to 192.168.0.10
  port: 35000
  name: "Kona SoC"
  header: "7E4"
  response_id: "7EC"
  pid: "220105"
  offset: 24
  length: 1
  scale: 0.5
  scan_interval:
    seconds: 60
  hold_last_value: true   # <- ensures last value is kept on errors/disconnects
```
