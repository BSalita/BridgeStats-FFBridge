# FFBridge Club parquets

Build or refresh with:

```
python build_ffbridge_club_parquets.py --demo
python build_ffbridge_club_parquets.py --source-dir E:\bridge\data\ffbridge\data
```

These files are read at query time. Lancelot is not called from the app or MCP.

`lancelot_persons.parquet` is the local alias index (`lancelot_person_id`,
`classic_person_id`, `license_number`). Player lookup and board filters
accept any of those ids. Seat columns store `classic_person_id`.
