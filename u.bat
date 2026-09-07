@echo off
rem Sync BridgeStats parquet from the pipeline (no pkl files).
rem Large board-results monoliths stay on E: and are found at runtime via
rem resolve_data_file. Only the small lookup/narrow files are copied into
rem data\ and published to prod.

set "ffbridge_source=e:\bridge\data\ffbridge"
set "ffbridge_source_data=e:\bridge\data\ffbridge\data"
set "prod_bridgestats=\\X1-pro-470-1tb\c\sw\bridge\ML-Contract-Bridge\src\bridgestats-ffbridge\data"
rem Do not treat C:\sw\bridge\...\data as prod: that path is also the OneDrive
rem junction on the data host (P620). Detect the prod box by computer name.

if not exist "data\" (
    mkdir data
    if errorlevel 1 exit /b 1
)

for %%S in ("%ffbridge_source%" "%ffbridge_source_data%") do (
    if exist "%%~S\" (
        for %%F in (
            ffbridge_club_hand_records_augmented_narrow.parquet
            ffbridge_player_info.parquet
            ffbridge_clubs.parquet
        ) do (
            if exist "%%~S\%%F" (
                xcopy "%%~S\%%F" "data\" /D /Y
                if errorlevel 1 exit /b 1
            )
        )
    )
)

if /i "%COMPUTERNAME%"=="X1-PRO-470-1TB" (
    echo Already on prod host; using local data\ and skipping UNC publish.
    exit /b 0
)

if not exist "%prod_bridgestats%\" (
    mkdir "%prod_bridgestats%"
    if errorlevel 1 (
        echo Prod data directory not reachable: %prod_bridgestats%
        exit /b 1
    )
)

for %%F in (
    ffbridge_club_hand_records_augmented_narrow.parquet
    ffbridge_player_info.parquet
    ffbridge_clubs.parquet
) do (
    if exist "data\%%F" (
        xcopy "data\%%F" "%prod_bridgestats%\" /D /Y
        if errorlevel 1 exit /b 1
    )
)

exit /b 0
