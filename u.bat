@echo off
rem Sync BridgeStats parquet from the pipeline (no pkl files).
rem Club board-results is still small (builder output / demo) so it is copied
rem into data\ and published with the lookup/narrow files. If it later becomes
rem an E: monolith, drop it from this list and mount extra-data instead.

set "ffbridge_source=e:\bridge\data\ffbridge"
set "ffbridge_source_data=e:\bridge\data\ffbridge\data"
set "elo_index=%~dp0..\elo\data\ffbridge\player_session_index"
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
            ffbridge_club_board_results_augmented.parquet
            ffbridge_club_hand_records_augmented_narrow.parquet
            ffbridge_player_info.parquet
            ffbridge_clubs.parquet
            lancelot_persons.parquet
        ) do (
            if exist "%%~S\%%F" (
                xcopy "%%~S\%%F" "data\" /D /Y
                if errorlevel 1 exit /b 1
            )
        )
    )
)

if exist "%elo_index%\lancelot_persons.parquet" (
    xcopy "%elo_index%\lancelot_persons.parquet" "data\" /D /Y
    if errorlevel 1 exit /b 1
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
    ffbridge_club_board_results_augmented.parquet
    ffbridge_club_hand_records_augmented_narrow.parquet
    ffbridge_player_info.parquet
    ffbridge_clubs.parquet
    lancelot_persons.parquet
) do (
    if exist "data\%%F" (
        xcopy "data\%%F" "%prod_bridgestats%\" /D /Y
        if errorlevel 1 exit /b 1
    )
)

exit /b 0
