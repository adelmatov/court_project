@'
@echo off
chcp 65001 > nul
cd /d "C:\Users\adelmatov001\court_project"

REM Создаем папку logs если её нет
if not exist "logs" mkdir "logs"

REM Имя файла лога с датой и временем
set LOG_FILE=logs\run_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
set LOG_FILE=%LOG_FILE: =0%

echo ================================================
echo Запуск парсера Qamqor
echo Время: %date% %time%
echo Лог: %LOG_FILE%
echo ================================================
echo.

REM Активация виртуального окружения
call venv\Scripts\activate.bat

REM Запуск парсера (вывод только в лог-файл)
python -m parsers.qamqor.qamqor_parser >> "%LOG_FILE%" 2>&1

REM Проверка результата
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ================================================
    echo Парсер завершен УСПЕШНО
    echo ================================================
    echo Проверьте лог: %LOG_FILE%
) else (
    echo.
    echo ================================================
    echo Парсер завершен с ОШИБКОЙ: %ERRORLEVEL%
    echo ================================================
    echo Проверьте лог: %LOG_FILE%
)

call venv\Scripts\deactivate.bat

pause
'@ | Out-File -FilePath "run_qamqor_parser.bat" -Encoding ascii -Force