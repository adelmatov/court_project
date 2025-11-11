@echo off
REM ========== УСТАНОВКА UTF-8 ДЛЯ WINDOWS ==========
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

cd /d "C:\Users\adelmatov001\court_project"

REM Создаем папку logs если её нет
if not exist "logs" mkdir "logs"

REM Имя файла лога с датой и временем
set LOG_FILE=logs\orchestrator_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
set LOG_FILE=%LOG_FILE: =0%

echo ================================================ >> "%LOG_FILE%"
echo ОРКЕСТРАТОР - ЗАПУСК ВСЕХ ПАРСЕРОВ >> "%LOG_FILE%"
echo Время: %date% %time% >> "%LOG_FILE%"
echo ================================================ >> "%LOG_FILE%"
echo.

echo ================================================
echo ОРКЕСТРАТОР - ЗАПУСК ВСЕХ ПАРСЕРОВ
echo Время: %date% %time%
echo Лог: %LOG_FILE%
echo ================================================
echo.

REM Активация виртуального окружения
call venv\Scripts\activate.bat

REM =============================================================================
REM ШАГ 1: Запуск первого парсера (qamqor_parser) - поиск проверок
REM =============================================================================
echo [1/4] Запуск первого парсера (поиск проверок)...
echo [1/4] Запуск первого парсера (поиск проверок)... >> "%LOG_FILE%"
echo --------------------------------------------- >> "%LOG_FILE%"

python -m parsers.qamqor.qamqor_parser >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] Первый парсер завершен успешно
    echo [OK] Первый парсер завершен успешно >> "%LOG_FILE%"
) else (
    echo [ОШИБКА] Первый парсер завершен с ошибкой: %ERRORLEVEL%
    echo [ОШИБКА] Первый парсер завершен с ошибкой: %ERRORLEVEL% >> "%LOG_FILE%"
)
echo.

REM =============================================================================
REM ШАГ 2: Параллельный запуск парсера 2 (updater) и парсера 3 (company_info)
REM =============================================================================
echo [2/4] Параллельный запуск парсеров 2 и 3...
echo [2/4] Параллельный запуск парсеров 2 и 3... >> "%LOG_FILE%"
echo --------------------------------------------- >> "%LOG_FILE%"

REM Создаем временные логи для параллельных процессов
set LOG_PARSER2=logs\parser2_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
set LOG_PARSER2=%LOG_PARSER2: =0%

set LOG_PARSER3=logs\parser3_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~6,2%%time:~6,2%.log
set LOG_PARSER3=%LOG_PARSER3: =0%

REM Запуск второго парсера (updater) в фоне
echo   - Запуск парсера 2 (updater)...
echo   - Запуск парсера 2 (updater)... >> "%LOG_FILE%"
start "Parser2_Updater" /B cmd /c "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && call venv\Scripts\activate.bat && python -m parsers.qamqor.qamqor_updater --status "1" > "%LOG_PARSER2%" 2>&1"

REM Запуск третьего парсера (company_info) в фоне
echo   - Запуск парсера 3 (company_info)...
echo   - Запуск парсера 3 (company_info)... >> "%LOG_FILE%"
start "Parser3_CompanyInfo" /B cmd /c "chcp 65001 >nul && set PYTHONIOENCODING=utf-8 && call venv\Scripts\activate.bat && python -m parsers.company_info.main --source qamqor > "%LOG_PARSER3%" 2>&1"

REM Ожидание завершения обоих процессов
echo   - Ожидание завершения параллельных парсеров...
echo   - Ожидание завершения параллельных парсеров... >> "%LOG_FILE%"

:wait_parsers
timeout /t 2 /nobreak >nul

REM Проверяем, запущен ли Parser2
tasklist /FI "WINDOWTITLE eq Parser2_Updater" 2>nul | find "cmd.exe" >nul
set PARSER2_RUNNING=%ERRORLEVEL%

REM Проверяем, запущен ли Parser3
tasklist /FI "WINDOWTITLE eq Parser3_CompanyInfo" 2>nul | find "cmd.exe" >nul
set PARSER3_RUNNING=%ERRORLEVEL%

REM Если хотя бы один еще работает, продолжаем ждать
if %PARSER2_RUNNING% EQU 0 goto wait_parsers
if %PARSER3_RUNNING% EQU 0 goto wait_parsers

echo [OK] Параллельные парсеры завершены
echo [OK] Параллельные парсеры завершены >> "%LOG_FILE%"

REM Добавляем логи параллельных парсеров в основной лог
echo. >> "%LOG_FILE%"
echo === ЛОГ ПАРСЕРА 2 (UPDATER) === >> "%LOG_FILE%"
type "%LOG_PARSER2%" >> "%LOG_FILE%" 2>nul
echo. >> "%LOG_FILE%"
echo === ЛОГ ПАРСЕРА 3 (COMPANY_INFO) === >> "%LOG_FILE%"
type "%LOG_PARSER3%" >> "%LOG_FILE%" 2>nul
echo. >> "%LOG_FILE%"
echo.

REM =============================================================================
REM ШАГ 3: Ожидание 10 секунд
REM =============================================================================
echo [3/4] Ожидание 10 секунд перед запуском сборщика...
echo [3/4] Ожидание 10 секунд... >> "%LOG_FILE%"
timeout /t 10 /nobreak >nul
echo.

REM =============================================================================
REM ШАГ 4: Запуск скрипта-сборщика данных в Excel
REM =============================================================================
echo [4/4] Запуск сборщика данных в Excel...
echo [4/4] Запуск сборщика данных в Excel... >> "%LOG_FILE%"
echo --------------------------------------------- >> "%LOG_FILE%"

python -m parsers.qamqor.qamqor_data_to_excel_collector >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] Сборщик завершен успешно
    echo [OK] Сборщик завершен успешно >> "%LOG_FILE%"
) else (
    echo [ОШИБКА] Сборщик завершен с ошибкой: %ERRORLEVEL%
    echo [ОШИБКА] Сборщик завершен с ошибкой: %ERRORLEVEL% >> "%LOG_FILE%"
)
echo.

REM =============================================================================
REM ЗАВЕРШЕНИЕ
REM =============================================================================
echo ================================================ >> "%LOG_FILE%"
echo ОРКЕСТРАТОР ЗАВЕРШЕН >> "%LOG_FILE%"
echo Время: %date% %time% >> "%LOG_FILE%"
echo ================================================ >> "%LOG_FILE%"

echo ================================================
echo ОРКЕСТРАТОР ЗАВЕРШЕН
echo Время: %date% %time%
echo ================================================
echo.
echo Основной лог: %LOG_FILE%
echo Лог парсера 2: %LOG_PARSER2%
echo Лог парсера 3: %LOG_PARSER3%
echo.

call venv\Scripts\deactivate.bat

REM pause