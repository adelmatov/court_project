@echo off
REM ========== УСТАНОВКА UTF-8 ДЛЯ WINDOWS ==========
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

cd /d "C:\Users\adelmatov001\court_project"

REM Создаем папку logs если её нет
if not exist "logs" mkdir "logs"

REM ========== АВТООЧИСТКА СТАРЫХ ЛОГОВ ==========
for /f "skip=3 delims=" %%F in ('dir /b /o-d "logs\orchestrator_*.log" 2^>nul') do del "logs\%%F" 2>nul
REM ===============================================

REM Имя файла лога с датой и временем (изменено имя переменной)
set ORCHESTRATOR_LOG=logs\orchestrator_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
set ORCHESTRATOR_LOG=%ORCHESTRATOR_LOG: =0%

echo ================================================ >> "%ORCHESTRATOR_LOG%"
echo ОРКЕСТРАТОР - ЗАПУСК ВСЕХ ПАРСЕРОВ >> "%ORCHESTRATOR_LOG%"
echo Время: %date% %time% >> "%ORCHESTRATOR_LOG%"
echo ================================================ >> "%ORCHESTRATOR_LOG%"
echo.

echo ================================================
echo ОРКЕСТРАТОР - ЗАПУСК ВСЕХ ПАРСЕРОВ
echo Время: %date% %time%
echo Лог: %ORCHESTRATOR_LOG%
echo ================================================
echo.

REM Активация виртуального окружения
call venv\Scripts\activate.bat

REM =============================================================================
REM ШАГ 1: Запуск первого парсера (qamqor_parser) - поиск проверок
REM =============================================================================
echo [1/4] Запуск первого парсера (поиск проверок)...
echo [1/4] Запуск первого парсера (поиск проверок)... >> "%ORCHESTRATOR_LOG%"
echo --------------------------------------------- >> "%ORCHESTRATOR_LOG%"

python -u -m parsers.qamqor.qamqor_parser >> "%ORCHESTRATOR_LOG%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] Первый парсер завершен успешно
    echo [OK] Первый парсер завершен успешно >> "%ORCHESTRATOR_LOG%"
) else (
    echo [ОШИБКА] Первый парсер завершен с ошибкой: %ERRORLEVEL%
    echo [ОШИБКА] Первый парсер завершен с ошибкой: %ERRORLEVEL% >> "%ORCHESTRATOR_LOG%"
)
echo.

REM =============================================================================
REM ШАГ 2: Запуск второго парсера (updater) - обновление старых записей
REM =============================================================================
echo [2/4] Запуск парсера 2 (updater)...
echo [2/4] Запуск парсера 2 (updater)... >> "%ORCHESTRATOR_LOG%"
echo --------------------------------------------- >> "%ORCHESTRATOR_LOG%"

python -u -m parsers.qamqor.qamqor_updater --status "1" >> "%ORCHESTRATOR_LOG%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] Парсер 2 завершен успешно
    echo [OK] Парсер 2 завершен успешно >> "%ORCHESTRATOR_LOG%"
) else (
    echo [ОШИБКА] Парсер 2 завершен с ошибкой: %ERRORLEVEL%
    echo [ОШИБКА] Парсер 2 завершен с ошибкой: %ERRORLEVEL% >> "%ORCHESTRATOR_LOG%"
)
echo.

REM =============================================================================
REM ШАГ 3: Запуск третьего парсера (company_info) - получение данных о компаниях
REM =============================================================================
echo [3/4] Запуск парсера 3 (company_info)...
echo [3/4] Запуск парсера 3 (company_info)... >> "%ORCHESTRATOR_LOG%"
echo --------------------------------------------- >> "%ORCHESTRATOR_LOG%"

python -u -m parsers.company_info.main --source qamqor >> "%ORCHESTRATOR_LOG%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] Парсер 3 завершен успешно
    echo [OK] Парсер 3 завершен успешно >> "%ORCHESTRATOR_LOG%"
) else (
    echo [ОШИБКА] Парсер 3 завершен с ошибкой: %ERRORLEVEL%
    echo [ОШИБКА] Парсер 3 завершен с ошибкой: %ERRORLEVEL% >> "%ORCHESTRATOR_LOG%"
)
echo.

REM =============================================================================
REM ШАГ 4: Запуск скрипта-сборщика данных в Excel
REM =============================================================================
echo [4/4] Запуск сборщика данных в Excel...
echo [4/4] Запуск сборщика данных в Excel... >> "%ORCHESTRATOR_LOG%"
echo --------------------------------------------- >> "%ORCHESTRATOR_LOG%"

python -u -m parsers.qamqor.qamqor_data_to_excel_collector >> "%ORCHESTRATOR_LOG%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [OK] Сборщик завершен успешно
    echo [OK] Сборщик завершен успешно >> "%ORCHESTRATOR_LOG%"
) else (
    echo [ОШИБКА] Сборщик завершен с ошибкой: %ERRORLEVEL%
    echo [ОШИБКА] Сборщик завершен с ошибкой: %ERRORLEVEL% >> "%ORCHESTRATOR_LOG%"
)
echo.

REM =============================================================================
REM ЗАВЕРШЕНИЕ
REM =============================================================================
echo ================================================ >> "%ORCHESTRATOR_LOG%"
echo ОРКЕСТРАТОР ЗАВЕРШЕН >> "%ORCHESTRATOR_LOG%"
echo Время: %date% %time% >> "%ORCHESTRATOR_LOG%"
echo ================================================ >> "%ORCHESTRATOR_LOG%"

echo ================================================
echo ОРКЕСТРАТОР ЗАВЕРШЕН
echo Время: %date% %time%
echo ================================================
echo.
echo Лог сохранён: %ORCHESTRATOR_LOG%
echo.

call venv\Scripts\deactivate.bat

REM pause