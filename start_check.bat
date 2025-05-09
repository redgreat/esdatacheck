@echo off
REM 数据一致性检查启动脚本
echo 正在启动数据一致性检查工具...

REM 切换到脚本所在目录
cd /d %~dp0

REM 检查是否存在Python环境
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo 错误：未找到Python环境，请安装Python后再运行此脚本。
    pause
    exit /b 1
)

REM 检查配置文件是否存在
if not exist "config\config.ini" (
    echo 警告：未找到配置文件，正在创建默认配置文件模板...
    if not exist "config" mkdir config
    copy "config\config.ini.example" "config\config.ini" >nul 2>nul
    echo 已创建配置文件模板，请修改 config\config.ini 后再次运行。
    start notepad config\config.ini
    pause
    exit /b 0
)

REM 检查是否已安装依赖
echo 检查依赖项...
pip show pymysql elasticsearch loguru requests >nul 2>nul
if %errorlevel% neq 0 (
    echo 正在安装所需依赖...
    pip install -r requirements.txt
)

REM 运行检查程序
echo 正在启动检查程序...
python src\main.py %*

pause
