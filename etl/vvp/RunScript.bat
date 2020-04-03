REM script file to call python for data transformation
@echo off

echo "loading file"

REM Check python version
python -V

REM call python program
python DEEPA_VVP.py
echo "Output file generated"

REM end of script file