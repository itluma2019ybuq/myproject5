@echo off
echo === 生产工艺优化系统 ===
echo 正在编译项目...
sbt clean compile

echo 正在运行程序...
sbt "runMain ProductionOptimization"

echo 程序运行完成！
pause 