@echo off
echo === 生产工艺优化系统 (Maven版本) ===
echo 正在清理项目...
mvn clean

echo 正在编译项目...
mvn compile

echo 正在运行程序...
mvn exec:java -Dexec.mainClass="ProductionOptimization"

echo 程序运行完成！
pause 