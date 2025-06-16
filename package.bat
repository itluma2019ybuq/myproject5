@echo off
echo === 生产工艺优化系统 - 打包部署 ===
echo 正在清理和编译项目...
mvn clean compile

echo 正在打包可执行JAR文件...
mvn package

echo 打包完成！
echo 可执行文件位置: target/production-optimization-1.0.0.jar
echo 运行命令: java -jar target/production-optimization-1.0.0.jar

echo 或者直接运行打包后的程序:
java -jar target/production-optimization-1.0.0.jar

pause 