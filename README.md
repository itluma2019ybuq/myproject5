# 生产工艺优化系统

基于Scala 2.12和Spark 3.3.1的生产工艺参数优化系统，使用机器学习和遗传算法技术。

## 项目结构

```
ProductionOptimization/
├── data/                           # 测试数据目录
│   ├── production_logs.csv         # 生产设备日志
│   └── quality_results.csv         # 质量检测结果
├── src/main/scala/                 # Scala源代码
│   └── ProductionOptimization.scala # 主程序文件
├── target/                         # Maven构建输出目录
├── pom.xml                         # Maven构建配置
├── build.sbt                       # SBT构建配置（备用）
├── run-maven.bat                   # Maven运行脚本
├── package.bat                     # Maven打包脚本
├── run.bat                         # SBT运行脚本（备用）
├── 任务5：生产工艺优化.md              # 详细任务文档
└── README.md                       # 项目说明文档
```

## 功能特性

### 子任务一：特征工程
- ✅ 解析生产设备日志提取工艺参数
- ✅ 关联质量检测结果数据
- ✅ 处理生产过程中的缺失值
- ✅ 数据聚合和统计分析

### 子任务二：参数优化
- ✅ 使用随机森林建立质量预测回归模型
- ✅ 遗传算法寻找最优工艺参数组合
- ✅ 输出详细的参数优化建议

## 技术栈

- **Scala**: 2.12.17
- **Apache Spark**: 3.3.1
- **机器学习**: Spark MLlib
- **优化算法**: 遗传算法
- **构建工具**: Maven 3.6+

## 快速开始

### 环境要求

- JDK 8 或 JDK 11
- Maven 3.6+
- Scala 2.12.17（由Maven自动管理）

### 运行方法

**方式一：使用批处理文件（Windows）**
```cmd
# 直接运行程序
run-maven.bat

# 打包生成可执行JAR
package.bat
```

**方式二：使用Maven命令**
```bash
# 清理和编译
mvn clean compile

# 运行程序
mvn exec:java -Dexec.mainClass="ProductionOptimization"

# 打包可执行JAR
mvn package

# 运行打包后的JAR
java -jar target/production-optimization-1.0.0.jar
```

## 数据说明

### 生产设备日志 (production_logs.csv)
| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | DateTime | 时间戳 |
| device_id | String | 设备ID |
| temperature | Double | 温度(°C) |
| pressure | Double | 压力(MPa) |
| rotation_speed | Integer | 转速(rpm) |
| flow_rate | Double | 流量(L/min) |
| ph_value | Double | pH值 |
| batch_id | String | 批次ID |

### 质量检测结果 (quality_results.csv)
| 字段名 | 类型 | 说明 |
|--------|------|------|
| batch_id | String | 批次ID |
| quality_score | Double | 质量分数 |
| defect_rate | Double | 缺陷率 |
| strength | Double | 强度 |
| density | Double | 密度 |
| color_index | Double | 颜色指数 |
| pass_rate | Double | 合格率 |

## 算法原理

### 特征工程
1. **数据清洗**: 识别和处理缺失值
2. **特征聚合**: 按批次计算统计特征
3. **数据关联**: 匹配生产参数和质量结果

### 参数优化：遗传算法详解

**为什么使用遗传算法？**
- 生产工艺参数优化是一个5维非线性优化问题
- 传统梯度方法无法处理ML模型预测的黑盒函数
- 需要全局搜索避免局部最优解
- 参数空间复杂，存在多个局部最优点

**遗传算法核心机制：**
1. **种群初始化**: 随机生成50个参数组合
2. **适应度评估**: 使用随机森林模型预测质量分数
3. **选择操作**: 轮盘赌选择，优秀个体有更高被选概率
4. **交叉操作**: 线性组合两个父代生成子代
5. **变异操作**: 高斯扰动增加种群多样性
6. **精英保留**: 保留最优5个个体到下一代

**算法参数设置：**
```
种群大小: 50个个体
进化代数: 20代  
变异率: 10%
精英保留: 5个
交叉策略: 线性组合交叉
变异策略: 高斯扰动变异
```

**优化过程：**
- 第1代：探索阶段，种群多样性高，适应度约82.5
- 第10代：开发阶段，逐步收敛，适应度约91.8  
- 第20代：精细化阶段，接近最优解，适应度约94.1

## 输出结果

程序运行后将输出：
- 数据加载和处理统计
- 缺失值处理结果
- 模型训练和评估指标
- 遗传算法优化过程
- 最优工艺参数组合
- 具体的改进建议

## Maven依赖说明

**核心依赖库：**
- `org.scala-lang:scala-library:2.12.17` - Scala标准库
- `org.apache.spark:spark-core_2.12:3.3.1` - Spark核心引擎
- `org.apache.spark:spark-sql_2.12:3.3.1` - Spark SQL引擎
- `org.apache.spark:spark-mllib_2.12:3.3.1` - Spark机器学习库
- `com.fasterxml.jackson.*:*:2.13.4` - JSON处理库（解决版本冲突）

**插件配置：**
- `scala-maven-plugin` - Scala代码编译
- `maven-shade-plugin` - 生成包含所有依赖的可执行JAR
- `exec-maven-plugin` - 直接运行主程序

## 注意事项

- 确保数据文件格式正确，CSV文件包含标题行
- 程序需要足够的内存运行Spark任务（建议4GB以上）
- 遗传算法参数可根据实际需求调整
- 生产环境使用时建议增加数据验证和异常处理
- Maven首次构建会下载较多依赖，请保持网络连接

## 技术支持

如有问题请参考 `任务5：生产工艺优化.md` 文档中的详细实施说明。 