# Contributing

## C# 规范

- 所有 public 类型和 public 方法必须有完整 XML 注释。
- 每个文件只能有一个顶层类型，私有嵌套类型除外。
- 所有代码块必须使用大括号，禁止省略方法体的表达式体方法。
- 非静态、非常量字段使用 `_camelCase`；`const` 和 `static readonly` 使用 PascalCase。
- `.cs` 文件使用 CRLF。
- 大数组通过 `ArrayPool<T>` 或 `MemoryPool<T>` 获取并释放。
- 只读数组和列表参数优先使用 Span/Memory。
- 禁止无意义的 `Span<T>.ToArray()`。
- Logger、正则和 JSON 使用源生成。

## 开发流程

1. 先写失败测试并运行确认失败。
2. 实现最小改动使测试通过。
3. 运行相关测试和完整解决方案构建。
4. 运行 `SourceConventionTests`。
5. 更新实现计划或 README 中受影响的行为说明。

```powershell
dotnet build MqttZero.slnx -c Release
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release
```
