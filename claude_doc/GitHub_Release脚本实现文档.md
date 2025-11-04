# GitHub CLI 自动发布脚本实现文档

## 概述

本文档描述了一个使用 GitHub CLI 创建 release 的自动化脚本的实现方案。该脚本能够从 CHANGELOG.md 中提取版本信息，并基于当前 master 分支创建 GitHub release。

## 需求分析

### 核心需求
1. **版本号提取**: 从 CHANGELOG.md 文件中找到第一个 `## [x.x.x]` 格式的版本号作为本次 release 版本
2. **内容生成**: 基于版本号范围提取 CHANGELOG.md 中的相关内容作为 release 描述
3. **GitHub CLI 集成**: 使用 GitHub CLI 创建 release
4. **Dry Run 模式**: 提供预览模式，不实际执行发布操作
5. **灵活性**: 支持手动指定上一个版本号，或自动获取最新 release

### CHANGELOG.md 格式分析
通过分析项目中的 CHANGELOG.md 文件，发现其遵循 Keep a Changelog 格式：
- 版本格式：`## [3.32.1] - 2025-10-21`
- 包含 Unreleased 部分
- 按时间倒序排列（最新版本在前）

## 设计思路

### 1. 版本号提取策略
- 使用正则表达式 `^## \[(\d+\.\d+\.\d+)\]` 匹配版本号
- 跳过 `## [Unreleased]` 部分
- 取第一个匹配的版本号作为当前发布版本

### 2. 内容提取策略
- 支持两种模式：
  - **自动模式**: 通过 GitHub CLI 获取最新发布的版本号
  - **手动模式**: 通过参数指定上一个版本号
- 提取两个版本之间的所有 changelog 内容
- 保留原始格式和链接

### 3. 安全性设计
- Dry run 模式：只打印将要执行的操作，不实际执行
- 参数验证：确保版本号格式正确
- 错误处理：完善的错误检查和用户友好的错误信息

### 4. 用户体验设计
- 清晰的命令行参数说明
- 详细的执行过程输出
- 颜色高亮显示重要信息
- 确认机制（可选）

## 技术实现方案

### 脚本结构
```
create-release.sh
├── 参数解析
├── 版本号提取
├── 上一个版本号获取
├── Release 内容生成
├── Dry Run 预览
└── GitHub CLI 调用
```

### 关键函数设计

#### 1. `extract_latest_version()`
从 CHANGELOG.md 提取最新版本号
- 输入：CHANGELOG.md 文件路径
- 输出：版本号字符串
- 错误处理：文件不存在、格式错误

#### 2. `get_latest_release_version()`
通过 GitHub CLI 获取最新 release 版本
- 输入：仓库名称（可选）
- 输出：版本号字符串或空值
- 错误处理：API 调用失败、无 release

#### 3. `generate_release_content()`
生成 release 内容
- 输入：当前版本、上一个版本、CHANGELOG 内容
- 输出：格式化的 release 内容
- 内容过滤：只包含指定版本范围的内容

#### 4. `create_github_release()`
调用 GitHub CLI 创建 release
- 输入：版本号、标题、内容、dry run 标志
- 输出：执行结果
- 错误处理：GitHub CLI 调用失败

### 参数设计
```bash
./create-release.sh [OPTIONS]

选项：
  -p, --prev-version VERSION    指定上一个版本号
  -d, --dry-run                预览模式，不实际执行发布
  -c, --changelog FILE         指定 CHANGELOG.md 文件路径（默认：./CHANGELOG.md）
  -r, --repo REPO              指定仓库名称（默认：当前目录的 Git 仓库）
  -t, --title TITLE            自定义 release 标题（默认：版本号）
  -h, --help                   显示帮助信息
```

### 错误处理策略
1. **文件检查**: 确保 CHANGELOG.md 文件存在且可读
2. **格式验证**: 验证版本号格式的正确性
3. **Git 状态检查**: 确保当前在 master 分支且无未提交更改
4. **GitHub CLI 检查**: 确保 GitHub CLI 已安装且已认证
5. **网络错误处理**: 处理 GitHub API 调用失败的情况

### 输出格式设计

#### Dry Run 模式输出
```
=== Dry Run 模式 ===
将执行以下操作：

📦 Release 信息：
  版本号: 3.32.1
  标题: Release 3.32.1
  仓库: heroiclabs/nakama

📝 Release 内容：
[显示完整的 release 内容]

⚠️  这是预览模式，不会实际创建 release
使用 --no-dry-run 参数执行实际发布操作
```

#### 实际执行输出
```
=== 创建 GitHub Release ===
🔍 检查环境...
✓ Git 仓库检查通过
✓ GitHub CLI 已安装并认证
✓ 当前在 master 分支

📦 提取版本信息...
✓ 当前版本: 3.32.1
✓ 上一个版本: 3.32.0

📝 生成 release 内容...
✓ Release 内容已生成

🚀 创建 release...
✓ Release 已成功创建: https://github.com/heroiclabs/nakama/releases/tag/3.32.1
```

## 使用场景

### 1. 标准发布流程
```bash
# 自动获取上一个版本并发布
./create-release.sh

# 指定上一个版本
./create-release.sh --prev-version 3.31.0
```

### 2. 预览模式
```bash
# 预览将要发布的内容
./create-release.sh --dry-run
```

### 3. 自定义设置
```bash
# 自定义标题和 changelog 文件
./create-release.sh --title "Nakama 3.32.1 Release" --changelog /path/to/CHANGELOG.md
```

## 依赖项

### 必需依赖
- **GitHub CLI (gh)**: 用于与 GitHub API 交互
- **Git**: 用于版本控制操作
- **基础 Unix 工具**: grep, sed, awk 等

### 可选依赖
- **终端颜色支持**: tput 或类似的颜色工具（用于更好的用户体验）

## 安全考虑

1. **权限检查**: 确保用户有权限创建 release
2. **敏感信息**: 避免在日志中暴露敏感信息
3. **操作确认**: 重要操作前提供确认机制
4. **回滚机制**: 提供删除错误 release 的指导

## 扩展性

### 未来可能的扩展
1. **多项目支持**: 支持多个项目的批量发布
2. **模板系统**: 支持自定义 release 模板
3. **集成测试**: 添加发布前的自动化测试
4. **通知机制**: 发布后自动发送通知
5. **版本验证**: 验证版本号的合理性和连续性

## 总结

该脚本提供了一个安全、灵活、用户友好的 GitHub release 自动化解决方案。通过模块化设计和完善的错误处理，确保了脚本的可靠性和可维护性。Dry run 模式的引入让用户能够在实际执行前预览操作，大大提高了使用的安全性。