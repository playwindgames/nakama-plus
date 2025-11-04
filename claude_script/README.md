# GitHub Release 自动发布脚本

## 概述

这是一个基于 GitHub CLI 的自动化发布脚本，能够从 CHANGELOG.md 中提取版本信息并创建 GitHub release。

## 文件说明

- `create-release.sh` - 主要的发布脚本
- `README.md` - 本说明文档

## 快速开始

### 前置要求

1. 安装 GitHub CLI (gh)
2. 进行 GitHub CLI 认证：`gh auth login`
3. 确保当前目录是 Git 仓库且没有未提交的更改

### 基本用法

```bash
# 预览模式（推荐先使用）
./create-release.sh --dry-run

# 实际发布
./create-release.sh

# 指定上一个版本
./create-release.sh --prev-version 3.31.0

# 自定义标题和仓库
./create-release.sh --title "Nakama v3.32.1" --repo heroiclabs/nakama
```

## 参数说明

| 参数 | 长参数 | 说明 |
|------|--------|------|
| `-p` | `--prev-version` | 指定上一个版本号 |
| `-d` | `--dry-run` | 预览模式，不实际执行发布 |
| `-c` | `--changelog` | 指定 CHANGELOG.md 文件路径 |
| `-r` | `--repo` | 指定仓库名称（格式：owner/repo） |
| `-t` | `--title` | 自定义 release 标题 |
| `-h` | `--help` | 显示帮助信息 |

## 工作流程

1. **版本提取**: 从 CHANGELOG.md 中提取最新的版本号（格式：## [x.x.x]）
2. **内容生成**: 提取当前版本与上一个版本之间的 changelog 内容
3. **预览确认**: Dry run 模式下显示将要执行的操作
4. **创建 Release**: 调用 GitHub CLI 创建 release

## 示例输出

### Dry Run 模式
```
=== Dry Run 模式 ===
将执行以下操作：

📦 Release 信息：
  版本号: 3.32.1
  标题: Release 3.32.1
  仓库: heroiclabs/nakama

📝 Release 内容：
## Release 3.32.1

### Fixed
- Shorter processing for matchmaker custom function when inactive.
- Google and Apple In-App Purchase notification handling improvements for subscription upgrade/downgrade events.

⚠️  这是预览模式，不会实际创建 release
```

## 安全特性

- **Dry Run 模式**: 默认提供预览功能，避免误操作
- **环境检查**: 验证 Git 状态和依赖项
- **确认机制**: 实际发布前需要用户确认
- **错误处理**: 完善的错误检查和友好的错误信息

## 故障排除

### 常见错误

1. **"存在未提交的更改"**
   - 解决：提交所有更改后再运行脚本

2. **"无法从 CHANGELOG.md 中提取版本号"**
   - 解决：检查 CHANGELOG.md 格式是否为 `## [x.x.x] - YYYY-MM-DD`

3. **"GitHub CLI 未认证"**
   - 解决：运行 `gh auth login` 进行认证

### 调试技巧

- 使用 `--dry-run` 参数预览操作
- 检查脚本输出的日志信息
- 确认 CHANGELOG.md 格式正确

## 相关文档

详细的技术实现文档请参考：`../claude_doc/GitHub_Release脚本实现文档.md`