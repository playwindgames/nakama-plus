#!/bin/bash

# GitHub CLI 自动发布脚本
# 用于基于 CHANGELOG.md 创建 GitHub release

set -euo pipefail

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 默认配置
DEFAULT_CHANGELOG_FILE="CHANGELOG.md"
DEFAULT_REPO=""  # 空字符串表示使用当前目录的 Git 仓库
DRY_RUN=false
PREV_VERSION=""
TITLE=""
REPO=""

# 显示帮助信息
show_help() {
    cat << EOF
GitHub CLI 自动发布脚本

用法: $0 [OPTIONS]

选项:
    -p, --prev-version VERSION    指定上一个版本号
    -d, --dry-run                预览模式，不实际执行发布
    -c, --changelog FILE         指定 CHANGELOG.md 文件路径 (默认: ./CHANGELOG.md)
    -r, --repo REPO              指定仓库名称 (格式: owner/repo)
    -t, --title TITLE            自定义 release 标题 (默认: 版本号)
    -h, --help                   显示此帮助信息

示例:
    $0                           # 使用默认设置发布
    $0 --dry-run                 # 预览模式
    $0 --prev-version 3.31.0     # 指定上一个版本
    $0 --repo heroiclabs/nakama  # 指定仓库
    $0 --title "Nakama 3.32.1"   # 自定义标题

EOF
}

# 日志函数
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
    exit 1
}

# 检查必需工具
check_dependencies() {
    log_info "检查依赖项..."

    # 检查 GitHub CLI
    if ! command -v gh &> /dev/null; then
        log_error "GitHub CLI (gh) 未安装。请访问 https://cli.github.com/ 安装"
    fi
    log_success "GitHub CLI 已安装"

    # 检查 GitHub CLI 认证
    if ! gh auth status &> /dev/null; then
        log_error "GitHub CLI 未认证。请运行 'gh auth login' 进行认证"
    fi
    log_success "GitHub CLI 已认证"

    # 检查 Git
    if ! command -v git &> /dev/null; then
        log_error "Git 未安装"
    fi
    log_success "Git 已安装"
}

# 检查 Git 仓库状态
check_git_status() {
    log_info "检查 Git 仓库状态..."

    # 检查是否在 Git 仓库中
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        log_error "当前目录不是 Git 仓库"
    fi

    # 检查当前分支
    local current_branch=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$current_branch" != "main" && "$current_branch" != "master" ]]; then
        log_warning "当前不在 main/master 分支，而是在 '$current_branch' 分支"
        read -p "是否继续? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "操作已取消"
            exit 0
        fi
    fi

    # 检查是否有未提交的更改
    if ! git diff-index --quiet HEAD --; then
        log_error "存在未提交的更改，请先提交所有更改"
    fi

    # 检查是否与远程同步
    local remote_exists=$(git remote | wc -l)
    if [[ $remote_exists -gt 0 ]]; then
        local remote_url=$(git remote get-url $(git remote | head -n 1))
        log_info "远程仓库: $remote_url"
    fi

    log_success "Git 仓库状态检查通过"
}

# 从 CHANGELOG.md 提取最新版本号
extract_latest_version() {
    local changelog_file="$1"

    if [[ ! -f "$changelog_file" ]]; then
        log_error "CHANGELOG.md 文件不存在: $changelog_file"
    fi

    # 使用正则表达式匹配版本号，跳过 Unreleased
    local version=$(grep -m 1 -E '^## \[(\d+\.\d+\.\d+)\]' "$changelog_file" | sed -E 's/^## \[([0-9]+\.[0-9]+\.[0-9]+)\].*/\1/')

    if [[ -z "$version" ]]; then
        log_error "无法从 CHANGELOG.md 中提取版本号。请检查格式是否为 '## [x.x.x] - YYYY-MM-DD'"
    fi

    echo "$version"
}

# 获取最新 release 版本号
get_latest_release_version() {
    local repo="$1"

    local latest_release
    if [[ -n "$repo" ]]; then
        latest_release=$(gh release view --repo "$repo" --json tagName --jq '.tagName' 2>/dev/null || echo "")
    else
        latest_release=$(gh release view --json tagName --jq '.tagName' 2>/dev/null || echo "")
    fi

    echo "$latest_release"
}

# 生成 release 内容
generate_release_content() {
    local changelog_file="$1"
    local current_version="$2"
    local prev_version="$3"

    # 读取整个 CHANGELOG.md 文件
    local content=$(cat "$changelog_file")

    # 提取当前版本的内容
    local current_section
    if [[ -n "$prev_version" ]]; then
        # 提取两个版本之间的内容
        current_section=$(echo "$content" | sed -n "/^## \[$current_version\]/,/^## \[$prev_version\]/p" | sed '$d')
    else
        # 提取当前版本到下一个版本之间的内容
        current_section=$(echo "$content" | sed -n "/^## \[$current_version\]/,/^## \[/p" | sed '$d')
    fi

    if [[ -z "$current_section" ]]; then
        log_error "无法提取版本 $current_version 的内容"
    fi

    # 移除版本行，保留内容
    local release_content=$(echo "$current_section" | sed -E '1d')

    # 添加标题
    local full_content="## Release $current_version\n\n$release_content"

    echo -e "$full_content"
}

# 创建 GitHub release
create_github_release() {
    local version="$1"
    local title="$2"
    local content="$3"
    local repo="$4"

    log_info "创建 GitHub release..."

    local repo_flag=""
    if [[ -n "$repo" ]]; then
        repo_flag="--repo $repo"
    fi

    # 创建临时文件存储内容
    local temp_file=$(mktemp)
    echo -e "$content" > "$temp_file"

    # 构建 gh release 命令
    local cmd="gh release create $version --title \"$title\" --notes-file \"$temp_file\" $repo_flag"

    if [[ "$DRY_RUN" == true ]]; then
        echo -e "\n${YELLOW}=== Dry Run 模式 ===${NC}"
        echo -e "${BLUE}将执行以下操作：\n${NC}"
        echo -e "${YELLOW}📦 Release 信息：${NC}"
        echo "  版本号: $version"
        echo "  标题: $title"
        [[ -n "$repo" ]] && echo "  仓库: $repo"
        echo -e "\n${YELLOW}📝 Release 内容：${NC}"
        echo -e "$content"
        echo -e "\n${YELLOW}🔧 命令：${NC}"
        echo "$cmd"
        echo -e "\n${YELLOW}⚠️  这是预览模式，不会实际创建 release${NC}"
        echo "移除 --dry-run 参数执行实际发布操作"
    else
        # 执行命令
        if eval "$cmd"; then
            log_success "Release 已成功创建"

            # 获取 release URL
            local release_url
            if [[ -n "$repo" ]]; then
                release_url="https://github.com/$repo/releases/tag/$version"
            else
                local remote_url=$(git remote get-url $(git remote | head -n 1) 2>/dev/null || echo "")
                if [[ "$remote_url" =~ github\.com[:/](.+)\.git$ ]]; then
                    release_url="https://github.com/${BASH_REMATCH[1]}/releases/tag/$version"
                else
                    release_url="Release created for version $version"
                fi
            fi

            echo -e "${GREEN}🎉 Release URL: $release_url${NC}"
        else
            log_error "创建 release 失败"
        fi
    fi

    # 清理临时文件
    rm -f "$temp_file"
}

# 解析命令行参数
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -p|--prev-version)
                PREV_VERSION="$2"
                shift 2
                ;;
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -c|--changelog)
                DEFAULT_CHANGELOG_FILE="$2"
                shift 2
                ;;
            -r|--repo)
                REPO="$2"
                shift 2
                ;;
            -t|--title)
                TITLE="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "未知参数: $1。使用 -h 查看帮助信息"
                ;;
        esac
    done
}

# 主函数
main() {
    echo -e "${BLUE}=== GitHub CLI 自动发布脚本 ===${NC}\n"

    # 解析参数
    parse_arguments "$@"

    # 检查依赖项
    check_dependencies

    # 检查 Git 状态
    check_git_status

    # 提取当前版本
    log_info "从 CHANGELOG.md 提取最新版本号..."
    local current_version=$(extract_latest_version "$DEFAULT_CHANGELOG_FILE")
    if [[ -z "$current_version" ]]; then
        log_error "无法提取当前版本号"
    fi
    log_success "提取到版本号: $current_version"

    # 获取上一个版本（如果未指定）
    if [[ -z "$PREV_VERSION" ]]; then
        log_info "获取最新 release 版本号..."
        PREV_VERSION=$(get_latest_release_version "$REPO")
        if [[ -z "$PREV_VERSION" ]]; then
            log_warning "未找到任何 release，这将是第一个 release"
        else
            log_success "最新 release 版本: $PREV_VERSION"
        fi
    fi

    # 设置标题
    if [[ -z "$TITLE" ]]; then
        TITLE="Release $current_version"
    fi

    # 生成 release 内容
    log_info "生成 release 内容..."
    local release_content=$(generate_release_content "$DEFAULT_CHANGELOG_FILE" "$current_version" "$PREV_VERSION")
    if [[ -z "$release_content" ]]; then
        log_error "无法生成 release 内容"
    fi
    log_success "Release 内容已生成"

    # 显示摘要
    echo -e "${BLUE}=== 发布摘要 ===${NC}"
    echo "当前版本: $current_version"
    [[ -n "$PREV_VERSION" ]] && echo "上一个版本: $PREV_VERSION" || echo "上一个版本: 无（首次发布）"
    echo "标题: $TITLE"
    [[ -n "$REPO" ]] && echo "仓库: $REPO"
    echo "模式: $([ "$DRY_RUN" == true ] && echo "预览" || echo "实际发布")"
    echo

    # 确认操作（非 dry run 模式）
    if [[ "$DRY_RUN" == false ]]; then
        echo -e "${YELLOW}即将创建 release，请确认以上信息是否正确。${NC}"
        read -p "继续? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "操作已取消"
            exit 0
        fi
    fi

    # 创建 release
    create_github_release "$current_version" "$TITLE" "$release_content" "$REPO"
}

# 执行主函数
main "$@"