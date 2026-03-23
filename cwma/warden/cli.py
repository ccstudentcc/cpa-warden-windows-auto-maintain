from __future__ import annotations

import argparse


def build_parser(default_config_path: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基于 auth-files + wham/usage + auth-files 上传的交互式 CPA 账号维护脚本")
    parser.add_argument("--config", default=default_config_path, help="配置文件路径（默认: config.json）")
    parser.add_argument("--mode", choices=["scan", "maintain", "upload", "maintain-refill"], help="运行模式")
    parser.add_argument("--target-type", help="按 files[].type 过滤")
    parser.add_argument("--provider", help="按 provider 过滤")
    parser.add_argument("--probe-workers", type=int, help="api-call 探测并发")
    parser.add_argument("--action-workers", type=int, help="删除/禁用/启用并发")
    parser.add_argument("--timeout", type=int, help="请求超时秒数")
    parser.add_argument("--retries", type=int, help="单账号探测失败重试次数")
    parser.add_argument("--delete-retries", type=int, help="删除动作失败重试次数")
    parser.add_argument("--user-agent", help="wham/usage 探测使用的 User-Agent")
    parser.add_argument("--quota-action", choices=["disable", "delete"], help="限额账号处理动作")
    parser.add_argument("--quota-disable-threshold", type=float, help="剩余额度比例触发禁用阈值（0~1，默认 0 表示仅耗尽）")
    parser.add_argument("--maintain-names-file", help="维护范围名称文件（每行一个 auth 名称，仅 maintain 模式生效）")
    parser.add_argument("--upload-names-file", help="上传范围名称文件（每行一个 auth 名称，仅 upload 模式生效）")
    parser.add_argument("--db-path", help="SQLite 状态库路径")
    parser.add_argument("--invalid-output", help="401 导出文件路径")
    parser.add_argument("--quota-output", help="限额导出文件路径")
    parser.add_argument("--log-file", help="日志文件路径")
    parser.add_argument("--debug", action="store_true", help="开启调试模式，在终端打印更详细信息")
    parser.add_argument("--upload-dir", help="upload 模式下认证文件目录")
    parser.add_argument("--upload-workers", type=int, help="upload 模式并发数")
    parser.add_argument("--upload-retries", type=int, help="upload 模式失败重试次数")
    parser.add_argument("--upload-method", choices=["json", "multipart"], help="upload 模式上传方式")
    parser.add_argument("--min-valid-accounts", type=int, help="maintain-refill 模式下最小有效账号阈值")
    parser.add_argument("--refill-strategy", choices=["to-threshold", "fixed"], help="maintain-refill 模式补充策略")
    parser.add_argument("--register-command", help="外部注册命令（仅 maintain-refill + auto_register）")
    parser.add_argument("--register-timeout", type=int, help="外部注册命令超时秒数")
    parser.add_argument("--register-workdir", help="外部注册命令工作目录")

    upload_force_group = parser.add_mutually_exclusive_group()
    upload_force_group.add_argument("--upload-force", dest="upload_force", action="store_true", help="upload 模式强制上传远端同名文件")
    upload_force_group.add_argument("--no-upload-force", dest="upload_force", action="store_false", help="upload 模式不强制上传远端同名文件")

    delete_group = parser.add_mutually_exclusive_group()
    delete_group.add_argument("--delete-401", dest="delete_401", action="store_true", help="维护模式下自动删除 401")
    delete_group.add_argument("--no-delete-401", dest="delete_401", action="store_false", help="维护模式下不删除 401")

    reenable_group = parser.add_mutually_exclusive_group()
    reenable_group.add_argument("--auto-reenable", dest="auto_reenable", action="store_true", help="维护模式下自动启用恢复账号")
    reenable_group.add_argument("--no-auto-reenable", dest="auto_reenable", action="store_false", help="维护模式下不自动启用恢复账号")
    parser.add_argument(
        "--reenable-scope",
        choices=["signal", "managed"],
        help="自动恢复候选范围：signal=按实时信号，managed=仅恢复本工具曾禁用账号",
    )

    register_group = parser.add_mutually_exclusive_group()
    register_group.add_argument("--auto-register", dest="auto_register", action="store_true", help="maintain-refill 模式不足时启用外部注册命令")
    register_group.add_argument("--no-auto-register", dest="auto_register", action="store_false", help="maintain-refill 模式不足时不启用外部注册命令")

    recursive_group = parser.add_mutually_exclusive_group()
    recursive_group.add_argument("--upload-recursive", dest="upload_recursive", action="store_true", help="upload 模式递归扫描子目录")
    recursive_group.add_argument("--no-upload-recursive", dest="upload_recursive", action="store_false", help="upload 模式仅扫描当前目录")

    parser.set_defaults(
        delete_401=None,
        auto_reenable=None,
        auto_register=None,
        debug=None,
        upload_recursive=None,
        upload_force=None,
    )
    parser.add_argument("--yes", action="store_true", help="跳过删除确认")
    return parser


def parse_cli_args(default_config_path: str, argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser(default_config_path)
    return parser.parse_args(argv)

