# 多次返工流程的累积快照与阶段驻留

本项目保存虚构实验设施的仓湖源数据与Python读取工具。现有工具只加载文件并显示条数，不执行事实加工、解码、合并、联接或重映射。全部设备、样品、记录和数值均为合成。

## 字段与单位

type是业务里程碑，event_seconds为发生时刻，revision按同event_id裁决；配置给同刻优先级与截止秒。状态与attempt尚未构建。

material-index.json记录全部输入内容哈希；fixtures保存原始资料与配置，计算输出可放.workspace-output，原始资料不应被覆盖。

## 运行已有读取器

Python 3.11以上，标准库即可，不需安装第三方依赖。

    python -m src.workflow_reader fixtures/workflow.jsonl
    python -m compileall -q src tests
    python -m unittest discover -s tests -v

## 累积快照事实层

`src/workflow_facts.py` 把合成样品事件回放为可追溯事实，不运行实验作业、不预测通过率：

* 版本裁决：同 event_id 最高 revision 替代旧版，withdrawn=true 退出回放并保留证据，平级内容冲突拒绝。
* 回放排序：event_seconds → 配置的同刻类型优先级 → event_id；每次构建从裁决后全集重放，不用最新事件类型覆盖状态，版本更正经重建改变合法性。
* 状态机：received 开始；test_started 开启递增 attempt；运行中只能 pass/fail 结束；失败后须 rework_requested 才能再 started；通过后不得再开尝试；cancelled 终止未终止流程。
* 非法转换：事件进隔离区（quarantine.jsonl），case 在阻塞位置停住，后续事件挂起不跨过，不宣称最终完成；补正后重建解除阻塞。
* as_of 截止：只用 event_seconds ≤ 截止的事件；未结束区间右端取截止并标 open，open 驻留与已完成耗时分列不混加；负时长与重复开始拒绝。

构建事实层（case_snapshot / attempt_facts / state_intervals / quarantine + run_metadata，写入 .workspace-output）：

    python -m src.workflow_facts build --events fixtures/workflow.jsonl --config fixtures/workflow.json --out .workspace-output

区间分解查询（指定 case 在窗口内的阶段驻留分解与汇总）：

    python -m src.workflow_facts decompose --events fixtures/workflow.jsonl --config fixtures/workflow.json --case s1 --as-of 10

事件回放支持检查点与进程恢复（--stop-after 限步、--checkpoint 落盘、--resume 续放）：

    python -m src.workflow_facts replay --events fixtures/workflow.jsonl --config fixtures/workflow.json --as-of 12 --out .workspace-output/replay

场景演示（撤回阻塞与补正、同刻、取消、进程恢复、区间分解）：

    python -m src.workflow_demo

## 固定容器

    docker build -t accumulating-workflow:local .
    docker run --rm accumulating-workflow:local

容器镜像以完整digest固定，默认仅检查输入契约；实现新增分析功能所需依赖由开发者明确登记。
