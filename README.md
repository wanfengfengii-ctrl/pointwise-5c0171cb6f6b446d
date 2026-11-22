# 澄湾观测日账

站点操作员通过有序源事件提交读数、更正和试算。入仓员分段接收，收到仓库确认后释放源空间。夜班用固定成果号领取已提交的账页并核对本站缺测、读数合计和更正出处。

## 运行

使用 Python 3.11 标准库。从项目根目录运行完整回归 `python3 -B check_project.py`。控制台提供此入口。也可直接运行 `python3 -B -m unittest discover -s tests -t . -v`。演示命令是 `python3 -B demo.py`，会在临时仓库运行两份有限样例，并打印实测明细、原事件号和分站核账结果。

## 接纳一班观测

1. 用 `warehouse.storage.open_warehouse(path)` 打开连接，再调用 `warehouse.initialize.initialize(db)`。默认配置保存在 `settings` 表。重新初始化补齐缺项并保留已经设置的值和业务数据。调用完成后由调用方关闭连接。
2. 用 `feed.registry.SourceRegistry().register(source_id, events)` 登记有限源。`feed.events.load_events(path)` 加载样例格式的 JSON 数组。各源从 1 开始连续编号，段和事件的身份均受源号限定。
3. `feed.segments.segment(registry, source_id, start, end, segment_id=None)` 读取包含两端的范围。省略段号时使用 `start:end`。直接接纳用 `intake.admission.accept(db, batch)`，返回 `AcceptReceipt`，含本段范围、终局和发布成果号。`intake.collector.collect_segment(db, registry, source_id, start, end, segment_id=None)` 还会将成功返回的原确认交给源。可通过关键字 `receiver` 接上实际转发端口。
4. `feed.retention.discard_confirmed_prefix(stream, through=None)` 实际释放已确认事件。缺省清理到该源已收到的最高确认位置。回包丢失时可重投原包。确认后源已清理的部分仍由仓库持久恢复。在途操作对正式读口不可见。
5. `warehouse.publication.finish(db, terminal)` 消费接纳事务交付的 `DecodedEvent` 终局。已完成后可用 `AcceptReceipt.terminals` 中的同一终局再次调用，取得同一个发布对象或撤销结果 `None`。新终局由 `accept` 交付，前沿、确认和全部发布数据在该段的同一 SQLite 事务中生效。
6. `review.selection.read(db, result_id)` 按成果号返回固定明细、清单和回执。`review.station_totals.totals(db, result_id, station)` 计算分站账目。`review.reconcile.reconcile(db, result_id, station)` 另返回本站原操作及出处核对结果。源离线和同日再更正后均可按旧号领取。

## 输入和结果口径

`SourceEvent` 的必填字段为 `source_id`、`number`、`transaction_id`、`kind`、`day`。日期采用 `YYYY-MM-DD`，一笔具名事务使用同一个日期，事务名在同源内唯一。事件号为正的 SQLite 有符号整数。每源至多一笔在途事务，首个数据或保存点事件开启事务，终局须对应已有在途事务。源接纳的是串行事务，一段可以跨过事务终局并接续下一笔。

- `write` 指定 `station`、`point` 和整数 `reading`，省略或使用 `null` 表示缺测。`delete` 指定站点与测点且读数为空。日期、站点和测点构成明细键。站点、测点、来源、事务、段号及保存点名使用非空文本。
- `savepoint` 和 `rollback_to` 指定 `savepoint` 名称。回滚使用最近有效的同名实例，撤回其后操作并结束内层实例，目标可以反复回滚。`commit` 发布，`abort` 结束暂存。
- 每次提交从该日最近正式成果开始应用保留操作，其他日期独立。清单记录该笔事务中每次保留的原操作，按原事件号排序，包括同一键的重复更正和保留删除。
- 记录数包含缺测。非缺测数包含零和负数，合计按观测源整数单位精确计算。全缺测的合计为空。零行站点返回记录数 0、非缺测数 0、合计为空。

## 错误和重试

源读取区分 `UnknownSource`、`ClearedRange` 和 `InvalidRange`。新段缺口报 `SegmentGap`，同号异参报 `SegmentConflict`，非法载荷、保存点或事务顺序报 `InvalidEvent`。这些错误以及 SQLite 最终提交错误保留原前沿、暂存和正式成果。整笔撤销仍有源接纳确认。成功发布回执仅随成果产生。复核对无效参数报 `InvalidQuery`，对不存在的合法成果号报 `ResultNotFound`。错误类型位于 `contracts.py`。

`accept` 自身拥有 SQLite 最终提交边界，需要在连接无打开写事务时调用。源码中的暂存和发布协作接口使用该连接共享接纳事务。源确认使用来自接纳端的真实回包。采集端只保留单次调用所需数据，SQLite 仓库是持久接收方。工作流面向本地单源串行接纳和仓内离线复核。
