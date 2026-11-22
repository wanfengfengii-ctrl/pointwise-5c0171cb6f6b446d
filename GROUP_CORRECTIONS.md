# 带前像的勘误组

夜班在同一日账上办理各源补送的勘误。实收使供数站能清理已确认的源段。正式日账在联合发账时变化，夜班能凭组号领取这次实际采用的次序、成员成果和逐笔前像。

## 勘误实收

`intake.admission.accept_correction(db, group_id, batch, before=None) -> AcceptReceipt`。沿用 README.md 的连接、初始化、名称、事件、日期和连续段规则。`group_id` 为非空文本。`batch` 是一个 `SourceSegment`，从该源下一未收事件起覆盖一笔同日完整事务，最后一项为唯一终局 `commit`。勘误事务从本段开始，沿用源内事务名唯一和源内事务串行的规则。保存点裁决沿用 V1。

`before` 是从本段写或删的原事件号到 `(present, reading)` 二元组的字典。`present` 为布尔值。`(False, None)` 表示缺行，`(True, None)` 表示现有行缺测，`(True, 0)` 表示现有行读数为零。其他现有读数沿用 V1 整数规则。省略某事件号表示这笔没有前像要求。`None` 和空字典都表示整件无前像要求。所有条件都纳入来件身份。保存点撤回的操作不参与发账时的前像核对。其条件仍须绑定到本段合法写删事件，参数格式仍须正确。

成功确认与实收原件、前像条件和连续前沿一起生效。确认中有原提交终局，`result_ids` 为空。沿用 `collect_segment` 的 `receiver` 接口转交确认，或在收到确认后调用 `feed.retention.acknowledge`，随后按 V1 清源。回包丢失时可用缓存的同一原段及条件重投。重投返回原确认，即使该件后来已入账。待发勘误不占用该源下一笔普通事务的暂存位置，也不能通过 `warehouse.publication.finish` 单件发账。未联合发布时直接交该终局给 `finish` 报 `InvalidEvent`，发布后领取其正式成员成果。

同一组每源限一件。段身份仍为 `(source_id, segment_id)`，普通接纳和勘误实收共用这个身份空间。段号改绑模式、组号、内容或条件，或同组同源另送一段，报 `SegmentConflict`。组已成功发布后只允许原实收同参重投。其他非法段、断号、非法操作、复用事务名依 V1 报错，非法前像参数报 `InvalidEvent`。失败不占段号、不推进前沿、不产生新确认。

## 联合发账与领取

`warehouse.publication.publish_group(db, group_id, base_id, members) -> dict`。`base_id` 是所选同日现行成果号。`members` 为非空列表或元组，元素为 `(source_id, segment_id)` 二元组，每源一次。成员参数表示集合，输入次序不参与请求身份或指定办理次序。名称、成果号或成员参数格式错误报 `InvalidQuery`。

- `{'status': 'pending'}` 表示所指定的段有尚未实收的。已知段属于其他组、属于普通接纳或与底账不同日，或者底账号不存在或已经非现行，返回 `{'status': 'conflict'}`。已知冲突优先于待齐。
- `{'status': 'unsatisfiable'}` 表示指定件齐备，但从该底账找不到兑现全部件的合法次序。每件完整依原序执行。每笔有效写删在当时账面核对其条件并随即改变本次试办账面。无条件写删依 V1，含删除原本缺席的行。可以采用任一可行全序。
- 成功时返回 `{'status': 'published', 'group_id': group_id, 'base_id': base_id, 'members': [...]}`。成员列表按实际入账次序排列。每个成员对象含 `source_id`、`segment_id`、`transaction_id`、`terminal_number`、`result_id` 和 `before`。成员的 `before` 是按原操作顺序排列的 `(event_number, (present, reading))` 列表，记录包括无条件操作在内的每笔有效写删的实际前像。各 `result_id` 都能由 V1 `read` 领取，含办到该件为止的正式明细、该件原有效操作清单及其发布回执。没有有效写删的合法成员也取得成果号。尾件成为该日现行账，后续普通提交从该账继续。

只对指定成员办账。一次成功的联合发账确定本组唯一首执。成功后先核请求身份，同号同底账同成员集合即返原首执，即使现行账已推进。换底账或换成员返回 `conflict`。待齐、无解和冲突不占发布号。SQLite 提交异常照传，失败不改变已实收、正式账或组首执。一次成功提交同时产生全部成员账及组首执。`warehouse.publication.read_group(db, group_id)` 按组号领相同的成功回执，未发布返回 `None`。关闭并重开仓库后，这两个领取路径均无需源流。继续保留普通 `accept`、`finish` 和历史读回。

## 交班样例

`samples/group_corrections.json` 提供普通底账、三份勘误原件和条件。`group_fixture.load_case()` 返回登记源、底账段、组号及按到件顺序排列的 `(batch, before)`。先用普通入口办底账得 `east/x=0`。A、B、C 分别实收后，三源可清到事件 2，底账仍为 0。关库重开，用原底账号及三份已收段的身份发账。应领 B、C、A，成员账的 x 依次为 2、0、1。三份原写入事件都是各源的 1 号，实际前像为 `(True, 0)`、`(True, 2)`、`(True, 0)`。原底账仍按原号读取。A 的第 3、4 号另组成普通事务，写入 `east/y=7`。可在联合发账前接纳第 3 号，在发账后接纳第 4 号，现行账仍保留联合发账的 `x=1`。

另一核账例从 x=0 开始，A 无条件写 1，B 无条件写 0。两种次序都可发账，按回执次序核对实际前像、成员和尾账。换序重投同一成员集合应取已有首执。若组内某件先将缺行 z 写为缺测、再以缺测前像写 0、最后以零前像删 z，应依次记 `(False, None)`、`(True, None)`、`(True, 0)`。在保存点内对 x 试写并附不匹配的条件，若试写后来已撤，则该条件不阻碍发布且试写不列入成员清单。

`group_fixture.drop_reply` 在被包装的调用成功后丢弃回包。`tests.support.RejectCommit` 可拒绝连接的最终提交。两者可用于核验清源资格、失败重试及成功后的原号复领。`python3 -B group_fixture.py` 在内存仓中检查样例原段及 V1 的普通发布链。

沿用 `.pointwise-runtime.json` 的版本 1、`cli` 类型及数组形式。`setup` 仍是命令数组的列表，`verify` 和 `demo` 分别是一条命令的参数数组。交付时让核验入口运行完整原回归和本业务核验，异常退出非零。演示入口运行真实接纳、清源、重开、联合发账和按号领取，展示实际成员原号、前像和尾账。