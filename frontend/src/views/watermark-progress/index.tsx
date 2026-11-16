import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/watermark-progress'

type LateEvent = {
  事件标识: string
  事件时间: string
  进入缓冲时间: string
  当前水位: string | null
  原因: string
}

type TriggerWindow = {
  窗口编号: string
  窗口开始: string | null
  窗口结束: string
  窗口状态: string
  收口水位: string | null
  收口版本: number | null
  收口时间: string | null
}

type FailureItem = {
  事件标识: string
  失败原因: string
  最近失败原因: string
  重试次数: number
}

type SyncLink = {
  链路名称: string
  消费侧版本: number
  最近同步版本: number | null
  最近同步时间: string | null
}

type ProgressRecord = {
  id: number
  配置编号: string
  流名称: string | null
  时间字段: string
  空闲超时: string
  延迟容忍: string
  版本标识: number
  可确认水位: string | null
  观察最大事件时间: string | null
  最近事件时间: string | null
  最近心跳时间: string | null
  空闲状态: string
  迟到事件缓冲: LateEvent[]
  触发窗口: TriggerWindow[]
  失败项: FailureItem[]
  最近推进原因: string
  同步链路: SyncLink[]
  更新时间: string | null
}

type LogEntry = { at: string; ok: boolean; text: string }

const listColumns = [
  '配置编号',
  '流名称',
  '时间字段',
  '空闲超时',
  '延迟容忍',
  '版本标识',
  '可确认水位',
  '空闲状态',
]

function nowText(): string {
  return new Date().toISOString().slice(0, 19)
}

export default function WatermarkProgressView() {
  const [rows, setRows] = useState<ProgressRecord[]>([])
  const [total, setTotal] = useState(0)
  const [keyword, setKeyword] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [selectedCode, setSelectedCode] = useState('')
  const [detail, setDetail] = useState<ProgressRecord | null>(null)
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [eventText, setEventText] = useState('[{"event_id":"evt-1","ts":"2026-11-16T10:00:00"}]')
  const [tickTime, setTickTime] = useState(nowText())
  const [windowCode, setWindowCode] = useState('WIN-24')
  const [windowEnd, setWindowEnd] = useState('2026-11-16T10:05:00')
  const [linkName, setLinkName] = useState('downstream-a')
  const [useVersion, setUseVersion] = useState(false)

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(keyword ? { keyword } : {}).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('水位推进数据记录列表读取失败')
      }
      const payload = await response.json()
      const items = (payload.items ?? []) as ProgressRecord[]
      setRows(items)
      setTotal(payload.total ?? 0)
      if (items.length > 0) {
        setSelectedCode((prev) => prev || items[0].配置编号)
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '水位推进数据记录读取失败')
    }
  }, [keyword])

  const reloadDetail = useCallback(async (code: string) => {
    if (!code) {
      setDetail(null)
      return
    }
    try {
      const response = await request(`${ENDPOINT}/${code}`)
      if (response.status === 404) {
        setDetail(null)
        return
      }
      if (!response.ok) {
        throw new Error('推进记录明细读取失败')
      }
      setDetail((await response.json()) as ProgressRecord)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '推进记录明细读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  useEffect(() => {
    void reloadDetail(selectedCode)
  }, [selectedCode, reloadDetail])

  const versionPayload = useCallback(
    (extra: Record<string, unknown>): Record<string, unknown> => {
      if (useVersion && detail) {
        return { ...extra, expected_version: detail.版本标识 }
      }
      return extra
    },
    [useVersion, detail],
  )

  const pushLog = useCallback((ok: boolean, text: string) => {
    setLogs((prev) => [{ at: nowText(), ok, text }, ...prev].slice(0, 20))
  }, [])

  const postAction = useCallback(
    async (path: string, body: Record<string, unknown>, successHint: string) => {
      try {
        const response = await request(`${ENDPOINT}/${path}`, {
          method: 'POST',
          body: JSON.stringify(body),
        })
        const payload = await response.json()
        const ok = Boolean(payload.ok)
        pushLog(ok, `${successHint}：${payload.message ?? ''}`)
        if (!ok) {
          setErrorMessage(payload.message ?? '动作未生效')
        }
        await reload()
        await reloadDetail(selectedCode)
      } catch (error) {
        const text = error instanceof Error ? error.message : '请求未送达'
        setErrorMessage(text)
        pushLog(false, `${successHint}失败：${text}`)
      }
    },
    [pushLog, reload, reloadDetail, selectedCode],
  )

  const stats = useMemo(() => {
    const buffered = rows.reduce((sum, row) => sum + row.迟到事件缓冲.length, 0)
    const waiting = rows.reduce(
      (sum, row) => sum + row.触发窗口.filter((w) => w.窗口状态 === '等待触发').length,
      0,
    )
    const failed = rows.reduce((sum, row) => sum + row.失败项.length, 0)
    return [
      { label: '推进记录总数', value: total },
      { label: '迟到事件缓冲量', value: buffered },
      { label: '等待触发窗口', value: waiting },
      { label: '待重试失败项', value: failed },
    ]
  }, [rows, total])

  function handleIngest() {
    let events: unknown
    try {
      events = JSON.parse(eventText)
    } catch {
      setErrorMessage('事件 JSON 解析失败，请检查格式')
      return
    }
    if (!Array.isArray(events)) {
      setErrorMessage('事件必须是数组结构')
      return
    }
    void postAction(
      `${selectedCode}/events`,
      versionPayload({ events, processed_at: nowText() }),
      '写入事件',
    )
  }

  return (
    <section className="page" data-module="watermark-progress">
      <header className="page-head">
        <div>
          <h2>水位推进数据记录</h2>
          <p className="page-desc">
            读取 WATE 配置的时间字段、空闲超时与延迟容忍，空闲超时后形成可确认水位记录，
            同一份记录保留可确认水位、迟到事件缓冲与触发窗口，并以版本标识支持并发控制与跨链路同步。
          </p>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void reload()
        }}
      >
        <label className="filter-item">
          <span>配置编号</span>
          <input value={keyword} placeholder="如 WATE-0002" onChange={(e) => setKeyword(e.target.value)} />
        </label>
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setKeyword('')}>重置条件</button>
        <label className="filter-item">
          <span>
            <input
              type="checkbox"
              checked={useVersion}
              onChange={(e) => setUseVersion(e.target.checked)}
            />{' '}
            写操作携带当前版本标识（模拟乐观并发）
          </span>
        </label>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {listColumns.map((column) => <th key={column}>{column}</th>)}
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)} className={row.配置编号 === selectedCode ? 'row-selected' : ''}>
              {listColumns.map((column) => <td key={column}>{String(row[column as keyof ProgressRecord] ?? '—')}</td>)}
              <td className="row-actions">
                <button className="link" type="button" onClick={() => setSelectedCode(row.配置编号)}>
                  查看推进记录
                </button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={listColumns.length + 1}>
                暂无推进记录，可在下方对某个 WATE 配置写入事件或发送空闲心跳后生成
              </td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条水位推进数据记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>

      {selectedCode && (
        <div className="progress-panel">
          <h3>配置 {selectedCode} 的推进判定与窗口收口</h3>

          <div className="action-grid">
            <div className="action-card">
              <h4>写入事件（校验时间字段）</h4>
              <textarea rows={4} value={eventText} onChange={(e) => setEventText(e.target.value)} />
              <button className="btn primary" type="button" onClick={handleIngest}>写入事件</button>
            </div>

            <div className="action-card">
              <h4>空闲心跳（达到空闲超时才推进）</h4>
              <input value={tickTime} onChange={(e) => setTickTime(e.target.value)} />
              <button
                className="btn primary"
                type="button"
                onClick={() =>
                  void postAction(`${selectedCode}/tick`, versionPayload({ now: tickTime }), '空闲心跳')
                }
              >
                发送心跳
              </button>
            </div>

            <div className="action-card">
              <h4>窗口登记</h4>
              <input placeholder="窗口编号" value={windowCode} onChange={(e) => setWindowCode(e.target.value)} />
              <input placeholder="窗口结束时间" value={windowEnd} onChange={(e) => setWindowEnd(e.target.value)} />
              <button
                className="btn primary"
                type="button"
                onClick={() =>
                  void postAction(
                    `${selectedCode}/windows`,
                    versionPayload({
                      values: { 窗口编号: windowCode, 窗口结束: windowEnd },
                    }),
                    '登记窗口',
                  )
                }
              >
                登记窗口
              </button>
            </div>

            <div className="action-card">
              <h4>窗口收口 / 重试 / 同步</h4>
              <button
                className="btn"
                type="button"
                onClick={() =>
                  void postAction(
                    `${selectedCode}/windows/${windowCode || 'WIN-24'}/close`,
                    versionPayload({}),
                    '窗口收口',
                  )
                }
              >
                收口窗口 {windowCode || 'WIN-24'}
              </button>
              <button
                className="btn"
                type="button"
                onClick={() => void postAction(`${selectedCode}/retry`, { processed_at: nowText() }, '重试失败项')}
              >
                重试失败项
              </button>
              <input placeholder="链路名称" value={linkName} onChange={(e) => setLinkName(e.target.value)} />
              <button
                className="btn"
                type="button"
                onClick={() =>
                  void postAction(
                    `${selectedCode}/sync`,
                    { link_name: linkName, consumer_version: detail?.版本标识 ?? 0 },
                    '跨链路同步',
                  )
                }
              >
                同步到链路
              </button>
            </div>
          </div>

          {detail && (
            <div className="detail-grid">
              <article className="detail-card">
                <h4>可核对结果</h4>
                <p>可确认水位：<strong>{detail.可确认水位 ?? '尚未推进'}</strong></p>
                <p>版本标识：{detail.版本标识}　空闲状态：{detail.空闲状态}</p>
                <p className="muted">最近推进原因：{detail.最近推进原因}</p>
                <p className="muted">观察最大事件时间：{detail.观察最大事件时间 ?? '—'}</p>
              </article>

              <article className="detail-card">
                <h4>迟到事件缓冲（{detail.迟到事件缓冲.length}）</h4>
                {detail.迟到事件缓冲.length === 0 && <p className="muted">暂无迟到事件</p>}
                {detail.迟到事件缓冲.map((item) => (
                  <p key={item.事件标识}>
                    {item.事件标识}　{item.事件时间}
                    <span className="muted">（{item.原因}）</span>
                  </p>
                ))}
              </article>

              <article className="detail-card">
                <h4>触发窗口（{detail.触发窗口.length}）</h4>
                {detail.触发窗口.length === 0 && <p className="muted">暂无登记窗口</p>}
                {detail.触发窗口.map((item) => (
                  <p key={item.窗口编号}>
                    {item.窗口编号}　<span className={item.窗口状态 === '已触发收口' ? 'ok-text' : ''}>{item.窗口状态}</span>
                    <span className="muted">（结束 {item.窗口结束}）</span>
                  </p>
                ))}
              </article>

              <article className="detail-card">
                <h4>失败项 / 同步链路</h4>
                {detail.失败项.length === 0 ? (
                  <p className="muted">无待重试失败项</p>
                ) : (
                  detail.失败项.map((item, index) => (
                    <p key={`${item.事件标识}-${index}`} className="error-text">
                      {item.事件标识}：{item.最近失败原因}（重试 {item.重试次数} 次）
                    </p>
                  ))
                )}
                {detail.同步链路.length === 0 ? (
                  <p className="muted">暂无同步链路</p>
                ) : (
                  detail.同步链路.map((item) => (
                    <p key={item.链路名称}>
                      {item.链路名称}：消费侧版本 {item.消费侧版本}
                    </p>
                  ))
                )}
              </article>
            </div>
          )}

          <div className="log-box">
            <h4>操作日志（保留最近 20 条）</h4>
            {logs.length === 0 && <p className="muted">暂无操作记录</p>}
            {logs.map((log, index) => (
              <p key={index} className={log.ok ? 'ok-text' : 'error-text'}>
                {log.at}　{log.text}
              </p>
            ))}
          </div>
        </div>
      )}
    </section>
  )
}
