/**
 * Default Agent driver over queued turns and step-boundary input. Every request
 * is derived from the session log.
 * @module dsh-agent-loop/agent
 */

import type {
  Agent,
  AgentCancelCause,
  AgentEventDispatch,
  AgentOptions,
  AgentStatus,
  CancelOptions,
  InboxTarget,
  PreStepDecision,
  RequestErrorAction,
} from '@deepseek-ai/dsh-agent'
import { agentEvents, assembleContextFor } from '@deepseek-ai/dsh-agent'
import type { GenerateOptions, LlmCallConfig, Message, PreparedLlmCall } from '@deepseek-ai/dsh-llm'
import {
  LlmError,
  createAssistantMessage,
  errorChain,
  markAgentLoopRequest,
} from '@deepseek-ai/dsh-llm'
import { deepFreeze } from '@deepseek-ai/dsh-util-values'
import type { Scope } from '@deepseek-ai/dsh-scope'
import { createScope } from '@deepseek-ai/dsh-scope'
import type { EpochHeader, RequestContext, Session, SessionId, TurnEndReason, UserMessage } from '@deepseek-ai/dsh-session'
import { canonicalHeader, headerEquals } from '@deepseek-ai/dsh-session'
import { joinContextSections, renderContextSections, renderPrompt } from '@deepseek-ai/dsh-system-prompt'
import type { PromptAssembly } from '@deepseek-ai/dsh-system-prompt'
import type {} from '@deepseek-ai/dsh-session-projection'
import type { Context } from '@deepseek-ai/cordis'
import { ReactLoopInbox } from './inbox.ts'
import { RuntimeContextProjection } from './runtime-context.ts'
import { AssistantStreamAttempt } from './assistant-stream.ts'
import { SystemPromptProjection } from './runtime-context.ts'
import { executeToolCalls } from './tool-calls.ts'

type Phase =
  | { kind: 'idle'; lastTurn: number }
  | {
    kind: 'maintenance'
    abort: AbortController
    lastTurn: number
    wakeRequested: boolean
  }
  | { kind: 'running'; abort: AbortController; turn: number; step: number; wakeRequested: boolean }

type StepEndReason = Extract<TurnEndReason, { kind: 'completed' | 'max-tokens' }>

type PreparedStep =
  | { kind: 'reject' }
  | {
    kind: 'enter'
    messages: UserMessage[]
    startsRequestSeries?: true
    assembly: PromptAssembly
  }

/** Remove adapter-derived values before plugins propose the next request config. */
function requestProposal(header: EpochHeader): LlmCallConfig {
  if (header.adapterDefaults === undefined) return header.config
  const proposal = { ...header.config }
  if (header.adapterDefaults.reasoningEffort === true) delete proposal.reasoningEffort
  if (header.adapterDefaults.maxTokens === true) delete proposal.maxTokens
  return proposal
}

/** Drives one session through turn and step boundaries. */
export class ReactLoopAgent implements Agent {
  readonly inbox: ReactLoopInbox
  private phase: Phase
  private activityDone: Promise<void> = Promise.resolve()

  /** The agent-scoped registration boundary; the lifecycle owner unwinds it after the driver exits. */
  readonly scope: Scope
  readonly ctx: Context

  /** Fused dispatcher, built once in the constructor so hot-path dispatches never allocate. */
  private readonly dispatch: AgentEventDispatch

  /** Whether this loop instance has appended its initial/resume request anchor. */
  private requestHeaderLogged = false
  /** Surface generation at attachment or the preceding built request. */
  private requestSurfaceGeneration: number
  private readonly runtimeContext: RuntimeContextProjection
  /** Process-local revision of assistant frames for this attached Session. */
  private assistantStreamRevision = 0
  private assistantAttemptCounter = 0
  private readonly systemPrompt: SystemPromptProjection
  /** Identities fully frozen by this loop; weak references do not retain replaced history. */
  private readonly frozenMessages = new WeakSet<Message>()

  constructor(
    private loopCtx: Context,
    public readonly id: SessionId,
    public readonly options: AgentOptions,
    public readonly session: Session,
  ) {
    this.requestSurfaceGeneration = session.surface.replaceGeneration
    this.dispatch = agentEvents(loopCtx, this)

    // ScopeKey是this，所以就是当前ReactLoopAgent的实例对象
    // 里面相当于创建了agent.ctx
    this.scope = createScope(loopCtx, this)
    this.ctx = this.scope.ctx
    
    // 用来从session里获取接收到的消息
    this.inbox = new ReactLoopInbox(this.ctx.sessionProjections, session, this.dispatch)
    /* v8 ignore next -- the loop registers its own turnBoundary unit, so the key is always present */
    const lastTurn = this.loopCtx.sessionProjections.stateOf(session, 'turnBoundary')?.lastTurn ?? 0
    this.phase = { kind: 'idle', lastTurn }

    // 用来从session里获取runtimeContext和systemPrompt
    this.runtimeContext = new RuntimeContextProjection(this.ctx, session)
    this.systemPrompt = new SystemPromptProjection(session)
  }

  get status(): AgentStatus {
    return this.phase.kind === 'idle' || this.phase.kind === 'maintenance' ? 'idle' : 'running'
  }

  /** Commit a phase and publish its externally visible status transition. */
  private setPhase(next: Phase): void {
    const previousStatus = this.status
    this.phase = next
    const status = this.status
    if (status !== previousStatus) {
      this.dispatch.emit('agent/status', { status })
    }
  }

  send(message: UserMessage, target: InboxTarget, wakeup: boolean): void {
    // Waking input cannot join an aborted activity, so it starts the next turn.
    // Captured before the insertion so a reentrant cancel from a splice observer cannot reclassify it.
    const wakingAfterAbort = wakeup && this.phase.kind !== 'idle' && this.phase.abort.signal.aborted

    // steer退化成followup，已经被中断了的turn，就不要插队了
    const resolvedTarget = wakingAfterAbort ? 'next-turn' : target

    // 将message存到ReactLoopInbox，ReactLoopInbox里有两个列表，next-turn一个，next-step一个
    this.inbox.splice(resolvedTarget, Infinity, 0, [message])
    
    // 触发执行agent.ts中的kick，从而触发agent.ts中的turn
    if (wakeup) this.wakeDriver(wakingAfterAbort)
  }

  followup(input: UserMessage): void {
    this.send(input, 'next-turn', true)
  }

  steer(input: UserMessage): void {
    this.send(input, 'next-step', true)
  }

  inject(input: UserMessage): void {
    this.send(input, 'next-step', false)
  }

  cancel(cause: AgentCancelCause, options: CancelOptions = {}): void {
    if (!options.keepInbox) {
      this.inbox.clear()
      if (this.phase.kind !== 'idle') this.phase.wakeRequested = false
    }
    if (this.phase.kind !== 'idle') this.phase.abort.abort(cause)
  }

  runMaintenance<T>(job: (signal: AbortSignal) => Promise<T>): Promise<T> {
    if (this.phase.kind !== 'idle') throw new Error(`agent "${this.id}" already has active work`)
    const done = Promise.withResolvers<void>()
    const maintenance: Phase = {
      kind: 'maintenance',
      abort: new AbortController(),
      lastTurn: this.phase.lastTurn,
      wakeRequested: false,
    }
    this.setPhase(maintenance)
    this.activityDone = done.promise
    return (async () => {
      try {
        return await job(maintenance.abort.signal)
      } finally {
        this.setPhase({ kind: 'idle', lastTurn: maintenance.lastTurn })
        if (maintenance.wakeRequested && this.inbox.hasPending) this.wakeDriver()
        done.resolve()
      }
    })()
  }

  /**
   * Start one driver, or latch its wake behind maintenance or an aborted
   * activity. A wake sent while idle always opens its turn boundary, even
   * when its message was cleared; only a latched replay is suppressed when
   * the queue no longer holds the wake.
   * @param wakeAfterAbort - the {@link send} classification, captured before
   *   the inbox insertion so a reentrant cancel cannot reclassify it.
   */
  private wakeDriver(wakeAfterAbort = false): void {
    if (this.phase.kind !== 'idle') {
      // Maintenance and aborted drivers cannot deliver the wake: latch it for
      // replay at convergence. Live drivers claim queued work themselves;
      // disposal never latches, so teardown waits on no model turn.
      const reason = this.phase.abort.signal.reason as AgentCancelCause | undefined
      if (reason?.kind !== 'disposed' && (this.phase.kind === 'maintenance' || wakeAfterAbort)) {
        this.phase.wakeRequested = true
      }
      return
    }
    const driver = Promise.withResolvers<void>()
    this.activityDone = driver.promise
    this.setPhase({
      kind: 'running',
      abort: new AbortController(),
      turn: this.phase.lastTurn,
      step: 0,
      wakeRequested: false,
    })
    this.loopCtx.agents.withInitiator(this, () => this.kick()).then(driver.resolve, driver.reject)
  }

  async whenIdle(): Promise<void> {
    let activity: Promise<void>
    do {
      await (activity = this.activityDone)
    } while (activity !== this.activityDone)
  }

  /** Report one failure at its live boundary, then preserve it for driver containment. */
  private throwError(error: unknown): never {
    const turn = this.phase.kind === 'running' ? this.phase.turn : this.phase.lastTurn
    const step = this.phase.kind === 'running' ? this.phase.step : 0
    this.dispatch.emit('agent/error', { turn, step, error })
    throw error
  }

  private async kick(): Promise<void> {
    try {
      while (await this.turn()) {}
    } catch (_error) {
      // Reported failures and cancellation are contained at the driver boundary.
    } finally {
      /* v8 ignore next -- kick owns a running phase until this driver boundary */
      if (this.phase.kind === 'running') {
        const { turn, wakeRequested } = this.phase
        this.setPhase({ kind: 'idle', lastTurn: turn })
        if (wakeRequested && this.inbox.hasPending) this.wakeDriver()
      }
    }
  }

  private async preStep(target: InboxTarget, position: { turn: number; step: number }): Promise<PreparedStep> {
    /* v8 ignore next -- private callers establish the running phase before proposing a step */
    if (this.phase.kind !== 'running') throw new Error(`agent "${this.id}": pre-step outside running phase`)
    const signal = this.phase.abort.signal

    // 从inbox中获取消息，用户当前输入的Message，比如“你好”
    const claimed = this.inbox.claim(target, position.turn)

    // 组装系统提示词
    // assembly中包含
    const assembly = await this.loopCtx.systemPrompt.assemble(assembleContextFor(this, signal))
    signal.throwIfAborted()

    // 取assembly.contexts，默认有两段
    const sections = renderContextSections(assembly)

    // 把上面两段内容合起来，比"Current runtime context. This snapshot supersedes earlier runtime-context snapshots..........."
    const context = this.runtimeContext.project(joinContextSections(sections), sections)

    // 发布'agent/pre-step'事件，其他插件会监听这个事件，做一些处理，比如对messages进行修改
    // 默认情况下，decision会包含四条UserMessage，分别是：
    // 1. 用户输入的"你好""
    // 2. 一个<system-reminder> 描述AGENTS.md相关
    // 3. 动态上下文
    // 4. 一个<system-reminder> <available_skills>展示可用技能列表
    const decision = await this.dispatch.waterfall(
      'agent/pre-step', { messages: claimed, ...position, signal },
      (): Promise<PreStepDecision> => Promise.resolve<PreStepDecision>({
        kind: 'enter',
        messages: context === undefined ? claimed : [...claimed, context],
      }),
    )

    signal.throwIfAborted()
    if (decision.kind === 'reject') return decision

    // decision包含了用户提示词（含动态上下文），assembly中包含了系统提示词
    return { ...decision, assembly }
  }

  /** Whether the assembled tool schemas differ from the logged request header's. */
  private toolsChanged(tools: PromptAssembly['tools']): boolean {
    const baseline = this.session.requestHeader()
    if (baseline === undefined) return false
    return !headerEquals(baseline, canonicalHeader({ ...baseline, tools: [...tools] }))
  }

  /** Open one turn before claiming its first proposed step. */
  private async turn(): Promise<boolean> {
    if (this.phase.kind !== 'running') {
      this.throwError(new Error(`agent "${this.id}": turn without driver reservation`))
    }
    const phase = this.phase
    const { signal } = phase.abort
    signal.throwIfAborted()
    const turn = phase.turn + 1
    try {
      this.session.append('turn/start', { turn })
    } catch (error: unknown) {
      this.throwError(error)
    }
    phase.turn = turn
    let turnEnds: TurnEndReason | null = null
    let target: InboxTarget = 'next-turn'
    try {
      while (true) {
        signal.throwIfAborted()
        const step = phase.step + 1

        // 包含了用户提示词（含动态上下文）和系统提示词
        const decision = await this.preStep(target, { turn, step })

        // 被拒绝了，用户提交的 prompt 被外部 hook（如 Claude Code 的 settings.json hook / Codex config hook）显式拒绝时
        if (decision.kind === 'reject') {
          turnEnds = { kind: 'blocked' }
          return false
        }

        // turnEnds不为空，表示turn已经结束了，并且没有其他消息需要处理了，退出while
        if (turnEnds && decision.messages.length === 0) break

        // A removed waking message or an enter decision rewritten to empty
        // still owns the initial turn boundary, but it spends no model call.
        // 当前turn还没跑过step，但是消息已经没有了
        if (phase.step === 0 && decision.messages.length === 0) {
          turnEnds = { kind: 'completed' }
          return false
        }
        signal.throwIfAborted()
        this.session.append('step/start', { turn, step })
        phase.step = step
        try {
          // max-tokens is sticky: once any step hits the ceiling, later steps
          // that complete normally must not downgrade the turn outcome.
          // stepEnd如果为null，表示turn还要继续，不为null表示turn要结束了
          const stepEnd = await this.step(decision)

          // max-tokens stays sticky: a later completed step must not
          // downgrade the turn outcome.
          // 如果前一个stepEnd是'max-tokens'，那么就不再修改turnEnds了
          // 一个step如果返回了'max-tokens'，并不表示turn会结束，还得看this.inbox.nextStep有没有值，可能有steer插队进来了
          // 因为如果后面的step覆盖了turnEnds，那么就覆盖了‘max-tokens’这种情况了，turn外面就会以为内部所有step都是正常的
          if (turnEnds === null || turnEnds.kind !== 'max-tokens') turnEnds = stepEnd
        } finally {
          this.session.append('step/end', { turn, step })
        }
        signal.throwIfAborted()

        // turnEnds不为null，turn要结束了，并且inbox中没有消息了，那就确实可以结束了
        if (turnEnds && this.inbox.nextStep.length === 0) {
          await this.dispatch.serial('agent/turn-stopping', { turn, signal })
          signal.throwIfAborted()
        }

        // turnEnds不为null，turn要结束了，并且inbox中没有消息了，那就确实可以结束了，break出while
        if (turnEnds && this.inbox.nextStep.length === 0) break

        // 从next-step列表中取消息
        target = 'next-step'
      }
    } catch (error: unknown) {
      if (signal.aborted) {
        turnEnds = { kind: 'aborted', reason: signal.reason as AgentCancelCause }
        throw error
      }
      // Every failure is structured: an `LlmError` keeps its facts, anything
      // else flattens to `errorChain` text under the `UNKNOWN` code.
      turnEnds = {
        kind: 'error',
        error: error instanceof LlmError
          ? error.failure
          : { message: errorChain(error), code: 'UNKNOWN' },
      }
      this.throwError(error)
    } finally {
      try {
        // oxlint-disable-next-line typescript/no-non-null-assertion -- every exit assigns a turn ending
        this.session.append('turn/end', { turn, reason: turnEnds! })
      } catch (error: unknown) {
        this.throwError(error)
      }
    }

    // return
    if (!this.inbox.hasPending) return false
    phase.abort = new AbortController()
    // A fresh controller makes a latch set on the old one stale: the live driver claims the queue itself.
    phase.wakeRequested = false
    phase.step = 0
    return true
  }

  private async step(decision: Extract<PreparedStep, { kind: 'enter' }>): Promise<StepEndReason | null> {
    /* v8 ignore next -- private callers establish the running phase before executing a step */
    if (this.phase.kind !== 'running') throw new Error(`agent "${this.id}": step outside running phase`)
    const { turn, step, abort: { signal } } = this.phase
    signal.throwIfAborted()

    const { assembly } = decision

    // 把assembly中的session拼起来，也就是得到最终的系统提示词
    const renderedPrompt = renderPrompt(assembly)

    let firstAttempt = true

    // 这个while是用来进行重试的
    while (true) {
      // 模型的信息
      const { config, preparedCall } = await this.prepareRequest(turn, step, signal)
      const startsRequestSeries = firstAttempt && decision.startsRequestSeries === true


      const commits = this.systemPrompt.project(renderedPrompt, {
        inHistory: preparedCall?.systemPromptUpdate === 'in-history',
        startsSeries: startsRequestSeries
          || this.requestSurfaceGeneration !== this.session.surface.replaceGeneration
          || this.toolsChanged(assembly.tools),
      })

      // 把系统提示词存到Session
      for (const { message, intent } of commits) {
        this.session.append('system/message', { turn, step, message }, intent)
      }

      // 把所有UserMessage存到Session
      if (firstAttempt) {
        for (const message of decision.messages) {
          this.session.append('user/message', message, { surfaceOp: 'append' })
        }
      }
      firstAttempt = false
      const request = this.buildRequest(config, preparedCall, assembly.tools, startsRequestSeries, signal)

      // 每尝试调用一次大模型，就会构造一个新的live对象，live对象里有usage、ContentBlock
      const live = new AssistantStreamAttempt(
        this.session.id,
        ++this.assistantAttemptCounter,
        () => ++this.assistantStreamRevision,
        turn,
        step,
        (frame) => { this.dispatch.emit('agent/assistant-stream', { frame }) },
      )
      let started = false

      try {
        // 具体会调用到LlmAdapter的流式调用，比如llm-deepseek中的DeepSeekAdapter
        const stream = preparedCall?.stream(request) ?? this.loopCtx.llm.stream(request)
        signal.throwIfAborted()

        live.start()
        started = true
        // 开始接受响应chunk
        for await (const chunk of stream) {
          signal.throwIfAborted()
          // push里面会发agent/assistant-stream事件，在上面new AssistantStreamAttempt时定义的，session-controller中的history.ts中会监听该事件，最终通过websocket发送给浏览器
          live.push(chunk)
        }
        signal.throwIfAborted()
      } catch (error: unknown) {
        if (!started) throw error
        try {
          if (signal.aborted) {
            const content = live.interruptedBlocks()
            if (content.length > 0) {
              live.settle('assistant/message', () => this.session.append('assistant/message', {
                turn,
                step,
                message: createAssistantMessage({
                  content,
                  source: {
                    provider: request.provider,
                    model: request.model,
                    ...live.replayState === undefined ? {} : { replayState: live.replayState },
                  },
                }),
                interrupted: true,
                ...live.usage === undefined ? {} : { usage: live.usage },
                stream: live.stream,
              }, { surfaceOp: 'append' }).seq)
            } else {
              live.settle(
                'assistant/attempt',
                () => this.session.append('assistant/attempt', { turn, step, stream: live.stream }).seq,
              )
            }
          } else {
            live.settle(
              'assistant/attempt',
              () => this.session.append('assistant/attempt', { turn, step, stream: live.stream }).seq,
            )
          }
        } catch (settlementError: unknown) {
          throw new AggregateError(
            [error, settlementError],
            'Assistant stream failed and its durable settlement was rejected',
            { cause: error },
          )
        }
        throw error
      }

      try {
        const finish = live.finish
        // 如果响应返回的是错误，那就发布'agent/request-error'事件
        if (finish.kind === 'error' || finish.kind === 'aborted') {
          live.settle(
            'assistant/attempt',
            () => this.session.append('assistant/attempt', { turn, step, stream: live.stream }).seq,
          )
          const action = await this.dispatch.waterfall(
            'agent/request-error', {
              turn,
              step,
              provider: request.provider,
              failure: finish.failure,
              retryPolicy: preparedCall?.retryPolicy,
              signal,
            },
            () => Promise.resolve<RequestErrorAction>(undefined),
          )
          signal.throwIfAborted()
          if (action?.kind !== 'retry') {
            throw new LlmError(finish.failure.message, finish.failure.code, finish.failure)
          }
          continue
        }

        // 合并chunk得到完整的AssistantMessage
        const message = createAssistantMessage({
          content: live.blocks(),
          source: {
            provider: request.provider,
            model: request.model,
            ...live.replayState !== undefined ? { replayState: live.replayState } : {},
          },
        })
        live.settle(
          'assistant/message',
          () => this.session.append('assistant/message', {
            turn,
            step,
            message,
            ...live.usage === undefined ? {} : { usage: live.usage },
            stream: live.stream,
          }, { surfaceOp: 'append' }).seq,
        )

        // 如果响应说是达到了'max-tokens'
        if (finish.kind === 'max-tokens') return { kind: 'max-tokens' }

        // AssistantMessage实际是一次tool-call
        const toolCalls = message.content.filter(block => block.type === 'tool-call')

        // 没有tool-call就直接返回
        if (toolCalls.length === 0) return { kind: 'completed' }

        // 有tool-call就执行工具
        // concluded表示这批工具结果里有没有人宣告turn可以到此结束，比如某些工具的执行结果想直接返回给我用户，不需要再经过大模型了
        const { concluded } = await executeToolCalls(
          this.loopCtx, turn, step, toolCalls, signal,
          context => this.inbox.splice('next-step', this.inbox.nextStep.length, 0, [context]),
        )

        // concluded为true，表示turn可以直接解读，此时返回{ kind: 'completed' }，否则返回null，表示turn还要继续
        return concluded ? { kind: 'completed' } : null
      } catch (error: unknown) {
        if (!live.ended) live.abandon()
        throw error
      }
    }
  }

  /** Resolve request config and bind its adapter before admitting model-visible input. */
  private async prepareRequest(
    turn: number,
    step: number,
    signal: AbortSignal,
  ): Promise<{ config: LlmCallConfig; preparedCall?: PreparedLlmCall }> {
    const { session } = this

    // A loop instance starts from its declared route, restoring only an explicit
    // effort owned by that exact model. Later steps re-resolve marked defaults.
    // 这里相当于拿的是上一次请求的header
    const persistedHeader = session.requestHeader()
    const persistedConfig = persistedHeader?.config

    // 当前Agent里选择的模型
    const route = { provider: this.options.provider ?? '', model: this.options.model ?? '' }
    // 上一次请求选择的思考级别
    const persistedReasoningEffort = persistedConfig?.provider === route.provider
      && persistedConfig.model === route.model
      && persistedHeader?.adapterDefaults?.reasoningEffort !== true
      ? persistedConfig.reasoningEffort
      : undefined
    const reasoningEffort = this.options.reasoningEffort ?? persistedReasoningEffort
    const maxTokens = this.options.maxTokens

    // 兜底配置
    const seedConfig = deepFreeze(structuredClone(
      this.requestHeaderLogged
        // oxlint-disable-next-line typescript/no-non-null-assertion -- the instance logged the header it now folds
        ? requestProposal(persistedHeader!)
        : {
          ...route,
          ...reasoningEffort === undefined ? {} : { reasoningEffort },
          ...maxTokens === undefined ? {} : { maxTokens },
        },
    ))

    // 发送'agent/request'事件，事件瀑布决定用哪个provider和model来调用大模型
    // 如果没有插件监听这个事件，那么就采用seedConfig，里面包含了provider和model、思考级别、maxTokens
    // model-selection.ts中监听了这个事件，里面会根据用户选择的模型来决定provider和model
    const proposedConfig = await this.dispatch.waterfall(
      'agent/request', { turn, step, signal },
      () => Promise.resolve(seedConfig),
    )

    signal.throwIfAborted()
    if (!proposedConfig.provider || !proposedConfig.model) {
      throw new Error(`agent "${this.id}" has no provider/model: set AgentOptions.provider and AgentOptions.model or supply both via the agent/request waterfall`)
    }
    let config: LlmCallConfig
    let preparedCall: PreparedLlmCall | undefined

    try {
      // proposedConfig就是确定要用的模型以及相关信息，利用llm服务来预处理
      preparedCall = await this.loopCtx.llm.prepareCall(proposedConfig, signal)
      config = preparedCall.config
    } catch (error: unknown) {
      // Middleware may serve an unregistered route; terminal dispatch still requires an adapter.
      if (!(error instanceof LlmError) || error.code !== 'NO_ADAPTER') throw error
      config = proposedConfig
    }

    signal.throwIfAborted()
    return { config, ...preparedCall === undefined ? {} : { preparedCall } }
  }

  /** Log the resolved envelope and derive a frozen request from the admitted surface. */
  private buildRequest(
    config: LlmCallConfig,
    preparedCall: PreparedLlmCall | undefined,
    tools: GenerateOptions['tools'] & object,
    startsRequestSeries: boolean,
    signal: AbortSignal,
  ): GenerateOptions {
    const { session } = this
    const surfaceGeneration = session.surface.replaceGeneration

    // 构造请求头并存到session
    const header = canonicalHeader({
      config,
      ...preparedCall === undefined ? {} : { adapterDefaults: preparedCall.adapterDefaults },
      ...tools.length > 0 ? { tools } : {},
    })
    const baseline = this.session.requestHeader()
    const startsSeries = startsRequestSeries
      || this.requestSurfaceGeneration !== surfaceGeneration
    if (!this.requestHeaderLogged) {
      this.session.append('request/header', { header, reason: baseline === undefined ? 'initial' : 'resume' })
      this.requestHeaderLogged = true
    } else if (baseline === undefined || !headerEquals(baseline, header)) {
      this.session.append('request/header', {
        header,
        reason: 'change',
        ...startsSeries ? { startsSeries: true } : {},
      })
    } else if (startsSeries) {
      this.session.append('request/header', { header, reason: 'series' })
    }
    this.requestSurfaceGeneration = surfaceGeneration

    // 构造请求context并存到session，请求context并不是请求body
    const contextWindow = preparedCall?.context?.contextWindow
    const systemPromptUpdate = preparedCall?.systemPromptUpdate
    const requestContext: RequestContext = {
      provider: config.provider,
      model: config.model,
      ...contextWindow === undefined ? {} : { contextWindow },
      ...systemPromptUpdate === undefined ? {} : { systemPromptUpdate },
    }
    const previousContext = session.requestContext()
    if (previousContext?.provider !== requestContext.provider
      || previousContext.model !== requestContext.model
      || previousContext.contextWindow !== requestContext.contextWindow
      || previousContext.systemPromptUpdate !== requestContext.systemPromptUpdate) {
      session.append('request/context', requestContext)
    }
    signal.throwIfAborted()

    // canonicalHeader is shallow; append logs a detached snapshot, not these local values.
    deepFreeze(header)

    // 从session得到需要发送给大模型的消息，系统提示词和用户提示词
    const boundaryMessages = session.deriveMessages()
    for (const message of boundaryMessages) {
      if (this.frozenMessages.has(message)) continue
      deepFreeze(message)
      this.frozenMessages.add(message)
    }
    Object.freeze(boundaryMessages)

    const request = markAgentLoopRequest(Object.freeze({
      ...header.config,
      messages: boundaryMessages,
      ...header.tools !== undefined ? { tools: header.tools } : {},
      sessionId: this.session.id,
      signal,
    }))
    return request
  }
}
