import type { Context } from '@deepseek-ai/cordis'
import type { PreToolDecision } from '@deepseek-ai/dsh-tools'

export const name = 'tool-logger-plugin'

export function apply(ctx: Context) {
  // 观察者：每次工具执行完打印一条日志
  ctx.on('tools/result', (exec, result) => {
    console.log(`[tool-logger] ${exec.name} 执行完成`)
  })

  // 拦截者：拒绝greet工具
  ctx.on('tools/pre-execute', async (exec, next): Promise<PreToolDecision> => {
    if (exec.name === 'greet') {
      return {
        kind: 'deny',
        reason: `tool-logger: ${exec.name} is blocked by demo`,
      }
    }
    return next()
  })
}