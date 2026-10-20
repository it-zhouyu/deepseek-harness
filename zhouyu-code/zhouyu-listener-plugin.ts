import type { Context } from '@deepseek-ai/cordis'

export const name = 'zhouyu-listener-plugin'

export function apply(ctx: Context) {
  ctx.on('zhouyu-event', (count) => {
    console.log(`[zhouyu-listener-plugin] 收到第 ${count} 次广播`)
  })
}