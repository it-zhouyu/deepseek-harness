import type { Context } from '@deepseek-ai/cordis'

export const name = 'use-counter-plugin'
export const inject = ['counter']

export function apply(ctx: Context) {
  console.log('count:', ctx.counter.increment())
}