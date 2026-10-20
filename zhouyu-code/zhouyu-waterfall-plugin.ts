import type { Context } from '@deepseek-ai/cordis'

declare module '@deepseek-ai/cordis' {
  interface Events {
    'zhouyu-waterfall-event'(text: string, next: () => string): string
  }
}

export const name = 'zhouyu-waterfall-plugin'

export function apply(ctx: Context) {
  
  // 监听器A：注册在先，先拿到控制权
  ctx.on('zhouyu-waterfall-event', (text, next) => {
    if (text === 'blocked') {
      return 'refused'        // 不调 next：后面的环节全都不执行
    }
    return '[A] ' + next()    // 调 next：把后面的结果包上一层再返回
  })

  // 监听器B：注册在后
  ctx.on('zhouyu-waterfall-event', (text, next) => {
    return next() + ' [B]'
  })

  // 广播方要自己提供默认行为，也就是最后一个参数，最后一个监听器执行next()就会执行默认行为
  const result = ctx.waterfall('zhouyu-waterfall-event', 'hi', () => 'done')
  console.log('waterfall result:', result)

  const blocked = ctx.waterfall('zhouyu-waterfall-event', 'blocked', () => 'done')
  console.log('blocked result:', blocked)
}