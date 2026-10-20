import type { Context } from '@deepseek-ai/cordis'

declare module '@deepseek-ai/cordis' {
  interface Events {
    'zhouyu-event'(count: number): void
  }
}

export const name = 'zhouyu-event-plugin'

// export function apply(ctx: Context) {
//   let count = 0
  
//   const timer = setInterval(() => {
//     count += 1
//     ctx.emit('zhouyu-event', count)
//   }, 3000)
  
//   // 返回一个函数，插件卸载时会执行它，用来清理定时器，有问题
//   return () => clearInterval(timer)
// }

// 正确方式一
// export const apply = (ctx: Context) => {
//   let count = 0
//   const timer = setInterval(() => { count += 1; ctx.emit('zhouyu-event', count) }, 3000)
//   return () => clearInterval(timer)
// }

// 正确方式二
// export function apply(ctx: Context) {
//   let count = 0

//   ctx.effect(() => {
//     const timer = setInterval(() => {
//       count += 1
//       ctx.emit('zhouyu-event', count)
//     }, 3000)

//     return () => clearInterval(timer)
//   })
// }


export function apply(ctx: Context) {
  let count = 0
  
  // ctx.effect(execute)：execute 立刻执行，做的正是“正操作”；
  // 它 yield / 返回的函数都是“逆操作”，插件卸载时按反序自动执行。
  // 用 generator 就能在一次 effect 里登记多条逆操作。
  ctx.effect(function* () {
    // 正操作 1：启动定时器
    const timer = setInterval(() => {
      count += 1
      console.log(`[zhouyu-event-plugin] 第 ${count} 次 tick`)
    }, 3000)

    // 逆操作 1：清掉定时器
    yield () => {
      clearInterval(timer)
      console.log(`[zhouyu-event-plugin] 定时器已撤销（共 tick ${count} 次）`)
    }

    // 正操作 2：写一个锁文件，表示这个插件正在运行
    console.log(`[zhouyu-event-plugin] 锁文件已创建`)

    // 逆操作 2：卸载时按反序执行，所以这条比定时器先撤
    yield () => {
      console.log('[zhouyu-event-plugin] 锁文件已删除')
    }
  })
}