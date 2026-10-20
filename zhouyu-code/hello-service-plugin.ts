import { Context, Service } from '@deepseek-ai/cordis'

export const name = 'hello-service-plugin'

export class CounterService extends Service {
  count = 0

  constructor(ctx: Context) {
    super(ctx, 'counter')            
  }

  increment(): number {
    return ++this.count
  }
}

// export function apply(ctx: Context) {
//   ctx.plugin(CounterService)
// }

export default CounterService