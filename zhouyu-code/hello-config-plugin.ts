import type { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'

export const name = 'hello-config-plugin'

// 配置的类型定义
export interface HelloConfig {
  message: string
}

// 配置的校验 schema：cordis.yml 里写的 config 会经过它校验和补全默认值
export const Config: z<HelloConfig> = z.object({
  message: z.string().default('hello config plugin!'),
})

// apply 的第二个参数就是解析后的配置
export function apply(ctx: Context, config: HelloConfig) {
  console.log(`[hello-config-plugin] ${config.message}`)
}