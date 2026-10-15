import type { Context } from '@deepseek-ai/cordis'

export default class HelloPlugin {
    static name = 'hello-plugin'

    constructor(ctx: Context) {
        console.log('[hello-plugin] plugin loaded!')
    }
}