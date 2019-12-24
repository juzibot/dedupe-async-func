import { DelayQueueExecutor } from 'rx-queue'

export interface DedupeFuncOptions {
  forceCall?: boolean,
  expireIn?: number,
}

const DEFAULT_OPTIONS: DedupeFuncOptions = {
  forceCall: false,
  expireIn: 10 * 1000,
}

interface FuncCall {
  timestamp: number,
  expiredTimestamp: number,
  returned: boolean,
  result?: any,
  listener: PendingApiCall[],
}

interface PendingApiCall {
  resolve: (data: any) => void,
  reject: (e: any) => void,
}

const PRE = 'DedupeApi'

/**
 * This class will dedupe api calls
 * Multiple calls within a period of time will only execute the function once,
 * all the other calls will get the same response as the executed one
 */
export class DedupeFunc {

  private callPool: {
    [key: string]: FuncCall
  }

  private cleaner: NodeJS.Timer

  private apiQueue: DelayQueueExecutor

  constructor () {
    this.callPool = {}
    this.cleaner = setInterval(this.cleanData, EXPIRE_TIME * 1000)
    this.apiQueue = new DelayQueueExecutor(200)
  }

  public async dedupeFunc<T, P extends {}> (
    func: (params: P) => Promise<T>,
    params: P,
    dedupeOptions?: DedupeFuncOptions,
  ): Promise<T> {

    let key: string;
    try {
      key = this.getKey(params);
    } catch (e) {
      throw new Error(`The params passed in must be able to be stringified, please check your params passed in.`);
    }

    console.debug(PRE, `dedupeFunc(${JSON.stringify(params)})`);

    const { forceCall, expireIn } = {...DEFAULT_OPTIONS, ...dedupeOptions};

    if (forceCall) {
      delete this.callPool[key]
    }

    const existCall = this.callPool[key]
    const now = new Date().getTime()
    if (existCall && now < existCall.expiredTimestamp) {
      if (existCall.returned) {
        console.debug(PRE, `dedupeApi(${JSON.stringify(params)}) dedupe api call with existing results.`)
        return existCall.result
      } else {
        console.debug(PRE, `dedupeApi(${JSON.stringify(params)}) dedupe api call with pending listener.`)
        return new Promise((resolve, reject) => {
          existCall.listener.push({
            reject,
            resolve,
          })
        })
      }
    } else {
      console.debug(PRE, `dedupeApi(${JSON.stringify(params)}) dedupe api call missed, call the external service.`)
      this.callPool[key] = {
        listener: [],
        returned: false,
        timestamp: now,
        expiredTimestamp: now + expireIn,
      }
      let result: any
      try {
        result = await this.apiQueue.execute(() => func(params))
      } catch (e) {
        console.debug(PRE, `dedupeApi(${JSON.stringify(params)}) failed from external service, reject ${this.callPool[key].listener.length} duplicate api calls.`)
        this.callPool[key].listener.map(api => {
          api.reject(e)
        })
        this.callPool[key].listener = []
        throw e
      }

      this.callPool[key].result = result
      this.callPool[key].returned = true
      console.debug(PRE, `dedupeApi(${JSON.stringify(params)}) got results from external service, resolve ${this.callPool[key].listener.length} duplicate api calls.`)
      this.callPool[key].listener.map(api => {
        api.resolve(result)
      })
      this.callPool[key].listener = []

      return result
    }
  }

  public clean () {
    for (const key in this.callPool) {
      if (this.callPool.hasOwnProperty(key)) {
        this.callPool[key].listener.forEach(api => api.reject('Clean up func calls.'))
        delete this.callPool[key]
      }
    }
    clearInterval(this.cleaner)
  }

  /**
   * Get rid of data in pool that exists for more than EXPIRE_TIME
   */
  private cleanData () {
    const now = new Date().getTime()
    for (const key in this.callPool) {
      if (this.callPool.hasOwnProperty(key)) {
        const apiCache = this.callPool[key]
        if (apiCache.expiredTimestamp > now) {
          delete this.callPool[key]
        }
      }
    }
  }

  private getKey<T extends {}> (params: T) {
    return JSON.stringify(params);
  }

}
