import { randomUUID } from "node:crypto";
import redisConnection from "../../shared/config/redis"


const lockttlseconds = 300

export type StepLock = {
    key : string 
    token : string 
}

export const acquireSteplock = async ( steprunid: string , ) : Promise<StepLock | null> => {
    const lock = {
        key : `dag:lock:step:${steprunid}`,
        token : randomUUID(), 
    }

    const result = await redisConnection.set(
        lock.key,
        lock.token,
        "EX",
        lockttlseconds,
        "NX"
    )
    return result === "OK" ? lock : null 
}
export const releaseStepLock = async (lock: StepLock): Promise<boolean> => {
    const luaScript = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
    `

    const result = await redisConnection.eval(
        luaScript,
        1,
        lock.key,
        lock.token,
    )

    return result === 1
}