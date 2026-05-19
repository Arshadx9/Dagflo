import { Status } from "@prisma/client"
import prisma from "../../shared/utils/Prisma"

export const createjobrun =  async (wfid : string , vid : string , input : any , status : string ) => {

return await prisma.jobRun.create({
    data : {
        workflowid : wfid,
        versionid : vid,
        data : input,
        status : "PENDING"
    }
})

} 

export const createsteprun = async (jid : string , stid : string ) => {


    return await prisma.stepRun.create({
        data : {
        jobid : jid,
        stepid : stid,
        status : Status.PENDING,
        retries : 0 
        }
    })
}

export const getrunbywfid = async (wfid : string) => {

return await prisma.jobRun.findMany({
    where : {
         workflowid : wfid
    },
     include: { steprun: true } 
})


} 

export const getrunbyid = async (runid: string) => {
    return await prisma.jobRun.findFirst({
        where: {
            jobid: runid
        },
        include: {
            steprun: true
        }
    })
}

export const updatejobrunstatus = async (status : Status , jid : string) =>{ 
        
return await prisma.jobRun.update({
where : {jobid :jid},
 data : {status : status}
 })

}

export const updatesteprunstatus = async (status : Status , stid : string) => {
    return await prisma.stepRun.update({
        where :{steprunid : stid},
        data : {
            status : status
        }
    })
}

export const getsteprunbyjobrun = async (jid : string) => {
    return await prisma.stepRun.findMany ({
     where : {
        jobid : jid
     }
    })
}

