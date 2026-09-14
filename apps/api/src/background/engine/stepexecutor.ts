import { FailureOutcome, Status } from "../../generated/prisma/client"
import { executeHttpStep } from "../handlers/httpStepHandler"
import prisma from "../../shared/utils/Prisma"
import { dagresolve } from "./dagresolver"
import { recordStepFailure, resolveFailureLogs } from "./failurelogger"
import { handleretry } from "./handleretry"
import { acquireStepLock, releaseStepLock } from "../lock/stepLock"
import {
	UpdateStepError,
	UpdateStepRunOutput,
	UpdateStepRunStatus,
} from "./statemanager"

type jobdata = {
	stepRunId: string
	jobRunId: string
	stepId: string
	stepConfig: any
	workflowId?: string
	versionId?: string
	workflowid?: string
	versionid?: string
}

export const stepexecutor = async (jobdata: jobdata) => {
	let attemptStartedAt = Date.now()
	const lock = await acquireStepLock(jobdata.stepRunId)

	if (!lock) {
		return
	}

	try {
		await UpdateStepRunStatus(jobdata.stepRunId, Status.RUNNING)

		attemptStartedAt = Date.now()
		const result = await executeHttpStep(jobdata.stepConfig)

		// The call returned without throwing, so any earlier failures on this step
		// run have now been recovered from - close them out before the DAG moves on.
		await resolveFailureLogs(jobdata.stepRunId, FailureOutcome.SUCCESS)

		// executeHttpStep throws on failure, so reaching this line is the success
		// signal - the shape of the body says nothing about whether the call worked.
		await UpdateStepRunOutput(jobdata.stepRunId, result)

		const workflowId = jobdata.workflowId ?? jobdata.workflowid
		const versionId = jobdata.versionId ?? jobdata.versionid

		if (workflowId && versionId) {
			await dagresolve(versionId, jobdata.jobRunId, workflowId)
		}
	} catch (error) {
		const latencyMs = Date.now() - attemptStartedAt

		await UpdateStepRunStatus(jobdata.stepRunId, Status.RETRYING)
		await UpdateStepError(jobdata.stepRunId, "failed")

		const stepRun = await prisma.stepRun.findFirst({
			where: { steprunid: jobdata.stepRunId },
		})

		const currentRetries = stepRun?.retries || 0

		// retries counts attempts already made, so this failure is attempt N+1.
		await recordStepFailure({
			stepRunId: jobdata.stepRunId,
			stepId: jobdata.stepId,
			jobRunId: jobdata.jobRunId,
			attemptNumber: currentRetries + 1,
			latencyMs,
			error,
		})

		await handleretry(currentRetries, jobdata)
		
	} finally {
		await releaseStepLock(lock)
	}
}
